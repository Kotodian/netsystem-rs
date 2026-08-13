//! Focused tests for the HTTP plugin descriptor and the builtin HTTP Session
//! App registration seam (VPP `http_app_cb_vft` attach, http.c:1004-1063),
//! including the `accept` callback that branches on the accepted Session's
//! metadata (VPP `http_ts_accept_callback`, http.c:733-740): one
//! `ConnectionContext` is bound to a root lower QUIC Session
//! (`http_ts_accept_connection`, http.c:586) and one `StreamContext` is bound
//! to a stream child's parent connection (`http_ts_accept_stream`, http.c:675).
//! A root accept also bootstraps its local uni control stream exactly once
//! (`http3_conn_init`, http3.c:216-250), with typed transport actions faked
//! so the open and its failure rollback are observable. The `disconnect`
//! callback (VPP `http3_transport_stream_close_callback` → `http3_stream_close`)
//! finishes a peer uni stream FIN by peer role, clears its slot, and leaves
//! bidi request FINs to request cleanup. The `reset` callback (VPP
//! `http3_transport_stream_reset_callback`) classifies a peer uni stream
//! reset and closes the parent connection exactly once with
//! `ClosedCriticalStream` (0x0104), mutating no HTTP worker state and leaving
//! bidi request resets alone. The `cleanup` callback (VPP
//! `http3_conn_cleanup_callback` / `http3_stream_cleanup_callback`) removes the
//! root connection context and clears the published app session in
//! delete-before-free order, and removes a bidi request stream's upper request
//! Session before releasing its request stream context; uni request streams and
//! zero contexts are lifecycle no-ops. The `builtin_rx` callback publishes a
//! completed request HEADERS section and then the DATA payload from a bidi
//! request stream's lower RX FIFO to its upper request Session RX FIFO (VPP
//! `http3_stream_transport_rx_req`, http3.c:1733-1799, and
//! `http3_req_state_transport_io_more_data`, http3.c:1184-1263): the upper
//! request Session is created at the first completed HEADERS publication, a
//! HEADERS capacity retry keeps the retained pending section and the
//! unconsumed lower bytes intact for the next callback, and a DATA capacity
//! failure resets the request stream with HTTP3_ERROR_INTERNAL_ERROR after
//! dequeuing exactly the reader-accounted bytes, never re-feeding them; root,
//! peer-uni, and context-less dispatches are no-ops.

use std::cell::Cell;
use std::sync::Arc;

use hammer_infra::fifo::{Fifo, FifoError};
use hammer_infra::pool::Index;
use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSessionConfig, ApplicationId, SessionAppContext, SessionAppId, SessionFlags,
};
use hammer_runtime::session::{SessionApplicationErrorCode, SessionStreamDirection};
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine, RuntimeError, RuntimeRegistry,
    RuntimeResult, SessionTransportRegistration,
};
use hammer_service::session::error::SessionTransportActionError;
use hammer_service::session::protocol::SessionAppCallbacks;
use hammer_service::session::runtime::{SessionMain, SessionTransportWorkerActions, SessionWorker};
use hammer_service::session::{ApplicationMain, SessionEndpointRole, SessionId};

use super::http_app::{
    CALLBACKS, HTTP_SESSION_APP, HttpAppError, NAME, RequestErrorAction, accept, accept_on,
    builtin_rx_on, cleanup_on, destroy, disconnect_on, install, request_publish_error_action,
    reset_on,
};
use super::listener::{HTTP_MAIN, HttpMain};
use crate::http_common::{BodyAccumulator, EncodeError, PublishError};
use crate::http3::proto::error::ErrorCode;
use crate::http3::proto::qpack::block::encode_block;
use crate::http3::proto::qpack::field::HeaderField;
use crate::http3::request::RequestPublishError;
use crate::http3::request_frame_reader::{RequestFrameError, RequestFrameRead};
use crate::worker::{
    ContextId, HTTP_CONTEXT_CAPACITY, HttpWorker, HttpWorkerError, PeerControlOutcome,
    PeerUniStreamRole, PendingFieldSection, RequestReadError, StreamContextId,
};

/// SessionWorker (64-slot pool on data worker 0) whose Application registry
/// pre-registers the builtin HTTP Session App, plus an `HttpMain` owning one
/// installed worker slot on the current thread. Direct `HttpMain`, bypassing
/// the process-wide `HTTP_MAIN` OnceLock so each test owns its authority,
/// exactly as the listener tests do.
fn test_harness() -> (
    Arc<HttpMain>,
    SessionWorker<Index>,
    ApplicationId,
    SessionAppId,
) {
    let applications = ApplicationMain::with_session_apps(8, [HTTP_SESSION_APP]);
    let application = applications.attach().expect("attach test Application");
    let session_app = applications
        .session_app_id(NAME)
        .expect("test Application registry registers the builtin HTTP Session App");
    let mut sessions = SessionWorker::<Index>::new(
        DataWorkerId::new(0),
        1,
        AppSessionConfig::default(),
        64,
        Arc::clone(&applications),
        None,
    )
    .expect("test SessionWorker");
    let main = Arc::new(HttpMain::new(
        Arc::new(SessionMain::new(0, Arc::clone(&applications))),
        SessionTransportRegistration::new("quic", None, None, None),
        session_app,
        application,
        1,
    ));
    main.install_worker(DataWorkerId::new(0))
        .expect("install test HttpWorker");
    // These workers have no FileMain, so the per-Application MQ is staged by
    // `poll_app`; the builtin_rx seam needs it to publish an upper Session
    // (`create_upper_session`, runtime.rs `install_application_mq_for_test`).
    sessions
        .install_application_mq_for_test(application)
        .expect("install test Application MQ");
    sessions
        .install_transport_actions(
            HttpMain::TRANSPORT_ID,
            SessionTransportWorkerActions::new(
                fake_open_stream,
                fake_reset_stream,
                fake_stop_sending,
                fake_close_connection,
            ),
        )
        .expect("install fake transport actions");
    (main, sessions, application, session_app)
}

/// Per-thread observation of the fake transport actions, mirroring the
/// `worker` harness: invocation counts, the passed `app_context`, and whether
/// the next `open_stream` call fails.
thread_local! {
    static OPEN_CALLS: Cell<u32> = const { Cell::new(0) };
    static OPEN_CONTEXT: Cell<u64> = const { Cell::new(0) };
    static OPEN_FAIL: Cell<bool> = const { Cell::new(false) };
    static CLOSE_CALLS: Cell<u32> = const { Cell::new(0) };
    static CLOSE_SESSION: Cell<u64> = const { Cell::new(0) };
    static CLOSE_CODE: Cell<u64> = const { Cell::new(0) };
    static RESET_CALLS: Cell<u32> = const { Cell::new(0) };
    static RESET_SESSION: Cell<u64> = const { Cell::new(0) };
    static RESET_CODE: Cell<u64> = const { Cell::new(0) };
    static CLOSE_FAIL: Cell<bool> = const { Cell::new(false) };
}

fn fake_open_stream(
    _sessions: &mut SessionWorker<Index>,
    parent: SessionId,
    direction: SessionStreamDirection,
    app_context: SessionAppContext,
) -> RuntimeResult<SessionId> {
    OPEN_CALLS.with(|calls| calls.set(calls.get() + 1));
    OPEN_CONTEXT.with(|seen| seen.set(app_context));
    assert_eq!(
        direction,
        SessionStreamDirection::Uni,
        "control stream is uni"
    );
    if OPEN_FAIL.with(|fail| fail.get()) {
        return Err(RuntimeError::ServiceClosed);
    }
    Ok(SessionId::from_raw(parent.get() + 1))
}

fn fake_reset_stream(
    _sessions: &mut SessionWorker<Index>,
    session_id: SessionId,
    code: SessionApplicationErrorCode,
) -> RuntimeResult<()> {
    RESET_CALLS.with(|calls| calls.set(calls.get() + 1));
    RESET_SESSION.with(|seen| seen.set(session_id.get()));
    RESET_CODE.with(|seen| seen.set(u64::from(code)));
    Ok(())
}

fn fake_stop_sending(
    _sessions: &mut SessionWorker<Index>,
    _session_id: SessionId,
    _code: SessionApplicationErrorCode,
) -> RuntimeResult<()> {
    Ok(())
}

fn fake_close_connection(
    _sessions: &mut SessionWorker<Index>,
    session_id: SessionId,
    code: SessionApplicationErrorCode,
    reason: &[u8],
) -> RuntimeResult<()> {
    CLOSE_CALLS.with(|calls| calls.set(calls.get() + 1));
    CLOSE_SESSION.with(|seen| seen.set(session_id.get()));
    CLOSE_CODE.with(|seen| seen.set(u64::from(code)));
    assert!(
        reason.is_empty(),
        "connection close carries an empty reason"
    );
    if CLOSE_FAIL.with(|fail| fail.get()) {
        return Err(RuntimeError::ServiceClosed);
    }
    Ok(())
}

/// One Session App transport session owned by the test SessionWorker.
fn construct_session(
    sessions: &mut SessionWorker<Index>,
    application: ApplicationId,
    session_app: SessionAppId,
    transport_slot: u32,
) -> SessionId {
    sessions
        .construct_transport_session(
            HttpMain::TRANSPORT_ID,
            Index::new(transport_slot, 0),
            0,
            application,
            Some(session_app),
            None,
            None,
            false,
        )
        .expect("construct Session App session")
}

/// A root Session constructed as accepted from a listener, so accept
/// metadata reports `Server` (runtime.rs `endpoint_role`), exactly like a
/// lower QUIC connection the transport accepted.
fn construct_accepted_root(
    sessions: &mut SessionWorker<Index>,
    application: ApplicationId,
    session_app: SessionAppId,
    transport_slot: u32,
) -> SessionId {
    sessions
        .construct_transport_session(
            HttpMain::TRANSPORT_ID,
            Index::new(transport_slot, 0),
            0,
            application,
            Some(session_app),
            None,
            None,
            true,
        )
        .expect("construct accepted Session App session")
}

/// The typed `HttpWorkerError` inside a `RuntimeError` chain.
fn http_error(error: &RuntimeError) -> &HttpWorkerError {
    std::error::Error::source(error)
        .and_then(|cause| cause.downcast_ref::<HttpWorkerError>())
        .expect("typed HTTP worker error in the chain")
}

/// The typed `HttpAppError` inside a `RuntimeError` chain.
fn http_app_error(error: &RuntimeError) -> &HttpAppError {
    std::error::Error::source(error)
        .and_then(|cause| cause.downcast_ref::<HttpAppError>())
        .expect("typed HTTP app error in the chain")
}

/// The typed `SessionTransportActionError` inside a `RuntimeError` chain.
fn transport_action_error(error: &RuntimeError) -> &SessionTransportActionError {
    std::error::Error::source(error)
        .and_then(|cause| cause.downcast_ref::<SessionTransportActionError>())
        .expect("typed transport action error in the chain")
}

fn worker_len(main: &HttpMain) -> usize {
    main.with_worker(DataWorkerId::new(0), |http| Ok(http.len()))
        .expect("worker accessible on the test thread")
}

/// A root Session with its connection context allocated and published
/// (`set_app_session`), exactly the two steps the connection accept path
/// performs; returns the live `ContextId` for assertions.
fn construct_parent(
    main: &HttpMain,
    sessions: &mut SessionWorker<Index>,
    application: ApplicationId,
    session_app: SessionAppId,
    transport_slot: u32,
) -> (SessionId, ContextId) {
    let parent = construct_session(sessions, application, session_app, transport_slot);
    let context = main
        .with_worker(DataWorkerId::new(0), |http| {
            http.allocate(parent).map_err(RuntimeError::from)
        })
        .expect("allocate parent connection context");
    sessions
        .set_app_session(parent, u64::from(context))
        .expect("publish parent context");
    (parent, context)
}

/// A stream child of `parent` with the QUIC-derived flags and its listener
/// handle pinned to the parent, as `quic_quicly_on_stream_open` does.
fn construct_stream(
    sessions: &mut SessionWorker<Index>,
    application: ApplicationId,
    session_app: SessionAppId,
    transport_slot: u32,
    parent: SessionId,
    uni: bool,
) -> SessionId {
    let child = construct_session(sessions, application, session_app, transport_slot);
    let flags = if uni {
        SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL
    } else {
        SessionFlags::STREAM
    };
    sessions
        .set_session_flags(child, flags)
        .expect("derive stream child flags");
    sessions
        .pin_accepted_listener(child, sessions.session_handle(parent))
        .expect("pin child listener to its parent");
    child
}

/// A peer uni stream child of `parent`, accepted through `accept_on`; in the
/// fresh stream pool the first allocation's `StreamContextId` is the
/// deterministic `Index::new(0, 1)`.
fn accept_peer_uni_stream(
    main: &HttpMain,
    sessions: &mut SessionWorker<Index>,
    application: ApplicationId,
    session_app: SessionAppId,
    transport_slot: u32,
    parent: SessionId,
) -> SessionId {
    let child = construct_stream(
        sessions,
        application,
        session_app,
        transport_slot,
        parent,
        true,
    );
    accept_on(main, sessions, child, 0).expect("accept peer uni stream");
    child
}

/// One live parent connection and one accepted peer uni stream child, with
/// the parent connection context. In a fresh stream pool the single stream's
/// `StreamContextId` is the deterministic `Index::new(0, 1)`.
fn construct_reset_stream(
    main: &HttpMain,
    sessions: &mut SessionWorker<Index>,
    application: ApplicationId,
    session_app: SessionAppId,
    transport_slot: u32,
) -> (SessionId, ContextId, SessionId, StreamContextId) {
    let (parent, parent_context) =
        construct_parent(main, sessions, application, session_app, transport_slot);
    let child = accept_peer_uni_stream(
        main,
        sessions,
        application,
        session_app,
        transport_slot + 1,
        parent,
    );
    (
        parent,
        parent_context,
        child,
        StreamContextId::from(Index::new(0, 1)),
    )
}

/// One live parent connection and one accepted bidi request stream child,
/// with the parent connection context. In a fresh stream pool the single
/// stream's `StreamContextId` is the deterministic `Index::new(0, 1)`.
fn construct_bidi_request_stream(
    main: &HttpMain,
    sessions: &mut SessionWorker<Index>,
    application: ApplicationId,
    session_app: SessionAppId,
    transport_slot: u32,
) -> (SessionId, ContextId, SessionId, StreamContextId) {
    let (parent, parent_context) =
        construct_parent(main, sessions, application, session_app, transport_slot);
    let child = construct_stream(
        sessions,
        application,
        session_app,
        transport_slot + 1,
        parent,
        false,
    );
    accept_on(main, sessions, child, 0).expect("accept bidi request stream");
    (
        parent,
        parent_context,
        child,
        StreamContextId::from(Index::new(0, 1)),
    )
}

/// A complete zero-body HEADERS frame: a QPACK-encoded GET field section of
/// only pseudo fields (no Content-Length, so the request is body-less).
fn zero_body_headers_frame() -> Vec<u8> {
    let mut section = Vec::new();
    encode_block(
        &mut section,
        &[
            HeaderField::new(":method", "GET"),
            HeaderField::new(":scheme", "https"),
            HeaderField::new(":authority", "example.com"),
            HeaderField::new(":path", "/"),
        ],
    )
    .expect("fixture field section encodes");
    assert!(section.len() < 64, "fixture fits a one-byte varint length");
    let mut frame = vec![0x01, section.len() as u8];
    frame.extend_from_slice(&section);
    frame
}

/// A complete HEADERS frame whose field section declares a Content-Length of
/// `len`: the request publishes like `zero_body_headers_frame` (the declared
/// length is consumed into the body accumulator, not the published request)
/// and its body accepts exactly `len` DATA bytes.
fn content_length_headers_frame(len: u64) -> Vec<u8> {
    let mut section = Vec::new();
    encode_block(
        &mut section,
        &[
            HeaderField::new(":method", "GET"),
            HeaderField::new(":scheme", "https"),
            HeaderField::new(":authority", "example.com"),
            HeaderField::new(":path", "/"),
            HeaderField::new("content-length", len.to_string().as_str()),
        ],
    )
    .expect("fixture field section encodes");
    assert!(section.len() < 64, "fixture fits a one-byte varint length");
    let mut frame = vec![0x01, section.len() as u8];
    frame.extend_from_slice(&section);
    frame
}

#[test]
fn plugin_descriptor_declares_name_http_loaded_after_quic() {
    let manifest = crate::__HAMMER_PLUGIN_MANIFEST_TOML;
    assert!(manifest.contains("name = \"http\""), "manifest: {manifest}");
    assert!(
        manifest.contains("load_after = [\"quic\",]"),
        "manifest: {manifest}"
    );
}

#[test]
fn registers_exactly_the_builtin_http_session_app() {
    assert_eq!(HTTP_SESSION_APP.name(), NAME);
}

#[test]
fn callback_table_wires_accept_disconnect_reset_cleanup_and_builtin_rx_and_defers_the_rest() {
    // `accept`, `disconnect`, `reset`, `cleanup`, and `builtin_rx` are the
    // VPP `http_app_cb_vft` entries (http.c:1004-1017) this slice owns:
    // accept needs nothing beyond the per-worker context pool, disconnect
    // needs the worker's peer uni stream slots and
    // `finish_peer_*`/`remove_stream` helpers (worker.rs), reset needs only
    // the committed `classify_peer_uni_stream_reset` classification plus the
    // generic `close_connection` transport action — no HTTP worker state
    // mutation, mirroring VPP `http3_transport_stream_reset_callback` —
    // cleanup needs only the worker's `remove`/`release_request_stream`
    // pool removal plus the Session worker's `upper_session`/
    // `remove_upper_session` and `set_app_session` publication, mirroring
    // VPP `http3_conn_cleanup_callback`/`http3_stream_cleanup_callback`,
    // and builtin_rx needs the worker's request reader, pending-section
    // retention, and body publication plus the Session worker's
    // `create_upper_session`/`fifo_pair`/`reset_stream`/`publish_rx_*`
    // helpers, mirroring VPP `http3_stream_transport_rx_req`
    // (http3.c:1733-1799). Every other entry needs lifecycle state from
    // later slices, so it stays `None`: no speculative no-ops until the
    // owning slices land.
    let callbacks: SessionAppCallbacks = CALLBACKS;
    assert!(callbacks.accept.is_some());
    assert!(callbacks.disconnect.is_some());
    assert!(callbacks.reset.is_some());
    assert!(callbacks.cleanup.is_some());
    assert!(callbacks.builtin_rx.is_some());
    assert!(callbacks.add_segment.is_none());
    assert!(callbacks.del_segment.is_none());
    assert!(callbacks.connected.is_none());
    assert!(callbacks.transport_closed.is_none());
    assert!(callbacks.half_open_cleanup.is_none());
    assert!(callbacks.migrate.is_none());
    assert!(callbacks.listened.is_none());
    assert!(callbacks.unlistened.is_none());
    assert!(callbacks.builtin_tx.is_none());
    assert!(callbacks.fifo_tuning.is_none());
    assert!(callbacks.proxy_alloc_fifos.is_none());
    assert!(callbacks.proxy_write_early_data.is_none());
    assert!(callbacks.app_evt.is_none());
    assert!(callbacks.crypto_async.is_none());
}

#[test]
fn install_returns_typed_error_without_session_main() {
    let mut engine = Engine::new(
        DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
        RuntimeRegistry::new(),
    );
    let error = install(&mut engine).expect_err("install without SessionMain must fail");
    assert!(
        matches!(error, RuntimeError::RuntimeCapabilityMissing { .. }),
        "expected RuntimeCapabilityMissing, got {error:?}"
    );
}

#[test]
fn destroy_without_worker_contexts_is_noop() {
    // destroy cannot report a Result; with no HTTP worker context ever
    // created in this slice, the hook must be callable and do nothing.
    destroy(DataWorkerId::new(0), 0);
}

#[test]
fn accept_allocates_one_context_and_keeps_it_on_successful_publication() {
    let (main, mut sessions, application, session_app) = test_harness();
    let session = construct_session(&mut sessions, application, session_app, 1);

    accept_on(&main, &mut sessions, session, 0).expect("accept succeeds");

    // Publication succeeded, so the allocated context was not rolled back:
    // exactly one context remains bound to the worker's pool.
    assert_eq!(worker_len(&main), 1);
}

#[test]
fn accept_root_records_server_role_from_accept_metadata() {
    let (main, mut sessions, application, session_app) = test_harness();
    let session = construct_accepted_root(&mut sessions, application, session_app, 1);

    accept_on(&main, &mut sessions, session, 0).expect("accept root");

    // Fresh connection pool: the first allocation occupies slot 0 with
    // generation 1, so its identity is deterministic.
    let context = ContextId::from(Index::new(0, 1));
    main.with_worker(DataWorkerId::new(0), |http| {
        let connection = http.get(context).map_err(RuntimeError::from)?;
        assert_eq!(
            connection.role,
            Some(SessionEndpointRole::Server),
            "root accept records the accept-metadata role on the connection context"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn accept_is_idempotent_when_context_is_already_published() {
    let (main, mut sessions, application, session_app) = test_harness();
    let session = construct_session(&mut sessions, application, session_app, 2);
    OPEN_CALLS.with(|calls| calls.set(0));

    accept_on(&main, &mut sessions, session, 0).expect("first accept");
    OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "first accept bootstraps once"));
    // The dispatch layer passes the published nonzero context back on a
    // duplicate accept; it must allocate nothing further.
    accept_on(&main, &mut sessions, session, 1).expect("duplicate accept is a no-op");
    OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "duplicate accept bootstraps nothing"));
    assert_eq!(worker_len(&main), 1);
}

#[test]
fn connection_accept_bootstraps_control_stream_exactly_once() {
    let (main, mut sessions, application, session_app) = test_harness();
    let session = construct_session(&mut sessions, application, session_app, 1);
    // Fresh connection pool: the first allocation occupies slot 0 with
    // generation 1, so its identity is deterministic.
    let context = ContextId::from(Index::new(0, 1));
    OPEN_CALLS.with(|calls| calls.set(0));
    OPEN_CONTEXT.with(|seen| seen.set(0));

    accept_on(&main, &mut sessions, session, 0).expect("accept bootstraps the control stream");

    OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "bootstrap invoked exactly once"));
    OPEN_CONTEXT.with(|seen| {
        assert_eq!(
            seen.get(),
            u64::from(context),
            "the control stream carries the published connection context"
        )
    });
    main.with_worker(DataWorkerId::new(0), |http| {
        let connection = http.get(context).map_err(RuntimeError::from)?;
        assert_eq!(
            connection.local_control,
            Some(SessionId::from_raw(session.get() + 1)),
            "the opened control stream child is recorded on the context"
        );
        assert!(
            connection.peer_settings_pending,
            "peer SETTINGS still expected"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn connection_accept_rolls_back_and_closes_lower_connection_when_bootstrap_fails() {
    let (main, mut sessions, application, session_app) = test_harness();
    let session = construct_session(&mut sessions, application, session_app, 1);
    // Fresh connection pool: the first allocation occupies slot 0 with
    // generation 1, so its identity is deterministic.
    let context = ContextId::from(Index::new(0, 1));
    OPEN_CALLS.with(|calls| calls.set(0));
    OPEN_FAIL.with(|fail| fail.set(true));
    CLOSE_CALLS.with(|calls| calls.set(0));
    CLOSE_SESSION.with(|seen| seen.set(0));
    CLOSE_CODE.with(|seen| seen.set(0));

    let error = accept_on(&main, &mut sessions, session, 0)
        .expect_err("bootstrap failure must fail the accept");
    assert!(
        matches!(
            http_error(&error),
            HttpWorkerError::ControlStreamOpenFailed { context: failed } if *failed == context
        ),
        "primary bootstrap error preserved, got {error:?}"
    );
    OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "failing action invoked once"));
    assert_eq!(
        worker_len(&main),
        0,
        "connection context removed in rollback"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 1, "lower connection closed in rollback"));
    CLOSE_SESSION.with(|seen| {
        assert_eq!(
            seen.get(),
            session.get(),
            "close names the lower connection"
        )
    });
    CLOSE_CODE.with(|seen| assert_eq!(seen.get(), 0x0102, "close carries VPP H3_INTERNAL_ERROR"));

    // The cleared app context leaves the session fully re-acceptable: a fresh
    // dispatch context re-runs the whole path and re-bootstraps a new context
    // generation, with the failing open action healed.
    OPEN_FAIL.with(|fail| fail.set(false));
    OPEN_CALLS.with(|calls| calls.set(0));
    accept_on(&main, &mut sessions, session, 0).expect("retry accept succeeds");
    OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "retry bootstraps again"));
    OPEN_CONTEXT.with(|seen| {
        assert_eq!(
            seen.get(),
            u64::from(ContextId::from(Index::new(0, 2))),
            "retry re-publishes a fresh context generation"
        )
    });
    assert_eq!(worker_len(&main), 1, "retry left one live context");
}

#[test]
fn accept_rolls_back_context_when_publication_fails() {
    let (main, mut sessions, _application, _session_app) = test_harness();
    // A session outside the 64-slot worker pool: the context allocation
    // succeeds, but set_app_session fails with the session subsystem's
    // SessionMissing, so the context must be rolled back and the primary
    // error preserved.
    let bogus = SessionId::from_raw(123);
    // No accept metadata exists for the out-of-range session, so the accept
    // takes the missing-metadata fallback: the connection path allocates
    // with role `None`, then the publication failure below rolls it back.
    assert!(
        sessions.accept_metadata(bogus).is_none(),
        "fallback branch is the missing-metadata path"
    );
    let error = accept_on(&main, &mut sessions, bogus, 0)
        .expect_err("out-of-range session cannot be published");
    assert!(
        matches!(error, RuntimeError::Subsystem { subsystem, .. } if subsystem == "session"),
        "primary set_app_session error preserved, got {error:?}"
    );
    assert_eq!(
        worker_len(&main),
        0,
        "failed publication rolled back the context"
    );
}

#[test]
fn accept_surfaces_context_capacity_exhaustion_unchanged() {
    let (main, mut sessions, application, session_app) = test_harness();
    let session = construct_session(&mut sessions, application, session_app, 3);
    let worker = DataWorkerId::new(0);

    // Fill the worker's context pool to capacity, then accept once more: the
    // typed capacity error must propagate unchanged through the accept path.
    for _ in 0..HTTP_CONTEXT_CAPACITY {
        main.with_worker(worker, |http| {
            http.allocate(session).map_err(RuntimeError::from)
        })
        .expect("fill context pool");
    }
    let error = accept_on(&main, &mut sessions, session, 0).expect_err("pool is full");
    assert!(matches!(
        http_error(&error),
        HttpWorkerError::ContextCapacityExhausted {
            capacity: HTTP_CONTEXT_CAPACITY
        }
    ));
}

#[test]
fn accept_without_published_authority_is_typed_plugin_error() {
    // HTTP_MAIN is a process-wide OnceLock that the listener init test may
    // set concurrently; observe the unset path only while it is still unset
    // so the assertion stays deterministic.
    if HTTP_MAIN.get().is_some() {
        return;
    }
    let mut sessions = SessionWorker::<Index>::new(
        DataWorkerId::new(0),
        1,
        AppSessionConfig::default(),
        64,
        ApplicationMain::new(4),
        None,
    )
    .expect("test SessionWorker");
    let error = accept(&mut sessions, SessionId::from_raw(5), 0)
        .expect_err("accept without authority must fail");
    assert!(
        matches!(
            error,
            RuntimeError::PluginStateNotInitialized { plugin } if plugin == NAME
        ),
        "expected PluginStateNotInitialized, got {error:?}"
    );
}

#[test]
fn accept_stream_allocates_bidi_stream_bound_to_parent_and_direction() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = construct_stream(&mut sessions, application, session_app, 2, parent, false);

    accept_on(&main, &mut sessions, child, 0).expect("accept bidi stream");

    // The stream went to the stream pool: the connection pool still holds
    // exactly the parent context.
    assert_eq!(worker_len(&main), 1, "parent connection context only");
    main.with_worker(DataWorkerId::new(0), |http| {
        // Fresh stream pool: the first allocation occupies slot 0 with
        // generation 1, so its identity is deterministic.
        let stream = http
            .get_stream_for_session(StreamContextId::from(Index::new(0, 1)), child)
            .map_err(RuntimeError::from)?;
        assert_eq!(
            stream.parent, parent_context,
            "stream bound to its parent connection context"
        );
        assert_eq!(
            stream.direction,
            SessionStreamDirection::Bidi,
            "no UNIDIRECTIONAL flag derives a bidi stream"
        );
        assert_eq!(http.stream_len(), 1, "exactly one stream allocated");
        Ok(())
    })
    .expect("stream context readable on the test thread");
}

#[test]
fn accept_stream_allocates_uni_stream_bound_to_parent_and_direction() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = construct_stream(&mut sessions, application, session_app, 2, parent, true);

    accept_on(&main, &mut sessions, child, 0).expect("accept uni stream");

    assert_eq!(worker_len(&main), 1, "parent connection context only");
    main.with_worker(DataWorkerId::new(0), |http| {
        let stream = http
            .get_stream_for_session(StreamContextId::from(Index::new(0, 1)), child)
            .map_err(RuntimeError::from)?;
        assert_eq!(
            stream.parent, parent_context,
            "stream bound to its parent connection context"
        );
        assert_eq!(
            stream.direction,
            SessionStreamDirection::Uni,
            "UNIDIRECTIONAL flag derives a uni stream"
        );
        assert_eq!(http.stream_len(), 1, "exactly one stream allocated");
        Ok(())
    })
    .expect("stream context readable on the test thread");
}

#[test]
fn accept_stream_is_idempotent_when_context_is_already_published() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = construct_stream(&mut sessions, application, session_app, 2, parent, false);

    accept_on(&main, &mut sessions, child, 0).expect("first stream accept");
    // The dispatch layer passes the published StreamContextId back on a
    // duplicate accept; it must allocate nothing further.
    let published = u64::from(StreamContextId::from(Index::new(0, 1)));
    accept_on(&main, &mut sessions, child, published).expect("duplicate accept is a no-op");
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.stream_len(), 1, "no second stream allocated");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn accept_stream_without_parent_context_is_typed_error() {
    let (main, mut sessions, application, session_app) = test_harness();
    // A stream child whose listener handle resolves no live parent session:
    // `accept_metadata` yields no parent context, and the accept must fail
    // with the typed HTTP app error before allocating anything.
    let child = construct_session(&mut sessions, application, session_app, 1);
    sessions
        .set_session_flags(child, SessionFlags::STREAM)
        .expect("derive stream flags");

    let error = accept_on(&main, &mut sessions, child, 0)
        .expect_err("stream without parent cannot be accepted");
    assert!(
        matches!(
            http_app_error(&error),
            HttpAppError::StreamParentMissing { session } if *session == child
        ),
        "typed missing-parent error expected, got {error:?}"
    );
    assert_eq!(worker_len(&main), 0, "no connection context allocated");
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.stream_len(), 0, "no stream context allocated");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn accept_stream_stale_parent_context_is_typed_error() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    // The parent connection context is released (cleanup), so the identity
    // the parent Session's opaque still names is stale: its generation no
    // longer matches the pool slot.
    main.with_worker(DataWorkerId::new(0), |http| {
        http.remove(parent_context).map_err(RuntimeError::from)
    })
    .expect("release parent context");
    let child = construct_stream(&mut sessions, application, session_app, 2, parent, false);

    let error =
        accept_on(&main, &mut sessions, child, 0).expect_err("stale parent cannot be adopted");
    assert!(
        matches!(
            http_error(&error),
            HttpWorkerError::ParentContextMissing { parent } if *parent == parent_context
        ),
        "typed parent-missing error expected, got {error:?}"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(
            http.stream_len(),
            0,
            "no stream allocated under a stale parent"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn accept_stream_foreign_parent_context_is_typed_error() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);

    // A context live in a second worker's pool: when the parent Session's
    // opaque names that foreign identity, this worker must reject it rather
    // than attach the stream to a context it does not own.
    let (foreign_main, mut foreign_sessions, foreign_application, foreign_session_app) =
        test_harness();
    let foreign_parent = construct_session(
        &mut foreign_sessions,
        foreign_application,
        foreign_session_app,
        1,
    );
    // A throwaway allocation first, so the foreign identity's packed
    // slot|generation differs from every context in the accepting worker's
    // pool (both pools start at slot 0, generation 1).
    foreign_main
        .with_worker(DataWorkerId::new(0), |http| {
            http.allocate(foreign_parent)
                .map(|_| ())
                .map_err(RuntimeError::from)
        })
        .expect("allocate throwaway context in the foreign worker");
    let foreign_context = foreign_main
        .with_worker(DataWorkerId::new(0), |http| {
            http.allocate(foreign_parent).map_err(RuntimeError::from)
        })
        .expect("allocate context in the foreign worker");
    sessions
        .set_app_session(parent, u64::from(foreign_context))
        .expect("parent opaque names a foreign context");
    let child = construct_stream(&mut sessions, application, session_app, 2, parent, false);

    let error =
        accept_on(&main, &mut sessions, child, 0).expect_err("foreign parent cannot be adopted");
    assert!(
        matches!(
            http_error(&error),
            HttpWorkerError::ParentContextMissing { parent } if *parent == foreign_context
        ),
        "typed parent-missing error expected, got {error:?}"
    );
    assert_eq!(worker_len(&main), 1, "only the real parent context remains");
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(
            http.stream_len(),
            0,
            "no stream allocated under a foreign parent"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn disconnect_finishes_peer_control_stream_clearing_slot_and_reader() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    let stream = StreamContextId::from(Index::new(0, 1));
    main.with_worker(DataWorkerId::new(0), |http| {
        http.register_peer_uni_stream(stream, PeerUniStreamRole::Control)
            .map_err(RuntimeError::from)?;
        // The first control byte allocates the one-shot SETTINGS reader.
        assert_eq!(
            http.process_peer_control_bytes(stream, &[0x04])
                .expect("feed control byte"),
            PeerControlOutcome::Incomplete { consumed: 1 }
        );
        Ok(())
    })
    .expect("register control role");

    disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect("peer control FIN is silent");

    main.with_worker(DataWorkerId::new(0), |http| {
        let connection = http.get(parent_context).map_err(RuntimeError::from)?;
        assert_eq!(connection.peer_control, None, "peer control slot cleared");
        assert!(
            connection.peer_control_reader.is_none(),
            "SETTINGS reader freed with the control stream"
        );
        assert!(
            connection.peer_settings_pending,
            "EOF before SETTINGS keeps the expectation pending"
        );
        assert!(
            matches!(
                http.get_stream(stream),
                Err(HttpWorkerError::StreamMissing { stream: stale }) if stale == stream
            ),
            "stream context released"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn disconnect_finishes_only_the_matching_peer_qpack_stream() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let encoder_child =
        accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    let decoder_child =
        accept_peer_uni_stream(&main, &mut sessions, application, session_app, 3, parent);
    let encoder = StreamContextId::from(Index::new(0, 1));
    let decoder = StreamContextId::from(Index::new(1, 1));
    main.with_worker(DataWorkerId::new(0), |http| {
        http.register_peer_uni_stream(encoder, PeerUniStreamRole::QpackEncoder)
            .map_err(RuntimeError::from)?;
        http.register_peer_uni_stream(decoder, PeerUniStreamRole::QpackDecoder)
            .map_err(RuntimeError::from)?;
        Ok(())
    })
    .expect("register QPACK roles");

    disconnect_on(&main, &mut sessions, encoder_child, u64::from(encoder))
        .expect("QPACK encoder FIN is silent");

    main.with_worker(DataWorkerId::new(0), |http| {
        let connection = http.get(parent_context).map_err(RuntimeError::from)?;
        assert_eq!(connection.peer_encoder, None, "encoder slot cleared");
        assert_eq!(
            connection.peer_decoder,
            Some(decoder),
            "decoder slot untouched by the encoder FIN"
        );
        assert!(
            matches!(
                http.get_stream(encoder),
                Err(HttpWorkerError::StreamMissing { stream: stale }) if stale == encoder
            ),
            "encoder stream context released"
        );
        assert_eq!(
            http.get_stream(decoder)
                .map_err(RuntimeError::from)?
                .session,
            decoder_child,
            "decoder stream stays live"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn disconnect_removes_unknown_peer_uni_stream_owning_no_slot() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    let stream = StreamContextId::from(Index::new(0, 1));
    main.with_worker(DataWorkerId::new(0), |http| {
        http.register_peer_uni_stream(stream, PeerUniStreamRole::Unknown)
            .map_err(RuntimeError::from)?;
        Ok(())
    })
    .expect("register unknown role");

    disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect("unknown stream FIN is silent");

    main.with_worker(DataWorkerId::new(0), |http| {
        let connection = http.get(parent_context).map_err(RuntimeError::from)?;
        assert_eq!(connection.peer_control, None, "no control slot touched");
        assert_eq!(connection.peer_encoder, None, "no encoder slot touched");
        assert_eq!(connection.peer_decoder, None, "no decoder slot touched");
        assert!(
            connection.peer_control_reader.is_none(),
            "no SETTINGS reader touched"
        );
        assert_eq!(http.stream_len(), 0, "stream context released");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn disconnect_with_zero_context_is_a_lifecycle_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);

    disconnect_on(&main, &mut sessions, child, 0).expect("zero context is a no-op");

    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "no connection context touched");
        assert_eq!(http.stream_len(), 1, "no stream touched");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn disconnect_with_stale_stream_context_is_typed_error() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    let stream = StreamContextId::from(Index::new(0, 1));
    main.with_worker(DataWorkerId::new(0), |http| {
        http.remove_stream(stream).map_err(RuntimeError::from)
    })
    .expect("release stream before disconnect");

    let error = disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect_err("stale stream identity must fail");
    assert!(
        matches!(
            http_error(&error),
            HttpWorkerError::StreamMissing { stream: stale } if *stale == stream
        ),
        "typed stale-stream error expected, got {error:?}"
    );
}

#[test]
fn disconnect_on_bidi_request_stream_without_upper_releases_request() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);
    CLOSE_CALLS.with(|calls| calls.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    // With no upper request Session, a FIN on a complete request releases
    // the request stream context while the lower stream Session and the
    // parent connection Session/context stay live, and no transport action
    // is dispatched.
    disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect("bidi FIN without an upper releases the request");

    assert!(
        sessions.has_session(child),
        "the lower stream Session survives"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no connection close"));
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no stream reset"));
    main.with_worker(DataWorkerId::new(0), |http| {
        let connection = http.get(parent_context).map_err(RuntimeError::from)?;
        assert_eq!(connection.peer_control, None, "no control slot");
        assert_eq!(connection.peer_encoder, None, "no encoder slot");
        assert_eq!(connection.peer_decoder, None, "no decoder slot");
        assert_eq!(http.stream_len(), 0, "the request stream is released");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn disconnect_for_root_connection_with_colliding_stream_context_id_is_a_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    // Fresh connection and stream pools both place their first allocation
    // at slot 0 with generation 1, so the root ConnectionContextId
    // numerically equals the live StreamContextId.
    let stream = StreamContextId::from(Index::new(0, 1));
    assert_eq!(
        u64::from(parent_context),
        u64::from(stream),
        "root connection context numerically collides with the stream context"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        http.register_peer_uni_stream(stream, PeerUniStreamRole::Control)
            .map_err(RuntimeError::from)?;
        Ok(())
    })
    .expect("register control role");

    // The root connection Disconnected callback carries the published
    // ConnectionContextId of the parent Session — numerically identical to
    // the live stream's StreamContextId — and must be a clean no-op, never
    // a misattributed stream FIN.
    disconnect_on(&main, &mut sessions, parent, u64::from(parent_context))
        .expect("root connection disconnect is a no-op");

    main.with_worker(DataWorkerId::new(0), |http| {
        let connection = http.get(parent_context).map_err(RuntimeError::from)?;
        assert_eq!(
            connection.peer_control,
            Some(stream),
            "control slot untouched"
        );
        assert_eq!(
            http.get_stream(stream)
                .map_err(RuntimeError::from)?
                .peer_role,
            PeerUniStreamRole::Control,
            "stream context untouched"
        );
        assert_eq!(http.stream_len(), 1, "stream stays live");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn reset_on_peer_uni_stream_closes_connection_with_closed_critical_stream() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context, child, stream) =
        construct_reset_stream(&main, &mut sessions, application, session_app, 1);
    CLOSE_CALLS.with(|calls| calls.set(0));
    CLOSE_SESSION.with(|seen| seen.set(0));
    CLOSE_CODE.with(|seen| seen.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    reset_on(&main, &mut sessions, child, u64::from(stream)).expect("peer uni reset closes");

    assert_ne!(
        parent, child,
        "child stream SessionId differs from the parent root SessionId"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 1, "connection closed exactly once"));
    CLOSE_SESSION.with(|seen| {
        assert_eq!(
            seen.get(),
            parent.get(),
            "close names the parent root connection SessionId (reset.session)"
        )
    });
    CLOSE_CODE.with(|seen| assert_eq!(seen.get(), 0x0104, "close carries ClosedCriticalStream"));
    RESET_CALLS.with(|calls| {
        assert_eq!(
            calls.get(),
            0,
            "no stream reset for a connection-terminating reset"
        )
    });
    // The seam mutates no HttpWorker state: stream and connection stay live.
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "connection context untouched");
        assert_eq!(http.stream_len(), 1, "stream context untouched");
        assert_eq!(
            http.get_stream_for_session(stream, child)
                .map_err(RuntimeError::from)?
                .session,
            child,
            "stream still bound to its session"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn reset_repeated_callback_dispatches_the_close_exactly_once() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_reset_stream(&main, &mut sessions, application, session_app, 1);
    CLOSE_CALLS.with(|calls| calls.set(0));

    reset_on(&main, &mut sessions, child, u64::from(stream)).expect("first reset closes");
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 1, "first reset dispatches the close"));
    // The Session app-close guard (`on_app_close_dispatch`, runtime.rs) has
    // already recorded AppClosed, so the repeated dispatch through the public
    // Session seam is a no-op instead of a second transport action.
    reset_on(&main, &mut sessions, child, u64::from(stream))
        .expect("repeated reset is guarded to a no-op");
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 1, "close dispatched exactly once"));
}

#[test]
fn reset_with_zero_context_is_a_lifecycle_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, _stream) =
        construct_reset_stream(&main, &mut sessions, application, session_app, 1);
    CLOSE_CALLS.with(|calls| calls.set(0));

    reset_on(&main, &mut sessions, child, 0).expect("zero context is a no-op");

    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no close for a zero context"));
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "no connection context touched");
        assert_eq!(http.stream_len(), 1, "no stream touched");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// A RESET for a stream identity a seam already released (here `remove_stream`,
/// the peer-uni release path) is a clean no-op — never a typed error that
/// would abort the transport callback — matching VPP's reset path freeing
/// nothing (http3.c:2336-2361): the first reset already dispatched the close,
/// so the repeated reset has nothing left to do.
#[test]
fn reset_after_stream_release_is_a_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_reset_stream(&main, &mut sessions, application, session_app, 1);
    main.with_worker(DataWorkerId::new(0), |http| {
        http.remove_stream(stream).map_err(RuntimeError::from)
    })
    .expect("release stream before reset");
    CLOSE_CALLS.with(|calls| calls.set(0));

    reset_on(&main, &mut sessions, child, u64::from(stream))
        .expect("repeated reset after release is a no-op");

    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no close dispatched"));
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.stream_len(), 0, "stream stays released");
        assert_eq!(http.len(), 1, "the parent connection context remains");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn reset_with_foreign_session_is_typed_error_without_dispatch() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context, child, stream) =
        construct_reset_stream(&main, &mut sessions, application, session_app, 1);
    let foreign = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 3, parent);
    assert_ne!(child, foreign, "second stream child is a foreign Session");
    CLOSE_CALLS.with(|calls| calls.set(0));

    // The dispatch layer always passes the reset stream's own Session; a
    // foreign live stream Session (here a sibling child's, whose root parent
    // would instead be a metadata no-op) must be rejected by the session-
    // bound check, not closed.
    let error = reset_on(&main, &mut sessions, foreign, u64::from(stream))
        .expect_err("foreign session must fail");
    assert!(
        matches!(
            http_error(&error),
            HttpWorkerError::StreamSessionMismatch { stream: s, .. } if *s == stream
        ),
        "typed session-mismatch error expected, got {error:?}"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no close dispatched"));
}

#[test]
fn reset_with_missing_parent_connection_is_typed_error_without_dispatch() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, parent_context, child, stream) =
        construct_reset_stream(&main, &mut sessions, application, session_app, 1);
    main.with_worker(DataWorkerId::new(0), |http| {
        http.remove(parent_context).map_err(RuntimeError::from)
    })
    .expect("release parent connection before reset");
    CLOSE_CALLS.with(|calls| calls.set(0));

    // The stream context is live but its parent connection is gone; the
    // committed classification fails typed and nothing dispatches.
    let error = reset_on(&main, &mut sessions, child, u64::from(stream))
        .expect_err("missing parent must fail");
    assert!(
        matches!(
            http_error(&error),
            HttpWorkerError::ParentContextMissing { parent: p } if *p == parent_context
        ),
        "typed parent-missing error expected, got {error:?}"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no close dispatched"));
}

#[test]
fn reset_closes_the_connection_for_every_peer_uni_role() {
    let (main, mut sessions, application, session_app) = test_harness();
    CLOSE_CALLS.with(|calls| calls.set(0));
    // The committed classification is role-agnostic: every peer uni role is
    // critical (VPP `http3_transport_stream_reset_callback` checks only
    // unidirectional-ness), so the callback closes for each registerable
    // role. Fixed table, no collection. Each iteration allocates exactly one
    // stream and never removes it, so iteration `k` owns slot `k`.
    for (index, (transport_slot, role)) in [
        (0u32, PeerUniStreamRole::Control),
        (2, PeerUniStreamRole::QpackEncoder),
        (4, PeerUniStreamRole::QpackDecoder),
        (6, PeerUniStreamRole::Unknown),
    ]
    .into_iter()
    .enumerate()
    {
        let (parent, _parent_context, child, _stream) = construct_reset_stream(
            &main,
            &mut sessions,
            application,
            session_app,
            transport_slot,
        );
        let stream = StreamContextId::from(Index::new(index as u32, 1));
        main.with_worker(DataWorkerId::new(0), |http| {
            http.register_peer_uni_stream(stream, role)
                .map_err(RuntimeError::from)?;
            Ok(())
        })
        .expect("register peer uni role");

        reset_on(&main, &mut sessions, child, u64::from(stream))
            .expect("reset closes the connection for every peer uni role");

        CLOSE_CALLS
            .with(|calls| assert_eq!(calls.get(), (index + 1) as u32, "role {role:?} closed once"));
        CLOSE_SESSION.with(|seen| {
            assert_eq!(
                seen.get(),
                parent.get(),
                "role {role:?} closes the parent root connection SessionId"
            )
        });
        CLOSE_CODE.with(|seen| {
            assert_eq!(
                seen.get(),
                0x0104,
                "role {role:?} closes with ClosedCriticalStream"
            )
        });
        main.with_worker(DataWorkerId::new(0), |http| {
            assert_eq!(
                http.get_stream_for_session(stream, child)
                    .map_err(RuntimeError::from)?
                    .peer_role,
                role,
                "reset records no role change on the stream"
            );
            Ok(())
        })
        .expect("worker accessible on the test thread");
    }
}

#[test]
fn reset_on_bidi_request_stream_without_upper_releases_request() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);
    CLOSE_CALLS.with(|calls| calls.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    // With no upper request Session, a bidi RESET releases the request
    // stream context while the lower stream Session and the parent
    // connection Session/context stay live, and neither a connection close
    // nor a stream reset is dispatched.
    reset_on(&main, &mut sessions, child, u64::from(stream))
        .expect("bidi reset without an upper releases the request");

    assert!(
        sessions.has_session(child),
        "the lower stream Session survives"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    CLOSE_CALLS
        .with(|calls| assert_eq!(calls.get(), 0, "bidi reset must not close the connection"));
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no stream reset"));
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(http.stream_len(), 0, "the request stream is released");
        assert!(
            http.get(parent_context).is_ok(),
            "the parent connection context is still live"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn reset_close_action_failure_propagates_typed() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_reset_stream(&main, &mut sessions, application, session_app, 1);
    CLOSE_FAIL.with(|fail| fail.set(true));

    let error = reset_on(&main, &mut sessions, child, u64::from(stream))
        .expect_err("transport action failure must fail the reset");
    assert!(
        matches!(
            error,
            RuntimeError::Subsystem { subsystem, .. }
                if subsystem == "session transport action"
        ),
        "typed transport action error expected, got {error:?}"
    );
    assert!(
        matches!(
            transport_action_error(&error),
            SessionTransportActionError::TransportActionFailed {
                action: "close_connection",
                ..
            }
        ),
        "close_connection action failure preserved in the chain"
    );

    // Restore the thread-local failure flag so a pooled test thread never
    // leaks it into later tests.
    CLOSE_FAIL.with(|fail| fail.set(false));
}

// --- cleanup callback lifecycle ---------------------------------------------

/// The root connection's cleanup callback removes the HTTP root context and
/// clears the published app session in delete-before-free order, mirroring
/// VPP `http3_conn_cleanup_callback` (http3.c:2425-2436), which delete-
/// notifies the parent request's connection before freeing it; the Session
/// entry itself survives — the SessionWorker frees it after the callback —
/// and no transport action is dispatched.
#[test]
fn cleanup_removes_root_http_context_and_clears_app_session() {
    let (main, mut sessions, application, session_app) = test_harness();
    let root = construct_accepted_root(&mut sessions, application, session_app, 1);
    accept_on(&main, &mut sessions, root, 0).expect("accept root");
    // Fresh connection pool: the first allocation occupies slot 0 with
    // generation 1, so its identity is deterministic.
    let context = ContextId::from(Index::new(0, 1));
    // A live stream child's accept metadata reports the parent's published
    // app session (runtime.rs `accept_metadata`), the observable side of the
    // root's `set_app_session` publication and of its cleanup clearing.
    let child = construct_stream(&mut sessions, application, session_app, 2, root, true);
    let before = sessions
        .accept_metadata(child)
        .expect("live child metadata")
        .parent_app_context;
    assert_eq!(
        before,
        Some(u64::from(context)),
        "parent app session published"
    );
    CLOSE_CALLS.with(|calls| calls.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    cleanup_on(&main, &mut sessions, root, u64::from(context)).expect("root cleanup succeeds");

    assert!(
        sessions.has_session(root),
        "the root Session entry survives cleanup"
    );
    assert_eq!(worker_len(&main), 0, "the HTTP root context is removed");
    let after = sessions
        .accept_metadata(child)
        .expect("live child metadata")
        .parent_app_context;
    assert_eq!(after, Some(0), "the published app session is cleared");
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no connection close"));
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no stream reset"));
}

/// A bidi request stream's cleanup callback removes the upper request
/// Session before releasing the request stream context, matching VPP
/// `http3_stream_cleanup_callback`'s delete-notify-then-free order
/// (http3.c:2459-2462); the lower stream Session and the parent connection
/// Session/context stay live and no transport action is dispatched.
#[test]
fn cleanup_on_bidi_request_stream_removes_upper_before_releasing_request() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);
    let upper = sessions
        .create_upper_session(child, u64::from(stream))
        .expect("publish upper request Session");
    CLOSE_CALLS.with(|calls| calls.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    cleanup_on(&main, &mut sessions, child, u64::from(stream))
        .expect("bidi request cleanup succeeds");

    assert!(
        !sessions.has_session(upper),
        "the upper request Session is removed"
    );
    assert!(
        sessions.has_session(child),
        "the lower stream Session survives"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no connection close"));
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no stream reset"));
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert!(
            http.get(parent_context).is_ok(),
            "the parent connection context is still live"
        );
        assert_eq!(http.stream_len(), 0, "the request stream is released");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// A zero context names no published HTTP context; the cleanup callback is a
/// lifecycle fallback no-op, consistent with the accept, disconnect, and
/// reset fallbacks.
#[test]
fn cleanup_with_zero_context_is_a_lifecycle_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context, child, _stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    cleanup_on(&main, &mut sessions, child, 0).expect("zero context is a no-op");

    assert!(
        sessions.has_session(child),
        "the lower stream Session survives"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(http.stream_len(), 1, "the request stream context remains");
        assert!(
            http.get(parent_context).is_ok(),
            "the parent connection context is still live"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// A root connection RESET carries the published `ConnectionContextId` of
/// the parent Session — numerically identical to a live stream's
/// `StreamContextId` when both pools place their first allocation at the
/// same slot and generation — and must be a clean no-op, never a
/// misattributed stream abort and never a transport action: VPP
/// `http_ts_reset_callback` marks the connection closed and disconnects the
/// transport (http.c:851-869), and the root context removal is owned by the
/// cleanup callback when the SessionWorker removes the root entry.
#[test]
fn reset_on_root_connection_with_colliding_stream_context_id_is_a_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    // Fresh connection and stream pools both place their first allocation
    // at slot 0 with generation 1, so the root ConnectionContextId
    // numerically equals the live StreamContextId.
    let stream = StreamContextId::from(Index::new(0, 1));
    assert_eq!(
        u64::from(parent_context),
        u64::from(stream),
        "root connection context numerically collides with the stream context"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        http.register_peer_uni_stream(stream, PeerUniStreamRole::Control)
            .map_err(RuntimeError::from)?;
        Ok(())
    })
    .expect("register control role");
    CLOSE_CALLS.with(|calls| calls.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    // The root connection Reset callback carries the published
    // ConnectionContextId — numerically identical to the live stream's
    // StreamContextId — and must be a clean no-op, never a misattributed
    // stream abort.
    reset_on(&main, &mut sessions, parent, u64::from(parent_context))
        .expect("root connection reset is a no-op");

    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no connection close"));
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no stream reset"));
    main.with_worker(DataWorkerId::new(0), |http| {
        let connection = http.get(parent_context).map_err(RuntimeError::from)?;
        assert_eq!(
            connection.peer_control,
            Some(stream),
            "control slot untouched"
        );
        assert_eq!(
            http.get_stream(stream)
                .map_err(RuntimeError::from)?
                .peer_role,
            PeerUniStreamRole::Control,
            "stream context untouched"
        );
        assert_eq!(http.stream_len(), 1, "stream stays live");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// The SessionWorker removes a reset request stream's Session entry after
/// the reset callback already aborted it, so cleanup runs on an
/// already-released stream context and must be a clean no-op — never a
/// typed error that would abort the entry removal before the entry is freed
/// (runtime.rs `remove_session` runs the cleanup callback before freeing).
#[test]
fn cleanup_after_reset_on_bidi_request_stream_is_a_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    reset_on(&main, &mut sessions, child, u64::from(stream))
        .expect("bidi reset aborts the request");

    cleanup_on(&main, &mut sessions, child, u64::from(stream))
        .expect("cleanup after reset is a no-op");

    assert!(
        sessions.has_session(child),
        "the lower stream Session survives cleanup"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(http.stream_len(), 0, "the request stream stays released");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// The SessionWorker removes a FINned request stream's Session entry after
/// the disconnect callback already finished it, so the follow-on cleanup
/// runs on an already-released stream context and must be a clean no-op.
#[test]
fn cleanup_after_disconnect_on_bidi_request_stream_is_a_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect("bidi FIN finishes the request");

    cleanup_on(&main, &mut sessions, child, u64::from(stream))
        .expect("cleanup after FIN is a no-op");

    assert!(
        sessions.has_session(child),
        "the lower stream Session survives cleanup"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(http.stream_len(), 0, "the request stream stays released");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// A peer uni stream closed by connection teardown — no per-stream FIN or
/// RESET seam first — is released by cleanup, mirroring VPP
/// `http3_stream_cleanup_callback` freeing the uni stream's request state at
/// cleanup (http3.c:2440-2462): the stream context, its peer control slot,
/// and the SETTINGS reader are freed while the parent connection context
/// stays live, and no transport action is dispatched.
#[test]
fn cleanup_releases_peer_uni_stream_context_at_teardown() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    let stream = StreamContextId::from(Index::new(0, 1));
    main.with_worker(DataWorkerId::new(0), |http| {
        http.register_peer_uni_stream(stream, PeerUniStreamRole::Control)
            .map_err(RuntimeError::from)?;
        // The first control byte allocates the one-shot SETTINGS reader.
        assert_eq!(
            http.process_peer_control_bytes(stream, &[0x04])
                .expect("feed control byte"),
            PeerControlOutcome::Incomplete { consumed: 1 }
        );
        Ok(())
    })
    .expect("register control role");
    CLOSE_CALLS.with(|calls| calls.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    cleanup_on(&main, &mut sessions, child, u64::from(stream))
        .expect("uni stream cleanup succeeds");

    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no connection close"));
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no stream reset"));
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(http.stream_len(), 0, "the uni stream context is released");
        let connection = http.get(parent_context).map_err(RuntimeError::from)?;
        assert_eq!(
            connection.peer_control,
            None,
            "the peer control slot is cleared"
        );
        assert!(
            connection.peer_control_reader.is_none(),
            "SETTINGS reader freed with the control stream"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// The FIN seam already released a peer uni stream context before the
/// SessionWorker removes its entry, so the follow-on cleanup is a clean
/// no-op on the already-released identity.
#[test]
fn cleanup_after_peer_uni_fin_is_a_noop() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    let child = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    let stream = StreamContextId::from(Index::new(0, 1));
    main.with_worker(DataWorkerId::new(0), |http| {
        http.register_peer_uni_stream(stream, PeerUniStreamRole::Control)
            .map_err(RuntimeError::from)?;
        Ok(())
    })
    .expect("register control role");

    disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect("peer control FIN finishes the stream");

    cleanup_on(&main, &mut sessions, child, u64::from(stream))
        .expect("cleanup after FIN is a no-op");

    assert!(
        sessions.has_session(child),
        "the lower stream Session survives cleanup"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(http.stream_len(), 0, "the uni stream stays released");
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// The worker-local retention sink for a completed request HEADERS field
/// section: the by-value `Headers(Vec<u8>)` returned by
/// `HttpWorker::process_request_bytes` is moved into the generation-checked
/// pending slot of its request stream and taken back by the decode/publish
/// seam, so a completed HEADERS is never silently discarded. Mirrors VPP
/// recording the received request on the per-request `http_ctx_t` owned by
/// the data worker (`req->headers`, `http3_req_state_wait_transport_method`,
/// http3.c:835-899) for the later app-dispatch stage.
#[test]
fn builtin_rx_completed_headers_retains_field_section_in_worker_slot() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    // Nothing is pending before any HEADERS completes.
    assert_eq!(
        worker.pending_field_section(stream, session).unwrap(),
        None,
        "no field section pending before any HEADERS frame"
    );

    // The exact feed the RX seam would perform: one complete HEADERS frame
    // (type 0x01, length 2) on the bidi request stream.
    let (read, consumed) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'i'])
        .expect("feed one complete HEADERS frame");
    assert_eq!(consumed, 4, "the whole HEADERS frame is consumed");
    let RequestFrameRead::Headers(encoded) = read else {
        panic!("a complete HEADERS frame must surface its encoded field section");
    };

    // `encoded` is the by-value field section the RX seam must retain. The
    // worker-owned pending slot takes ownership of it plus the exact
    // lower-FIFO consumed count, and the borrow hands both back for the
    // decode/publish stage.
    worker
        .retain_pending_field_section(stream, session, PendingFieldSection { encoded, consumed })
        .expect("retain the completed field section");
    let pending = worker
        .pending_field_section(stream, session)
        .expect("live stream")
        .expect("a retained section is pending");
    assert_eq!(
        pending.encoded,
        b"hi".to_vec(),
        "the pending section is the encoded HEADERS field section"
    );
    assert_eq!(
        pending.consumed, 4,
        "the pending section records the exact lower-FIFO consumed count"
    );

    // Clearing empties the slot for a later trailer or retry.
    worker
        .clear_pending_field_section(stream, session)
        .expect("clear the pending section");
    assert_eq!(
        worker.pending_field_section(stream, session).unwrap(),
        None,
        "clearing empties the pending slot"
    );
}

/// `builtin_rx_on` routes only bidi request stream contexts: a root
/// connection, a peer uni stream, and a Session without live metadata are
/// no-ops that neither create an upper Session, touch the lower FIFO, nor
/// reset anything, exactly as the accept metadata distinguishes the roles
/// (VPP `http_ts_accept_callback` branches on `SESSION_F_STREAM`,
/// http.c:733-740); only a bidi request stream feeds and publishes.
#[test]
fn builtin_rx_routes_only_bidi_request_streams() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context) =
        construct_parent(&main, &mut sessions, application, session_app, 1);
    RESET_CALLS.with(|calls| calls.set(0));

    // A root connection dispatch is a no-op: no upper Session, the root RX
    // bytes stay readable, nothing resets.
    {
        let (root_rx, _) = sessions.fifo_pair(parent).expect("root RX fifo");
        root_rx.enqueue(b"ignored");
    }
    builtin_rx_on(&main, &mut sessions, parent, u64::from(parent_context))
        .expect("root RX is a no-op");
    assert!(
        sessions.upper_session(parent).is_none(),
        "no upper request Session for a root connection"
    );
    let (root_rx, _) = sessions.fifo_pair(parent).expect("root RX fifo");
    assert_eq!(root_rx.max_dequeue(), 7, "root FIFO bytes stay readable");

    // A peer uni stream dispatch is a no-op too: no upper Session, the uni
    // RX bytes stay readable, nothing resets.
    let uni = accept_peer_uni_stream(&main, &mut sessions, application, session_app, 2, parent);
    let uni_stream = StreamContextId::from(Index::new(0, 1));
    {
        let (uni_rx, _) = sessions.fifo_pair(uni).expect("uni RX fifo");
        uni_rx.enqueue(b"ignored");
    }
    builtin_rx_on(&main, &mut sessions, uni, u64::from(uni_stream))
        .expect("peer uni stream RX is a no-op");
    assert!(
        sessions.upper_session(uni).is_none(),
        "no upper request Session for a peer uni stream"
    );
    let (uni_rx, _) = sessions.fifo_pair(uni).expect("uni RX fifo");
    assert_eq!(uni_rx.max_dequeue(), 7, "uni FIFO bytes stay readable");

    // A bidi request stream dispatch feeds and publishes.
    let child = construct_stream(&mut sessions, application, session_app, 3, parent, false);
    accept_on(&main, &mut sessions, child, 0).expect("accept bidi request stream");
    let stream = StreamContextId::from(Index::new(1, 1));
    let frame = zero_body_headers_frame();
    sessions
        .fifo_pair(child)
        .expect("lower RX fifo")
        .0
        .enqueue(&frame);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("bidi request stream publishes");

    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created for the bidi stream");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    assert_eq!(
        upper_rx.max_dequeue(),
        88 + 11 + 1,
        "the bidi request publishes"
    );
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(lower_rx.max_dequeue(), 0, "the consumed bytes dequeue");
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no reset for root, uni, or bidi"));
}

/// `builtin_rx_on` publishes one completed zero-body request HEADERS section
/// from the lower transport RX FIFO to the upper Session RX FIFO: the upper
/// request Session is created on first RX, the exact consumed lower bytes
/// dequeue after the upper commit, and the pending slot is cleared only
/// after the commit, mirroring VPP dispatching the recorded request to the
/// app (http3.c:456-475).
#[test]
fn builtin_rx_publishes_zero_body_headers_to_upper_session() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    // The lower RX FIFO carries one complete HEADERS frame with a
    // QPACK-encoded GET field section of only pseudo fields (no
    // Content-Length, so the request is body-less).
    let frame = zero_body_headers_frame();
    sessions
        .fifo_pair(child)
        .expect("lower RX fifo")
        .0
        .enqueue(&frame);

    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("builtin RX publishes the zero-body request");

    // The upper request Session exists and the request is published in the
    // VPP inbound layout: 88-byte header + authority "example.com" (11) +
    // path "/" (1), with empty query, header list, and body.
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    assert_eq!(
        upper_rx.max_dequeue(),
        88 + 11 + 1,
        "published inbound request size"
    );
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the exact consumed lower bytes are dequeued"
    );

    // The pending slot is cleared only after the upper FIFO commit, and the
    // body accumulator was installed from the declared (absent)
    // Content-Length: a body-less request that rejects any DATA.
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(
            http.pending_field_section(stream, child)
                .expect("live stream"),
            None,
            "pending slot cleared after the upper commit"
        );
        let stream_context = http
            .get_stream_for_session(stream, child)
            .expect("live stream context");
        assert!(
            matches!(stream_context.body, BodyAccumulator::NoBody),
            "no declared Content-Length installs a body-less accumulator"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// A single HEADERS frame split across two RX callbacks: each callback
/// dequeues exactly the bytes its reader consumed and never re-feeds bytes
/// already in the reader's partial-frame state (VPP drains each callback's
/// processed bytes, `max_deq - left_deq`, http3.c:1798, and never re-reads a
/// frame's bytes), so the frame completes on the second callback and
/// publishes exactly as a single-callback HEADERS does.
#[test]
fn builtin_rx_fragmented_headers_consumes_each_byte_once_across_callbacks() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    let frame = zero_body_headers_frame();
    assert!(frame.len() >= 4, "fixture splits into two non-empty halves");
    // The lower FIFO receives the single frame across two RX callbacks: the
    // first feeds the frame prefix and half the payload, the second the
    // remainder.
    let split = 2 + (frame.len() - 2) / 2;
    sessions
        .fifo_pair(child)
        .expect("lower RX fifo")
        .0
        .enqueue(&frame[..split]);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("a partial frame returns Ok without publishing");

    // The partial frame moved into the reader's state: nothing is published
    // and no upper request Session exists yet (VPP records the request and
    // dispatches the app only when the full HEADERS block arrives,
    // http3.c:835-899 then http3.c:456-475); the exact fed bytes are
    // dequeued, so the next callback starts at the first byte the reader
    // has not seen.
    {
        assert!(
            sessions.upper_session(child).is_none(),
            "no upper request Session for an incomplete HEADERS frame"
        );
        let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
        assert_eq!(
            lower_rx.max_dequeue(),
            0,
            "partial bytes move into the reader state, not the FIFO"
        );
    }

    // The remainder completes the frame and commits the same publication as
    // a single-callback HEADERS.
    sessions
        .fifo_pair(child)
        .expect("lower RX fifo")
        .0
        .enqueue(&frame[split..]);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("the completing bytes publish the request");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    assert_eq!(
        upper_rx.max_dequeue(),
        88 + 11 + 1,
        "published inbound request size"
    );
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the exact consumed lower bytes are dequeued"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(
            http.pending_field_section(stream, child)
                .expect("live stream"),
            None,
            "pending slot cleared after the upper commit"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// An empty HEADERS field section is a request-stream error, not a QPACK
/// connection decompression failure: VPP checks `req->fh.length == 0` before
/// `qpack_parse_request` and terminates the request stream with
/// HTTP3_ERROR_MESSAGE_ERROR (http3.c:860-869, RFC 9114 Section 4.1.2).
#[test]
fn builtin_rx_empty_headers_resets_the_stream_with_message_error() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);
    RESET_CALLS.with(|calls| calls.set(0));
    RESET_CODE.with(|seen| seen.set(0));

    // One empty HEADERS frame: type 0x01, zero payload length.
    sessions
        .fifo_pair(child)
        .expect("lower RX fifo")
        .0
        .enqueue(&[0x01, 0x00]);

    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("empty HEADERS resets the stream, not the publication");

    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 1, "stream reset once"));
    RESET_CODE.with(|seen| {
        assert_eq!(
            seen.get(),
            ErrorCode::MessageError.value(),
            "reset with HTTP3_ERROR_MESSAGE_ERROR"
        )
    });
    assert!(
        sessions.upper_session(child).is_none(),
        "no upper request Session for an empty HEADERS field section"
    );
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the empty frame bytes are dequeued"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(
            http.pending_field_section(stream, child)
                .expect("live stream"),
            None,
            "no section retained for an empty HEADERS"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// FIFO-capacity backpressure on a HEADERS publication is transient: the
/// callback returns `Ok(())` with the retained pending section and the
/// unconsumed lower bytes intact — the exact dequeue happens only after a
/// successful upper commit — so the next RX callback retries the same
/// section against the already-attached upper (VPP stream-vs-connection
/// error split, http3.c:1107-1113).
#[test]
fn builtin_rx_headers_capacity_retry_preserves_pending_section_and_lower_bytes() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    let frame = zero_body_headers_frame();
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    // Two complete inbound requests: the first creates the upper Session,
    // the second is the one the capacity retry will publish.
    lower_rx.enqueue(&frame);
    lower_rx.enqueue(&frame);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("the first HEADERS publishes");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    let published = upper_rx.max_dequeue();
    assert_eq!(published, 88 + 11 + 1, "the zero-body request publishes");

    // Fill the upper RX FIFO so the second inbound request cannot fit.
    let filler = vec![0xabu8; upper_rx.max_enqueue()];
    upper_rx.enqueue(&filler);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("a full upper FIFO is transient backpressure, not an error");

    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        frame.len(),
        "the retried request stays readable"
    );
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    assert_eq!(
        upper_rx.max_dequeue(),
        filler.len() + published,
        "no bytes were published into the full upper FIFO"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        let pending = http
            .pending_field_section(stream, child)
            .expect("live request stream")
            .expect("the retained field section survives the capacity retry");
        assert_eq!(
            pending.consumed,
            frame.len(),
            "the retained section keeps its exact lower-FIFO consumed count"
        );
        assert_eq!(
            pending.encoded.as_slice(),
            &frame[2..],
            "the retained section is the unmodified encoded field section"
        );
        Ok(())
    })
    .expect("worker introspection");

    // Drain the upper FIFO and retry: the callback publishes the exact
    // retained request and dequeues the exact lower bytes only after the
    // upper commit.
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    upper_rx.dequeue_drop(upper_rx.max_dequeue());
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("the retry publishes the retained request");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    assert_eq!(
        upper_rx.max_dequeue(),
        published,
        "the exact retained request is published after the drain"
    );
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the exact retried bytes are dequeued only after the upper commit"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert!(
            http.pending_field_section(stream, child)
                .expect("live request stream")
                .is_none(),
            "the pending slot is cleared after a successful publication"
        );
        Ok(())
    })
    .expect("worker introspection");
}

/// `builtin_rx_on` forwards a completed DATA frame's payload to the upper
/// Session RX FIFO through the shared body publication
/// (`HttpWorker::process_request_data`), advancing the body accumulator, and
/// dequeues the exact lower bytes only after the upper commit (VPP
/// `http3_req_state_transport_io_more_data`, http3.c:1184-1263, publishes
/// each DATA frame's payload to the app FIFO).
#[test]
fn builtin_rx_data_publishes_chunk_advances_body_and_dequeues_after_commit() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    let frame = content_length_headers_frame(3);
    let data_frame: &[u8] = &[0x00, 0x03, b'a', b'b', b'c'];
    let mut combined = frame.clone();
    combined.extend_from_slice(data_frame);
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&combined);

    builtin_rx_on(&main, &mut sessions, child, u64::from(stream)).expect("the HEADERS publishes");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    let request_bytes = upper_rx.max_dequeue();
    assert!(
        request_bytes > 88 + 11 + 1,
        "the request with a declared Content-Length publishes more than the zero-body request"
    );
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        data_frame.len(),
        "the DATA frame stays readable for the next RX callback"
    );

    builtin_rx_on(&main, &mut sessions, child, u64::from(stream)).expect("the DATA publishes");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    assert_eq!(
        upper_rx.max_dequeue(),
        request_bytes + 3,
        "the DATA chunk is appended after the published request"
    );
    let mut all = [0u8; 256];
    assert_eq!(
        upper_rx.peek(0, all.len(), &mut all),
        request_bytes + 3,
        "the published request and chunk are readable"
    );
    assert_eq!(
        &all[request_bytes..request_bytes + 3],
        b"abc",
        "the exact chunk bytes are published after the request"
    );
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the exact DATA bytes are dequeued only after the upper commit"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        let context = http
            .get_stream_for_session(stream, child)
            .expect("live stream");
        assert_eq!(
            context.body,
            BodyAccumulator::Complete,
            "the body accumulator advanced past the published chunk"
        );
        Ok(())
    })
    .expect("worker introspection");
}

/// FIFO-capacity backpressure on a completed DATA frame is never retryable:
/// the reader already accounted for the frame's bytes (`payload_len`
/// advanced over the rejected chunk, request_frame_reader.rs:216-258), so a
/// retry would re-feed reader-consumed bytes into the frame reader and
/// re-publish duplicated body bytes (VPP never faces this: it checks the
/// app FIFO before draining any transport bytes, http3.c:1211-1218). The
/// request stream resets with HTTP3_ERROR_INTERNAL_ERROR and the
/// app-visible request ends: the upper request Session is removed (VPP
/// `http3_stream_terminate` → `session_transport_reset_notify`,
/// http3.c:140-149, session.c:1165-1180), the reader-accounted bytes
/// dequeue exactly once, and the stream context stays live with the body
/// accumulator unchanged.
#[test]
fn builtin_rx_data_capacity_resets_without_retry_or_duplicate() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    let frame = content_length_headers_frame(3);
    let mut combined = frame.clone();
    combined.extend_from_slice(&[0x00, 0x03, b'a', b'b', b'c']);
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&combined);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream)).expect("the HEADERS publishes");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");

    // Fill the upper FIFO so the DATA frame cannot be published.
    let filler = vec![0xabu8; upper_rx.max_enqueue()];
    upper_rx.enqueue(&filler);
    RESET_CALLS.with(|calls| calls.set(0));
    RESET_CODE.with(|seen| seen.set(0));
    RESET_SESSION.with(|seen| seen.set(0));
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("capacity on a completed DATA frame aborts the request, not the publication");
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 1, "the stream resets exactly once"));
    RESET_CODE.with(|seen| {
        assert_eq!(
            seen.get(),
            ErrorCode::InternalError.value(),
            "reset with HTTP3_ERROR_INTERNAL_ERROR"
        )
    });
    RESET_SESSION.with(|seen| {
        assert_eq!(
            seen.get(),
            child.get(),
            "the reset targets the lower request stream, never the upper"
        )
    });
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the reader-accounted DATA bytes dequeue with the stream error, never retried"
    );
    assert!(
        !sessions.has_session(upper),
        "the app-visible request ends: the upper request Session is removed"
    );
    assert!(
        sessions.has_session(child),
        "the lower stream Session survives the abort"
    );
    assert!(
        sessions.upper_session(child).is_none(),
        "the removed upper is no longer attached to the lower"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        let context = http
            .get_stream_for_session(stream, child)
            .expect("live stream");
        assert_eq!(
            context.body,
            BodyAccumulator::Receiving { remaining: 3 },
            "the rejected publication does not mutate the body accumulator"
        );
        Ok(())
    })
    .expect("worker introspection");
}

/// A trailing HEADERS (a request trailer the reader's ordering phase
/// accepts, RFC 9114 Section 4.1) arriving after `ResetStreamAbortRequest`
/// aborted the request is drained, not published: the request stream
/// context stays live for RX drain only (VPP `http3_stream_terminate`
/// resets the transport stream and never re-dispatches to the app,
/// http3.c:140-149, with `http3_stream_transport_rx_drain` discarding the
/// remaining bytes, http3.c:1571-1575), so the trailing HEADERS must not
/// re-enter the ready path, recreate the upper request Session, or fire a
/// second `app.connected`; the trailing bytes dequeue exactly once and no
/// second stream reset is sent.
#[test]
fn builtin_rx_post_abort_trailing_headers_drains_without_recreating_upper() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    // HEADERS (declaring a 3-byte body) and a complete DATA frame arrive
    // together: the HEADERS publishes and creates the upper request
    // Session, leaving the DATA frame readable.
    let mut combined = content_length_headers_frame(3);
    combined.extend_from_slice(&[0x00, 0x03, b'a', b'b', b'c']);
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&combined);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream)).expect("the HEADERS publishes");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");

    // Fill the upper FIFO so the DATA publication fails and aborts the
    // request: the upper request Session is removed, the request stream
    // resets exactly once, and the stream context stays live for drain.
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    let filler = vec![0xabu8; upper_rx.max_enqueue()];
    upper_rx.enqueue(&filler);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("capacity on a completed DATA frame aborts the request");
    assert!(
        !sessions.has_session(upper),
        "the abort removes the upper request Session"
    );
    assert!(
        sessions.upper_session(child).is_none(),
        "the removed upper is no longer attached to the lower"
    );
    RESET_CALLS.with(|calls| calls.set(0));

    // A trailing HEADERS arrives on the retained stream: it must drain
    // dequeue-only, never re-enter the ready path.
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&zero_body_headers_frame());
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("post-abort trailing HEADERS drains");
    assert!(
        sessions.upper_session(child).is_none(),
        "the trailing HEADERS does not recreate the upper request Session"
    );
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the trailing HEADERS bytes dequeue exactly once"
    );
    RESET_CALLS.with(|calls| {
        assert_eq!(calls.get(), 0, "the drain sends no second stream reset")
    });
}

/// A disconnect/FIN arriving after `ResetStreamAbortRequest` aborted the
/// request is clean, never a second request-incomplete failure: the stream
/// is already terminated and drain-only (upper removed, `http3_stream_terminate`
/// notified the app and reset the transport stream, http3.c:140-149), so
/// the FIN must not revalidate the half-received body — VPP never re-runs
/// the request-incomplete check on an app-closed/terminated request
/// (`http3_transport_stream_close_callback` fires it only while
/// `req_state < HTTP_REQ_STATE_WAIT_APP_REPLY`, http3.c:2312-2319). The FIN
/// still releases the live lower stream context — the upper removal is a
/// no-op — in the delete-notify-then-free order (http3.c:2459-2462), with
/// no extra reset or connection close, and the SessionWorker follow-on
/// cleanup on the released identity is a no-op too.
#[test]
fn disconnect_after_abort_is_clean_without_revalidation_or_recreation() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    // HEADERS (declaring a 3-byte body) and a complete DATA frame arrive
    // together: the HEADERS publishes and creates the upper request
    // Session, leaving the DATA frame readable.
    let mut combined = content_length_headers_frame(3);
    combined.extend_from_slice(&[0x00, 0x03, b'a', b'b', b'c']);
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&combined);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream)).expect("the HEADERS publishes");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");

    // Fill the upper FIFO so the DATA publication fails and aborts the
    // request: the upper request Session is removed, the request stream
    // resets exactly once, and the stream context stays live for drain with
    // the body still half-received — a FIN must not revalidate it.
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    let filler = vec![0xabu8; upper_rx.max_enqueue()];
    upper_rx.enqueue(&filler);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("capacity on a completed DATA frame aborts the request");
    assert!(
        !sessions.has_session(upper),
        "the abort removes the upper request Session"
    );
    CLOSE_CALLS.with(|calls| calls.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    // The FIN arrives on the drain-only stream: no RequestIncomplete
    // revalidation, no upper re-creation, no extra reset or close, and the
    // live lower stream context is released.
    disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect("post-abort FIN is clean");
    assert!(
        !sessions.has_session(upper),
        "the FIN does not recreate the upper request Session"
    );
    assert!(
        sessions.has_session(child),
        "the lower stream Session survives the FIN"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives the FIN"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no connection close"));
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no extra stream reset"));
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(
            http.stream_len(),
            0,
            "the FIN releases the live lower stream context"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");

    // The SessionWorker follow-on cleanup on the released identity is a
    // clean no-op, exactly as after a normal FIN.
    cleanup_on(&main, &mut sessions, child, u64::from(stream))
        .expect("cleanup after the aborted stream FIN is a no-op");
}

/// FIFO-capacity backpressure on a fragmented (incomplete) DATA chunk is not
/// retryable: the reader already advanced `payload_len` over the very bytes
/// the upper FIFO rejected, so a retry would re-feed accounted bytes into the
/// in-progress frame and corrupt the body (VPP never faces this: it checks
/// the app FIFO before draining any transport bytes, http3.c:1211-1218). The
/// request stream resets with HTTP3_ERROR_INTERNAL_ERROR, the app-visible
/// request ends (the upper request Session is removed), the reader-accounted
/// bytes dequeue exactly once, and the stream context stays live with the
/// body accumulator unchanged.
#[test]
fn builtin_rx_fragmented_data_capacity_resets_without_corrupt_body_or_retry() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    // HEADERS declares a three-byte body and publishes, attaching the upper
    // request Session.
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&content_length_headers_frame(3));
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream)).expect("the HEADERS publishes");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");

    // The first fragment of a three-byte DATA frame: "a" surfaces with
    // completed=false and publishes while the upper FIFO still has room.
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&[0x00, 0x03, b'a']);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("the first fragment publishes");
    main.with_worker(DataWorkerId::new(0), |http| {
        let context = http
            .get_stream_for_session(stream, child)
            .expect("live stream");
        assert_eq!(
            context.body,
            BodyAccumulator::Receiving { remaining: 2 },
            "the committed fragment advances the body"
        );
        Ok(())
    })
    .expect("worker introspection");

    // The remaining "b" arrives while the upper FIFO is full: its publication
    // is rejected with capacity on a completed=false chunk, which the reader
    // cannot re-feed (its payload_len already advanced over the byte), so the
    // request stream resets and the app-visible request ends instead of
    // retrying.
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    let filler = vec![0xabu8; upper_rx.max_enqueue()];
    upper_rx.enqueue(&filler);
    RESET_CALLS.with(|calls| calls.set(0));
    RESET_CODE.with(|seen| seen.set(0));
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&[b'b']);
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("capacity on a fragmented chunk aborts the request, not the publication");

    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 1, "the stream resets exactly once"));
    RESET_CODE.with(|seen| {
        assert_eq!(
            seen.get(),
            ErrorCode::InternalError.value(),
            "reset with HTTP3_ERROR_INTERNAL_ERROR"
        )
    });
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the reader-accounted byte dequeues with the stream error, never retried"
    );
    assert!(
        !sessions.has_session(upper),
        "the app-visible request ends: the upper request Session is removed"
    );
    assert!(
        sessions.has_session(child),
        "the lower stream Session survives the abort"
    );
    assert!(
        sessions.upper_session(child).is_none(),
        "the removed upper is no longer attached to the lower"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        let context = http
            .get_stream_for_session(stream, child)
            .expect("live stream");
        assert_eq!(
            context.body,
            BodyAccumulator::Receiving { remaining: 2 },
            "the rejected fragment does not mutate the body accumulator"
        );
        Ok(())
    })
    .expect("worker introspection");
}

/// After a DATA publication-failure abort, the request stream context stays
/// live and the peer's remaining RX bytes keep draining silently: a later
/// `builtin_rx` feed parses the next DATA frame, dequeues its bytes exactly
/// once, and publishes nothing (the upper Session is gone) with no second
/// reset and no error — the same dequeue-only behavior the feed loop already
/// applies to a stream that never created an upper. The `cleanup` callback
/// releases the context when the lower Session is eventually removed.
#[test]
fn builtin_rx_data_publish_failure_abort_drains_remaining_rx() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    // HEADERS declares a three-byte body and publishes, attaching the upper
    // request Session.
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&content_length_headers_frame(3));
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream)).expect("the HEADERS publishes");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");

    // The full DATA frame cannot be published into the filled upper FIFO:
    // the request stream resets and the upper request Session is removed.
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    let filler = vec![0xabu8; upper_rx.max_enqueue()];
    upper_rx.enqueue(&filler);
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&[0x00, 0x03, b'a', b'b', b'c']);
    RESET_CALLS.with(|calls| calls.set(0));
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("capacity aborts the request, not the publication");
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 1, "the stream resets exactly once"));
    assert!(!sessions.has_session(upper), "the upper request Session is removed");

    // The peer's remaining RX bytes (here a fresh DATA frame past the
    // rejected one) drain silently: parsed, dequeued, nothing published.
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&[0x00, 0x02, b'd', b'e']);
    RESET_CALLS.with(|calls| calls.set(0));
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("the drain feed parses and dequeues without an error");
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no second reset"));
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the drained frame's bytes dequeue exactly once, never re-fed"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        let context = http
            .get_stream_for_session(stream, child)
            .expect("the request stream context stays live for the RX drain");
        assert_eq!(
            context.body,
            BodyAccumulator::Receiving { remaining: 3 },
            "the drained bytes publish to no upper and never advance the body"
        );
        Ok(())
    })
    .expect("worker introspection");
}

/// A DATA frame on a request with no declared Content-Length is a typed
/// stream protocol error, not a silent discard: the request stream is reset
/// with FRAME_UNEXPECTED (RFC 9114 Section 4.1.2; the shared body
/// accumulator maps `DataWithoutDeclaredLength` to `FrameUnexpected`), the
/// reader-consumed bytes dequeue, and nothing is published.
#[test]
fn builtin_rx_bodyless_data_resets_stream_with_frame_unexpected() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (_parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);

    let mut combined = zero_body_headers_frame();
    combined.extend_from_slice(&[0x00, 0x03, b'a', b'b', b'c']);
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    lower_rx.enqueue(&combined);

    builtin_rx_on(&main, &mut sessions, child, u64::from(stream)).expect("the HEADERS publishes");
    let upper = sessions
        .upper_session(child)
        .expect("upper request Session created");
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    let published = upper_rx.max_dequeue();
    RESET_CALLS.with(|calls| calls.set(0));
    RESET_CODE.with(|seen| seen.set(0));
    builtin_rx_on(&main, &mut sessions, child, u64::from(stream))
        .expect("body-less DATA is a typed stream protocol error, not a publish error");
    RESET_CALLS
        .with(|calls| assert_eq!(calls.get(), 1, "the request stream is reset exactly once"));
    RESET_CODE.with(|seen| {
        assert_eq!(
            seen.get(),
            ErrorCode::FrameUnexpected.value(),
            "body-less DATA is FRAME_UNEXPECTED"
        );
    });
    let (upper_rx, _) = sessions.fifo_pair(upper).expect("upper RX fifo");
    assert_eq!(upper_rx.max_dequeue(), published, "no DATA was published");
    let (lower_rx, _) = sessions.fifo_pair(child).expect("lower RX fifo");
    assert_eq!(
        lower_rx.max_dequeue(),
        0,
        "the reader-consumed DATA bytes dequeue with the stream error"
    );
}

/// A publication retry borrows the same retained section without consuming
/// it: the encoded block and its exact lower-FIFO consumed count stay
/// identical across repeated borrows until the seam clears the slot.
#[test]
fn pending_field_section_borrow_survives_retry_until_clear() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    let (read, consumed) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'i'])
        .expect("feed one complete HEADERS frame");
    let RequestFrameRead::Headers(encoded) = read else {
        panic!("a complete HEADERS frame must surface its encoded field section");
    };
    worker
        .retain_pending_field_section(stream, session, PendingFieldSection { encoded, consumed })
        .expect("retain the completed field section");

    // The first publication attempt borrows the section.
    let first = worker
        .pending_field_section(stream, session)
        .expect("live stream")
        .expect("a retained section is pending");
    // A retry borrows again, with no clear in between: the same bytes and
    // the same exact consumed count must still be observable.
    let second = worker
        .pending_field_section(stream, session)
        .expect("live stream")
        .expect("the section survives the retry");
    assert_eq!(
        first.encoded, second.encoded,
        "the retry re-borrows the same encoded block"
    );
    assert_eq!(
        first.consumed, second.consumed,
        "the retry re-borrows the same exact consumed count"
    );
    assert_eq!(first.encoded, b"hi".to_vec(), "the encoded block is intact");
    assert_eq!(first.consumed, 4, "the lower-FIFO consumed count is intact");

    // The seam clears the slot once the section is finally published.
    worker
        .clear_pending_field_section(stream, session)
        .expect("clear the pending section");
    assert_eq!(
        worker.pending_field_section(stream, session).unwrap(),
        None,
        "the slot is empty after clear"
    );
}

/// The pending slot rejects rather than silently replaces, and stale stream
/// identities can never observe a reused slot.
#[test]
fn pending_field_section_rejects_overflow_and_stale_identities() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    // The initial HEADERS and its optional trailer (RFC 9114 Section 4.1)
    // complete on the same stream; both surface by value with the exact
    // lower-FIFO consumed count of their feeds.
    let (first, first_consumed) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'i'])
        .expect("feed the initial HEADERS frame");
    let RequestFrameRead::Headers(first) = first else {
        panic!("the initial HEADERS must surface its field section");
    };
    worker
        .retain_pending_field_section(
            stream,
            session,
            PendingFieldSection {
                encoded: first,
                consumed: first_consumed,
            },
        )
        .expect("retain the initial field section");

    let (trailer, trailer_consumed) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'x'])
        .expect("feed the trailer HEADERS frame");
    let RequestFrameRead::Headers(trailer) = trailer else {
        panic!("the trailer HEADERS must surface its field section");
    };
    // A section is already pending: the retain is rejected with the newer
    // section returned unreplaced, never silently replaced or dropped.
    let error = worker
        .retain_pending_field_section(
            stream,
            session,
            PendingFieldSection {
                encoded: trailer,
                consumed: trailer_consumed,
            },
        )
        .expect_err("a pending section rejects a second retain");
    assert!(
        matches!(
            error,
            HttpWorkerError::PendingFieldSectionOverflow {
                stream: rejected_stream,
                ref section
            } if rejected_stream == stream
                && section.encoded == b"hx".to_vec()
                && section.consumed == trailer_consumed
        ),
        "overflow carries the rejected section back, got {error:?}"
    );
    let pending = worker
        .pending_field_section(stream, session)
        .expect("live stream")
        .expect("the initial section is still pending");
    assert_eq!(
        pending.encoded,
        b"hi".to_vec(),
        "the pending initial section survives the rejected retain"
    );
    assert_eq!(
        pending.consumed, first_consumed,
        "its exact lower-FIFO consumed count survives too"
    );

    // The released stream's identity is stale: it can neither borrow nor
    // clear nor retain on the reused slot, and the fresh stream starts
    // empty.
    worker.remove_stream(stream).expect("release the stream");
    let next = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate a stream reusing the released slot");
    assert!(
        worker.pending_field_section(stream, session).is_err(),
        "stale stream identity cannot borrow on the reused slot"
    );
    assert!(
        worker.clear_pending_field_section(stream, session).is_err(),
        "stale stream identity cannot clear on the reused slot"
    );
    assert!(
        worker
            .retain_pending_field_section(
                stream,
                session,
                PendingFieldSection {
                    encoded: b"zz".to_vec(),
                    consumed: 2
                },
            )
            .is_err(),
        "stale stream identity cannot retain on the reused slot"
    );
    assert_eq!(
        worker.pending_field_section(next, session).unwrap(),
        None,
        "the fresh stream observes no pending section from the released stream"
    );
}

/// The emptied-but-recorded pending slot refills with an optional trailer
/// HEADERS (RFC 9114 Section 4.1) after the initial section was cleared:
/// the sink supports retain -> borrow -> clear -> retain trailer -> borrow
/// on one recorded slot, mirroring VPP reusing `req->headers` on the same
/// per-request `http_ctx_t` for the trailer section.
#[test]
fn pending_field_section_clear_then_trailer_refills_emptied_slot() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    // Initial HEADERS: retained, borrowed by the decode/publish seam, then
    // cleared.
    let (first, consumed) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'i'])
        .expect("feed the initial HEADERS frame");
    let RequestFrameRead::Headers(first) = first else {
        panic!("the initial HEADERS must surface its field section");
    };
    worker
        .retain_pending_field_section(
            stream,
            session,
            PendingFieldSection {
                encoded: first,
                consumed,
            },
        )
        .expect("retain the initial field section");
    let pending = worker
        .pending_field_section(stream, session)
        .expect("live stream")
        .expect("the initial section is pending");
    assert_eq!(
        pending.encoded,
        b"hi".to_vec(),
        "the initial section is borrowed"
    );
    assert_eq!(
        pending.consumed, 4,
        "the initial section's exact consumed count is borrowed"
    );
    worker
        .clear_pending_field_section(stream, session)
        .expect("clear the initial section");
    assert_eq!(
        worker.pending_field_section(stream, session).unwrap(),
        None,
        "the slot is empty after clearing"
    );

    // The trailer completes after the initial section was cleared: the same
    // recorded slot refills, and the trailer is borrowed in turn.
    let (trailer, consumed) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'x'])
        .expect("feed the trailer HEADERS frame");
    let RequestFrameRead::Headers(trailer) = trailer else {
        panic!("the trailer HEADERS must surface its field section");
    };
    worker
        .retain_pending_field_section(
            stream,
            session,
            PendingFieldSection {
                encoded: trailer,
                consumed,
            },
        )
        .expect("the emptied slot refills with the trailer");
    let pending = worker
        .pending_field_section(stream, session)
        .expect("live stream")
        .expect("the trailer section is pending");
    assert_eq!(
        pending.encoded,
        b"hx".to_vec(),
        "the refilled trailer section is borrowed"
    );
    assert_eq!(
        pending.consumed, 4,
        "the trailer's exact consumed count is borrowed"
    );
    worker
        .clear_pending_field_section(stream, session)
        .expect("clear the trailer section");
    assert_eq!(
        worker.pending_field_section(stream, session).unwrap(),
        None,
        "the slot is empty again after the trailer is cleared"
    );
}

/// A stream bound to one session rejects retain, borrow, and clear from a
/// foreign
/// session identity with the typed `StreamSessionMismatch` (the same
/// session-bound metadata check every other worker seam performs), and the
/// rejected calls leave the pending slot untouched.
#[test]
fn pending_field_section_rejects_foreign_session_identities() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let foreign = SessionId::from_raw(2);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    // Borrow and clear with a foreign session are rejected before touching
    // the slot.
    let error = worker
        .pending_field_section(stream, foreign)
        .expect_err("borrow with a foreign session must fail");
    assert!(
        matches!(
            error,
            HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected,
                actual
            } if s == stream && expected == foreign && actual == session
        ),
        "borrow typed mismatch expected, got {error:?}"
    );
    let error = worker
        .clear_pending_field_section(stream, foreign)
        .expect_err("clear with a foreign session must fail");
    assert!(
        matches!(
            error,
            HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected,
                actual
            } if s == stream && expected == foreign && actual == session
        ),
        "clear typed mismatch expected, got {error:?}"
    );

    // Retain with a foreign session is rejected the same way, and the
    // section is not left in the slot.
    let error = worker
        .retain_pending_field_section(
            stream,
            foreign,
            PendingFieldSection {
                encoded: b"zz".to_vec(),
                consumed: 2,
            },
        )
        .expect_err("retain with a foreign session must fail");
    assert!(
        matches!(
            error,
            HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected,
                actual
            } if s == stream && expected == foreign && actual == session
        ),
        "retain typed mismatch expected, got {error:?}"
    );
    assert_eq!(
        worker.pending_field_section(stream, session).unwrap(),
        None,
        "the rejected retain left no section in the slot"
    );

    // The stream still accepts its own session afterwards.
    worker
        .retain_pending_field_section(
            stream,
            session,
            PendingFieldSection {
                encoded: b"hi".to_vec(),
                consumed: 2,
            },
        )
        .expect("retain with the bound session still works");
    let pending = worker
        .pending_field_section(stream, session)
        .expect("live stream")
        .expect("the bound session's section is pending");
    assert_eq!(
        pending.encoded,
        b"hi".to_vec(),
        "the bound session retains and borrows normally"
    );
    assert_eq!(
        pending.consumed, 2,
        "the bound session's exact consumed count is borrowed"
    );
}

/// The worker-owned release boundary for bidi request stream cleanup
/// (`release_request_stream`, the future FIN/reset callback entry):
/// releasing a live bidi request stream frees its lazily allocated
/// request reader and its retained pending field section through the
/// existing `remove_stream` ownership path. Mirrors VPP
/// `http3_stream_cleanup_callback` → `http3_stream_free_req`
/// (http3.c:2440-2463, 59-78), where the bidi request stream's
/// per-request state is freed as the stream is cleaned up.
#[test]
fn release_request_stream_frees_reader_and_pending_slot() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    // Establish both worker-owned slots: the lazily allocated request
    // reader on first feed and the retained HEADERS field section.
    worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'i'])
        .expect("feed one complete HEADERS frame");
    worker
        .retain_pending_field_section(
            stream,
            session,
            PendingFieldSection {
                encoded: b"hi".to_vec(),
                consumed: 2,
            },
        )
        .expect("retain the completed field section");
    assert!(
        worker
            .get_stream(stream)
            .expect("live stream")
            .request_reader
            .is_some(),
        "the feed recorded the request reader slot"
    );
    assert!(
        worker
            .get_stream(stream)
            .expect("live stream")
            .pending_field_section
            .is_some(),
        "the retain recorded the pending section slot"
    );

    // The single release frees both slots and the stream context: the
    // identity is dead for the read path, the retention path, and
    // further release.
    worker
        .release_request_stream(stream, session)
        .expect("release the live bidi request stream");
    assert!(
        matches!(
            worker.process_request_bytes(stream, session, &[0x01]),
            Err(RequestReadError::Worker(HttpWorkerError::StreamMissing { stream: s })) if s == stream
        ),
        "the released identity is dead for the read path"
    );
    assert!(
        worker.pending_field_section(stream, session).is_err(),
        "the released identity is dead for the retention path"
    );
    assert!(
        matches!(worker.get_stream(stream), Err(HttpWorkerError::StreamMissing { stream: s }) if s == stream),
        "the released stream context is gone"
    );
}

/// A stale stream identity can never release a reused slot: after the
/// release, the freed slot is reallocated to a fresh bidi request stream
/// at a new generation, and releasing through the stale identity is
/// rejected before any removal, leaving the fresh stream's reader and
/// pending section untouched.
#[test]
fn release_request_stream_stale_identity_cannot_touch_reused_slot() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let first = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate the first bidi request stream");
    worker
        .process_request_bytes(first, session, &[0x01, 0x02, b'h', b'i'])
        .expect("feed HEADERS on the first stream");
    worker
        .retain_pending_field_section(
            first,
            session,
            PendingFieldSection {
                encoded: b"hi".to_vec(),
                consumed: 2,
            },
        )
        .expect("retain a section on the first stream");
    worker
        .release_request_stream(first, session)
        .expect("release the first stream");

    // The freed slot is reused by a fresh stream at a new generation.
    let second = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate a stream reusing the released slot");
    worker
        .process_request_bytes(second, session, &[0x01, 0x02, b'h', b'x'])
        .expect("feed HEADERS on the fresh stream");
    worker
        .retain_pending_field_section(
            second,
            session,
            PendingFieldSection {
                encoded: b"hx".to_vec(),
                consumed: 2,
            },
        )
        .expect("retain a section on the fresh stream");

    // The stale identity is rejected before any removal; the fresh
    // stream's reader and pending section survive untouched.
    assert!(
        matches!(
            worker.release_request_stream(first, session),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == first
        ),
        "releasing through the stale identity is rejected"
    );
    let fresh = worker.get_stream(second).expect("fresh stream still live");
    assert!(
        fresh.request_reader.is_some(),
        "the fresh stream's reader is untouched"
    );
    assert!(
        fresh.pending_field_section.is_some(),
        "the fresh stream's pending section is untouched"
    );
    let pending = worker
        .pending_field_section(second, session)
        .expect("live stream")
        .expect("the fresh stream still owns its pending section");
    assert_eq!(
        pending.encoded,
        b"hx".to_vec(),
        "the fresh stream still owns its pending section"
    );
    assert_eq!(
        pending.consumed, 2,
        "the fresh stream's exact consumed count is intact"
    );
}

/// Releasing through a foreign session is a typed
/// `StreamSessionMismatch` that mutates nothing: the live stream stays
/// live and can still be released through its bound session.
#[test]
fn release_request_stream_foreign_session_is_typed_mismatch_without_mutation() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let foreign = SessionId::from_raw(2);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    let error = worker
        .release_request_stream(stream, foreign)
        .expect_err("releasing through a foreign session must fail");
    assert!(
        matches!(
            error,
            HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected,
                actual
            } if s == stream && expected == foreign && actual == session
        ),
        "typed session mismatch expected, got {error:?}"
    );
    assert!(
        worker
            .get_stream(stream)
            .expect("stream still live")
            .request_reader
            .is_none(),
        "the failed release mutated nothing"
    );
    worker
        .release_request_stream(stream, session)
        .expect("the bound session still releases the stream");
}

/// A repeated release is pinned to a typed `StreamMissing`, never a
/// silent no-op, matching `remove_stream` idempotency.
#[test]
fn release_request_stream_repeated_release_is_stream_missing() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    worker
        .release_request_stream(stream, session)
        .expect("first release frees the stream");
    assert!(
        matches!(
            worker.release_request_stream(stream, session),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == stream
        ),
        "a repeated release is a typed StreamMissing"
    );
}

/// The bidi/request-role guard rejects a live non-bidi stream with a
/// typed `RequestStreamNotBidi` and mutates nothing; the uni stream
/// remains live and is released by `remove_stream` as before.
#[test]
fn release_request_stream_rejects_non_bidi_without_mutation() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let uni = worker
        .allocate_stream(session, parent, SessionStreamDirection::Uni)
        .expect("allocate a peer uni stream");

    let error = worker
        .release_request_stream(uni, session)
        .expect_err("releasing a uni stream must fail");
    assert!(
        matches!(
            error,
            HttpWorkerError::RequestStreamNotBidi {
                stream: s,
                direction: SessionStreamDirection::Uni
            } if s == uni
        ),
        "typed non-bidi rejection expected, got {error:?}"
    );
    assert!(
        worker.get_stream(uni).is_ok(),
        "the rejected release left the uni stream live"
    );
    worker
        .remove_stream(uni)
        .expect("remove_stream still owns uni stream release");
}

// --- request-body DATA ownership and publication ----------------------------

/// A local FIFO of `capacity` data bytes backed by a private 1 MiB segment,
/// as the http_common publish tests use.
fn local_fifo(capacity: usize) -> Fifo {
    Fifo::new(Segment::local(1 << 20), capacity).expect("local FIFO")
}

/// A worker with one live bidirectional request stream bound to
/// `SessionId::from_raw(1)`, plus its parent connection context.
fn request_stream_worker() -> (HttpWorker, SessionId, ContextId, StreamContextId) {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate a bidi request stream");
    (worker, session, parent, stream)
}

#[test]
fn install_request_body_length_rejects_stale_foreign_and_non_bidi_without_mutation() {
    let (mut worker, session, parent, stream) = request_stream_worker();
    let foreign = SessionId::from_raw(2);
    let uni = worker
        .allocate_stream(session, parent, SessionStreamDirection::Uni)
        .expect("allocate a peer uni stream");
    worker
        .release_request_stream(stream, session)
        .expect("release the first bidi stream");
    let stale = stream;

    let error = worker
        .install_request_body_length(stale, session, Some(4))
        .expect_err("stale stream identity must be rejected");
    assert!(
        matches!(error, HttpWorkerError::StreamMissing { stream: s } if s == stale),
        "typed stale rejection expected, got {error:?}"
    );

    let error = worker
        .install_request_body_length(uni, session, Some(4))
        .expect_err("a uni stream is not a request stream");
    assert!(
        matches!(
            error,
            HttpWorkerError::RequestStreamNotBidi {
                stream: s,
                direction: SessionStreamDirection::Uni
            } if s == uni
        ),
        "typed non-bidi rejection expected, got {error:?}"
    );

    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate a second bidi request stream");
    let error = worker
        .install_request_body_length(stream, foreign, Some(4))
        .expect_err("foreign session must be rejected");
    assert!(
        matches!(
            error,
            HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected: e,
                actual: a
            } if s == stream && e == foreign && a == session
        ),
        "typed session mismatch expected, got {error:?}"
    );
    // The rejected installs left the accumulator at its `NoBody` default:
    // DATA is still unexpected until a valid install declares a length.
    let fifo = local_fifo(8192);
    let error = worker
        .process_request_data(stream, session, &fifo, b"data")
        .expect_err("uninstalled body must reject DATA");
    assert_eq!(error.error_code(), Some(ErrorCode::FrameUnexpected));
    worker
        .install_request_body_length(stream, session, Some(4))
        .expect("valid install on the bidi stream");
    worker
        .process_request_data(stream, session, &fifo, b"data")
        .expect("declared body accepts its DATA");
    let mut out = [0u8; 4];
    assert_eq!(fifo.peek(0, 4, &mut out), 4);
    assert_eq!(&out, b"data");
}

#[test]
fn process_request_data_rejects_stale_foreign_and_non_bidi_without_mutation() {
    let (mut worker, session, parent, stream) = request_stream_worker();
    let foreign = SessionId::from_raw(2);
    let uni = worker
        .allocate_stream(session, parent, SessionStreamDirection::Uni)
        .expect("allocate a peer uni stream");
    let fifo = local_fifo(8192);

    let error = worker
        .process_request_data(uni, session, &fifo, b"data")
        .expect_err("a uni stream is not a request stream");
    assert!(
        matches!(
            error,
            RequestReadError::Worker(HttpWorkerError::RequestStreamNotBidi {
                stream: s,
                direction: SessionStreamDirection::Uni
            }) if s == uni
        ),
        "typed non-bidi rejection expected, got {error:?}"
    );

    let error = worker
        .process_request_data(stream, foreign, &fifo, b"data")
        .expect_err("foreign session must be rejected");
    assert!(
        matches!(
            error,
            RequestReadError::Worker(HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected: e,
                actual: a
            }) if s == stream && e == foreign && a == session
        ),
        "typed session mismatch expected, got {error:?}"
    );

    worker
        .release_request_stream(stream, session)
        .expect("release the stream");
    let error = worker
        .process_request_data(stream, session, &fifo, b"data")
        .expect_err("stale stream identity must be rejected");
    assert!(
        matches!(
            error,
            RequestReadError::Worker(HttpWorkerError::StreamMissing { stream: s }) if s == stream
        ),
        "typed stale rejection expected, got {error:?}"
    );
    // None of the rejected feeds touched the FIFO or a live body.
    assert_eq!(fifo.max_enqueue(), 8192);

    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate a second bidi request stream");
    worker
        .install_request_body_length(stream, session, Some(6))
        .expect("install the declared length");
    worker
        .process_request_data(stream, session, &fifo, b"ab")
        .expect("first chunk publishes");
    worker
        .process_request_data(stream, session, &fifo, b"cdef")
        .expect("second chunk publishes");
    let mut out = [0u8; 6];
    assert_eq!(fifo.peek(0, 6, &mut out), 6);
    assert_eq!(&out, b"abcdef");
}

#[test]
fn process_request_data_overrun_rejects_before_fifo_or_body_mutation() {
    let (mut worker, session, _, stream) = request_stream_worker();
    worker
        .install_request_body_length(stream, session, Some(4))
        .expect("install the declared length");
    let fifo = local_fifo(8192);

    let error = worker
        .process_request_data(stream, session, &fifo, b"12345")
        .expect_err("a chunk beyond the declared body must be rejected");
    assert_eq!(error.error_code(), Some(ErrorCode::GeneralProtocolError));
    assert_eq!(
        fifo.max_enqueue(),
        8192,
        "the rejected chunk never touched the upper FIFO"
    );

    worker
        .process_request_data(stream, session, &fifo, b"1234")
        .expect("the full declared body publishes");
    let mut out = [0u8; 4];
    assert_eq!(fifo.peek(0, 4, &mut out), 4);
    assert_eq!(&out, b"1234");
    let error = worker
        .process_request_data(stream, session, &fifo, b"5")
        .expect_err("data after a complete body must be rejected");
    assert_eq!(error.error_code(), Some(ErrorCode::FrameUnexpected));
}

#[test]
fn process_request_data_capacity_arms_dequeue_notification_without_body_change() {
    let (mut worker, session, _, stream) = request_stream_worker();
    worker
        .install_request_body_length(stream, session, Some(8))
        .expect("install the declared length");
    let tight = local_fifo(2);

    let error = worker
        .process_request_data(stream, session, &tight, b"12345678")
        .expect_err("the tight FIFO cannot hold the whole chunk");
    assert!(
        matches!(
            error,
            RequestReadError::Worker(HttpWorkerError::BodyChunkPublishFailed {
                stream: s,
                error: PublishError::Capacity { requested: 8, available: 2 },
            }) if s == stream
        ),
        "typed capacity rejection expected, got {error:?}"
    );
    assert!(
        tight.needs_deq_notification(1),
        "the capacity rejection armed the dequeue notification"
    );
    assert_eq!(
        tight.max_enqueue(),
        2,
        "the rejected chunk never touched the tight FIFO"
    );

    // The body still expects all 8 bytes: the identical retry against a
    // roomy FIFO completes it, so the failed publish never consumed them.
    let fifo = local_fifo(8192);
    worker
        .process_request_data(stream, session, &fifo, b"12345678")
        .expect("retry with room publishes the whole chunk");
    let mut out = [0u8; 8];
    assert_eq!(fifo.peek(0, 8, &mut out), 8);
    assert_eq!(&out, b"12345678");
}

#[test]
fn validate_request_finish_reports_incomplete_without_releasing_state() {
    let (mut worker, session, _, stream) = request_stream_worker();
    worker
        .install_request_body_length(stream, session, Some(4))
        .expect("install the declared length");
    let fifo = local_fifo(8192);
    worker
        .process_request_data(stream, session, &fifo, b"12")
        .expect("first half publishes");

    let error = worker
        .validate_request_finish(stream, session)
        .expect_err("FIN with a declared but unreceived body must fail");
    assert_eq!(error.error_code(), Some(ErrorCode::RequestIncomplete));
    assert!(
        worker.get_stream(stream).is_ok(),
        "the rejected FIN left the stream live and unreleased"
    );

    worker
        .process_request_data(stream, session, &fifo, b"34")
        .expect("second half publishes");
    worker
        .validate_request_finish(stream, session)
        .expect("FIN after the complete body passes");
    let error = worker
        .process_request_data(stream, session, &fifo, b"5")
        .expect_err("data after a complete body must be rejected");
    assert_eq!(error.error_code(), Some(ErrorCode::FrameUnexpected));
}

#[test]
fn remove_upper_session_rolls_back_upper_and_repeats_as_noop() {
    // VPP rollback of a newly-created upper is a direct session_free with no
    // app callback (session.c:782-801): the app was never notified, and the
    // owner link dies with the session. Hammer exposes the same rollback to
    // Session App plugins through the public `remove_upper_session` surface;
    // this test proves the cross-crate visibility and the typed no-op for a
    // stale ID.
    let (_main, mut sessions, application, session_app) = test_harness();
    let lower = construct_session(&mut sessions, application, session_app, 1);
    let upper = sessions
        .create_upper_session(lower, 0x55)
        .expect("create upper Session from the lower");

    sessions
        .remove_upper_session(upper)
        .expect("roll back the upper Session");

    assert!(!sessions.has_session(upper), "the upper is removed");
    assert!(sessions.has_session(lower), "the lower survives rollback");
    sessions
        .remove_upper_session(upper)
        .expect("repeated removal of the stale upper is a typed no-op");
}

#[test]
fn http_app_error_from_request_publish_error() {
    let error = HttpAppError::from(RequestPublishError::MessageError);
    assert_eq!(
        error,
        HttpAppError::RequestPublish {
            error: RequestPublishError::MessageError
        }
    );
    let runtime = RuntimeError::from(error);
    assert!(matches!(runtime, RuntimeError::Subsystem { subsystem, .. } if subsystem == "http"));
}

// --- bidi request FIN/reset cleanup ------------------------------------------

/// A FIN on a complete request (no declared body) removes the upper request
/// Session before the request stream context is released, exactly as VPP
/// `http3_stream_cleanup_callback` deletes the app notification before
/// freeing the request (http3.c:2459-2462); the lower stream and parent
/// connection Sessions and contexts survive.
#[test]
fn request_stream_cleanup_fin_complete_removes_upper_and_releases_request() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);
    let upper = sessions
        .create_upper_session(child, 0x55)
        .expect("create the upper request Session from the stream child");

    disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect("bidi FIN completes the request");

    assert!(!sessions.has_session(upper), "the upper Session is removed");
    assert!(
        sessions.has_session(child),
        "the lower stream Session survives"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(
            http.stream_len(),
            0,
            "the request stream context is released"
        );
        assert!(
            http.get(parent_context).is_ok(),
            "the parent connection context is still live"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// A FIN with a declared body that is only half received fails typed with
/// `RequestFinishProtocol` carrying `RequestIncomplete` before any cleanup:
/// the upper Session, the lower stream, and the parent connection all stay
/// live and no connection close is dispatched.
#[test]
fn request_stream_cleanup_fin_incomplete_keeps_stream_and_upper_live() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);
    let upper = sessions
        .create_upper_session(child, 0x55)
        .expect("create the upper request Session from the stream child");
    main.with_worker(DataWorkerId::new(0), |http| {
        http.install_request_body_length(stream, child, Some(4))
            .map_err(RuntimeError::from)
    })
    .expect("install the declared body length");
    let fifo = local_fifo(8192);
    main.with_worker(DataWorkerId::new(0), |http| {
        http.process_request_data(stream, child, &fifo, b"12")
            .map_err(|error| match error {
                RequestReadError::Worker(inner) => RuntimeError::from(inner),
                RequestReadError::Protocol(error) => {
                    RuntimeError::from(HttpAppError::RequestFinishProtocol {
                        code: error.error_code(),
                    })
                }
            })
    })
    .expect("feed the first half of the declared body");
    CLOSE_CALLS.with(|calls| calls.set(0));

    let error = disconnect_on(&main, &mut sessions, child, u64::from(stream))
        .expect_err("FIN with an incomplete declared body must fail");
    assert!(
        matches!(
            http_app_error(&error),
            HttpAppError::RequestFinishProtocol {
                code: ErrorCode::RequestIncomplete
            }
        ),
        "typed incomplete-request error expected, got {error:?}"
    );

    assert!(sessions.has_session(upper), "the upper Session stays live");
    assert!(
        sessions.has_session(child),
        "the lower stream Session stays live"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session stays live"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no connection close"));
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(http.stream_len(), 1, "the request stream stays live");
        assert_eq!(
            http.get_stream_for_session(stream, child)
                .map_err(RuntimeError::from)?
                .session,
            child,
            "the stream is still bound to its Session"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

/// A RESET on a bidi request stream removes the upper request Session and
/// releases the request stream context without finish validation and without
/// any transport action: the parent connection is never closed and no stream
/// reset is sent, matching VPP `http3_transport_stream_reset_callback`
/// returning before any error recording for non-uni streams.
#[test]
fn request_stream_cleanup_reset_removes_upper_without_transport_actions() {
    let (main, mut sessions, application, session_app) = test_harness();
    let (parent, _parent_context, child, stream) =
        construct_bidi_request_stream(&main, &mut sessions, application, session_app, 1);
    let upper = sessions
        .create_upper_session(child, 0x55)
        .expect("create the upper request Session from the stream child");
    CLOSE_CALLS.with(|calls| calls.set(0));
    RESET_CALLS.with(|calls| calls.set(0));

    reset_on(&main, &mut sessions, child, u64::from(stream))
        .expect("bidi RESET releases the request");

    assert!(!sessions.has_session(upper), "the upper Session is removed");
    assert!(
        sessions.has_session(child),
        "the lower stream Session survives"
    );
    assert!(
        sessions.has_session(parent),
        "the parent connection Session survives"
    );
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no connection close"));
    RESET_CALLS.with(|calls| assert_eq!(calls.get(), 0, "no stream reset"));
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.len(), 1, "the parent connection context remains");
        assert_eq!(
            http.stream_len(),
            0,
            "the request stream context is released"
        );
        Ok(())
    })
    .expect("worker accessible on the test thread");
}

#[test]
fn request_error_action_capacity_retries() {
    let action =
        request_publish_error_action(RequestPublishError::Publish(PublishError::Capacity {
            requested: 8,
            available: 2,
        }));
    assert_eq!(action, RequestErrorAction::Retry);
}

#[test]
fn request_error_action_message_error_resets_stream() {
    let action = request_publish_error_action(RequestPublishError::MessageError);
    assert_eq!(
        action,
        RequestErrorAction::ResetStream {
            code: ErrorCode::MessageError
        }
    );
}

#[test]
fn request_error_action_general_protocol_error_resets_stream() {
    let action = request_publish_error_action(RequestPublishError::GeneralProtocolError);
    assert_eq!(
        action,
        RequestErrorAction::ResetStream {
            code: ErrorCode::GeneralProtocolError
        }
    );
}

#[test]
fn request_error_action_internal_error_resets_stream() {
    let action = request_publish_error_action(RequestPublishError::InternalError);
    assert_eq!(
        action,
        RequestErrorAction::ResetStream {
            code: ErrorCode::InternalError
        }
    );
}

#[test]
fn request_error_action_qpack_decompression_failed_closes_connection() {
    let action = request_publish_error_action(RequestPublishError::QpackDecompressionFailed);
    assert_eq!(
        action,
        RequestErrorAction::CloseConnection {
            code: ErrorCode::QpackDecompressionFailed
        }
    );
}

#[test]
fn request_error_action_publish_encode_resets_stream() {
    let action = request_publish_error_action(RequestPublishError::Publish(PublishError::Encode(
        EncodeError::LengthOverflow,
    )));
    assert_eq!(
        action,
        RequestErrorAction::ResetStream {
            code: ErrorCode::InternalError
        }
    );
}

#[test]
fn request_error_action_publish_fifo_resets_stream() {
    let action = request_publish_error_action(RequestPublishError::Publish(PublishError::Fifo(
        FifoError::CommitExceedsReservation {
            initialized: 0,
            reserved: 8,
        },
    )));
    assert_eq!(
        action,
        RequestErrorAction::ResetStream {
            code: ErrorCode::InternalError
        }
    );
}

#[test]
fn request_frame_protocol_from_phase_frame_unexpected() {
    let error = HttpAppError::from(RequestFrameError::Phase(ErrorCode::FrameUnexpected));
    assert_eq!(
        error,
        HttpAppError::RequestFrameProtocol {
            code: ErrorCode::FrameUnexpected
        }
    );
    let runtime = RuntimeError::from(error);
    assert!(matches!(runtime, RuntimeError::Subsystem { subsystem, .. } if subsystem == "http"));
}

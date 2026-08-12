//! Focused tests for the HTTP plugin descriptor and the builtin HTTP Session
//! App registration seam (VPP `http_app_cb_vft` attach, http.c:1004-1063),
//! including the `accept` callback that branches on the accepted Session's
//! metadata (VPP `http_ts_accept_callback`, http.c:733-740): one
//! `ConnectionContext` is bound to a root lower QUIC Session
//! (`http_ts_accept_connection`, http.c:586) and one `StreamContext` is bound
//! to a stream child's parent connection (`http_ts_accept_stream`, http.c:675).
//! A root accept also bootstraps its local uni control stream exactly once
//! (`http3_conn_init`, http3.c:216-250), with typed transport actions faked
//! so the open and its failure rollback are observable.

use std::cell::Cell;
use std::sync::Arc;

use hammer_infra::pool::Index;
use hammer_runtime::app::{AppSessionConfig, ApplicationId, SessionAppContext, SessionAppId, SessionFlags};
use hammer_runtime::session::{SessionApplicationErrorCode, SessionStreamDirection};
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine, RuntimeError, RuntimeRegistry,
    RuntimeResult, SessionTransportRegistration,
};
use hammer_service::session::protocol::SessionAppCallbacks;
use hammer_service::session::runtime::{SessionMain, SessionTransportWorkerActions, SessionWorker};
use hammer_service::session::{ApplicationMain, SessionEndpointRole, SessionId};

use super::http_app::{
    CALLBACKS, HTTP_SESSION_APP, HttpAppError, NAME, accept, accept_on, destroy, install,
};
use super::listener::{HTTP_MAIN, HttpMain};
use crate::http3::request_frame_reader::RequestFrameRead;
use crate::worker::{ContextId, HTTP_CONTEXT_CAPACITY, HttpWorker, HttpWorkerError, StreamContextId};

/// SessionWorker (64-slot pool on data worker 0) whose Application registry
/// pre-registers the builtin HTTP Session App, plus an `HttpMain` owning one
/// installed worker slot on the current thread. Direct `HttpMain`, bypassing
/// the process-wide `HTTP_MAIN` OnceLock so each test owns its authority,
/// exactly as the listener tests do.
fn test_harness() -> (Arc<HttpMain>, SessionWorker<Index>, ApplicationId, SessionAppId) {
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
}

fn fake_open_stream(
    _sessions: &mut SessionWorker<Index>,
    parent: SessionId,
    direction: SessionStreamDirection,
    app_context: SessionAppContext,
) -> RuntimeResult<SessionId> {
    OPEN_CALLS.with(|calls| calls.set(calls.get() + 1));
    OPEN_CONTEXT.with(|seen| seen.set(app_context));
    assert_eq!(direction, SessionStreamDirection::Uni, "control stream is uni");
    if OPEN_FAIL.with(|fail| fail.get()) {
        return Err(RuntimeError::ServiceClosed);
    }
    Ok(SessionId::from_raw(parent.get() + 1))
}

fn fake_reset_stream(
    _sessions: &mut SessionWorker<Index>,
    _session_id: SessionId,
    _code: SessionApplicationErrorCode,
) -> RuntimeResult<()> {
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
    _reason: &[u8],
) -> RuntimeResult<()> {
    CLOSE_CALLS.with(|calls| calls.set(calls.get() + 1));
    CLOSE_SESSION.with(|seen| seen.set(session_id.get()));
    CLOSE_CODE.with(|seen| seen.set(u64::from(code)));
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
fn callback_table_wires_accept_and_defers_all_other_vpp_callbacks() {
    // `accept` is the one VPP `http_app_cb_vft` entry (http.c:1004-1017) this
    // slice owns: it needs nothing beyond the per-worker context pool. Every
    // other entry needs HTTP worker lifecycle state (streams, FIFOs,
    // publication) from later slices, so it stays `None`: no speculative
    // no-ops until the owning slices land.
    let callbacks: SessionAppCallbacks = CALLBACKS;
    assert!(callbacks.accept.is_some());
    assert!(callbacks.add_segment.is_none());
    assert!(callbacks.del_segment.is_none());
    assert!(callbacks.connected.is_none());
    assert!(callbacks.disconnect.is_none());
    assert!(callbacks.reset.is_none());
    assert!(callbacks.transport_closed.is_none());
    assert!(callbacks.cleanup.is_none());
    assert!(callbacks.half_open_cleanup.is_none());
    assert!(callbacks.migrate.is_none());
    assert!(callbacks.listened.is_none());
    assert!(callbacks.unlistened.is_none());
    assert!(callbacks.builtin_rx.is_none());
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
        assert!(connection.peer_settings_pending, "peer SETTINGS still expected");
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
    assert!(matches!(
        http_error(&error),
        HttpWorkerError::ControlStreamOpenFailed { context: failed } if *failed == context
    ), "primary bootstrap error preserved, got {error:?}");
    OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "failing action invoked once"));
    assert_eq!(worker_len(&main), 0, "connection context removed in rollback");
    CLOSE_CALLS.with(|calls| assert_eq!(calls.get(), 1, "lower connection closed in rollback"));
    CLOSE_SESSION.with(|seen| {
        assert_eq!(seen.get(), session.get(), "close names the lower connection")
    });
    CLOSE_CODE.with(|seen| {
        assert_eq!(seen.get(), 0x0102, "close carries VPP H3_INTERNAL_ERROR")
    });

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
    assert_eq!(worker_len(&main), 0, "failed publication rolled back the context");
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
            stream.parent,
            parent_context,
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
            stream.parent,
            parent_context,
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

    let error = accept_on(&main, &mut sessions, child, 0)
        .expect_err("stale parent cannot be adopted");
    assert!(
        matches!(
            http_error(&error),
            HttpWorkerError::ParentContextMissing { parent } if *parent == parent_context
        ),
        "typed parent-missing error expected, got {error:?}"
    );
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.stream_len(), 0, "no stream allocated under a stale parent");
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

    let error = accept_on(&main, &mut sessions, child, 0)
        .expect_err("foreign parent cannot be adopted");
    assert!(
        matches!(
            http_error(&error),
            HttpWorkerError::ParentContextMissing { parent } if *parent == foreign_context
        ),
        "typed parent-missing error expected, got {error:?}"
    );
    assert_eq!(worker_len(&main), 1, "only the real parent context remains");
    main.with_worker(DataWorkerId::new(0), |http| {
        assert_eq!(http.stream_len(), 0, "no stream allocated under a foreign parent");
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
        worker.take_pending_field_section(stream, session).unwrap(),
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
    // worker-owned pending slot takes ownership of it instead of dropping
    // it, and the sink hands it back for the decode/publish stage.
    worker
        .retain_pending_field_section(stream, session, encoded)
        .expect("retain the completed field section");
    assert_eq!(
        worker.take_pending_field_section(stream, session).unwrap(),
        Some(b"hi".to_vec()),
        "the retained section is the encoded HEADERS field section"
    );
    assert_eq!(
        worker.take_pending_field_section(stream, session).unwrap(),
        None,
        "taking the section empties the pending slot"
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
    // complete on the same stream; both surface by value.
    let (first, _) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'i'])
        .expect("feed the initial HEADERS frame");
    let RequestFrameRead::Headers(first) = first else {
        panic!("the initial HEADERS must surface its field section");
    };
    worker
        .retain_pending_field_section(stream, session, first)
        .expect("retain the initial field section");

    let (trailer, _) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'x'])
        .expect("feed the trailer HEADERS frame");
    let RequestFrameRead::Headers(trailer) = trailer else {
        panic!("the trailer HEADERS must surface its field section");
    };
    // A section is already pending: the retain is rejected with the newer
    // section returned unreplaced, never silently replaced or dropped.
    let error = worker
        .retain_pending_field_section(stream, session, trailer)
        .expect_err("a pending section rejects a second retain");
    assert!(
        matches!(
            error,
            HttpWorkerError::PendingFieldSectionOverflow {
                stream: rejected_stream,
                ref section
            } if rejected_stream == stream && *section == b"hx".to_vec()
        ),
        "overflow carries the rejected section back, got {error:?}"
    );
    assert_eq!(
        worker.take_pending_field_section(stream, session).unwrap(),
        Some(b"hi".to_vec()),
        "the pending initial section survives the rejected retain"
    );

    // The released stream's identity is stale: it can neither take nor
    // retain on the reused slot, and the fresh stream starts empty.
    worker.remove_stream(stream).expect("release the stream");
    let next = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate a stream reusing the released slot");
    assert!(
        worker.take_pending_field_section(stream, session).is_err(),
        "stale stream identity cannot take on the reused slot"
    );
    assert!(
        worker
            .retain_pending_field_section(stream, session, b"zz".to_vec())
            .is_err(),
        "stale stream identity cannot retain on the reused slot"
    );
    assert_eq!(
        worker.take_pending_field_section(next, session).unwrap(),
        None,
        "the fresh stream observes no pending section from the released stream"
    );
}

/// The emptied-but-recorded pending slot refills with an optional trailer
/// HEADERS (RFC 9114 Section 4.1) after the initial section was taken: the
/// sink supports retain -> take -> retain trailer -> take on one recorded
/// slot, mirroring VPP reusing `req->headers` on the same per-request
/// `http_ctx_t` for the trailer section.
#[test]
fn pending_field_section_take_then_trailer_refills_emptied_slot() {
    let mut worker = HttpWorker::with_capacities(4, 4);
    let session = SessionId::from_raw(1);
    let parent = worker.allocate(session).expect("allocate parent context");
    let stream = worker
        .allocate_stream(session, parent, SessionStreamDirection::Bidi)
        .expect("allocate bidi request stream");

    // Initial HEADERS: retained, then taken by the decode/publish seam.
    let (first, _) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'i'])
        .expect("feed the initial HEADERS frame");
    let RequestFrameRead::Headers(first) = first else {
        panic!("the initial HEADERS must surface its field section");
    };
    worker
        .retain_pending_field_section(stream, session, first)
        .expect("retain the initial field section");
    assert_eq!(
        worker.take_pending_field_section(stream, session).unwrap(),
        Some(b"hi".to_vec()),
        "the initial section is taken"
    );
    assert_eq!(
        worker.take_pending_field_section(stream, session).unwrap(),
        None,
        "the slot is empty after taking"
    );

    // The trailer completes after the initial section was taken: the same
    // recorded slot refills, and the trailer is taken in turn.
    let (trailer, _) = worker
        .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'x'])
        .expect("feed the trailer HEADERS frame");
    let RequestFrameRead::Headers(trailer) = trailer else {
        panic!("the trailer HEADERS must surface its field section");
    };
    worker
        .retain_pending_field_section(stream, session, trailer)
        .expect("the emptied slot refills with the trailer");
    assert_eq!(
        worker.take_pending_field_section(stream, session).unwrap(),
        Some(b"hx".to_vec()),
        "the refilled trailer section is taken"
    );
    assert_eq!(
        worker.take_pending_field_section(stream, session).unwrap(),
        None,
        "the slot is empty again after the trailer is taken"
    );
}

/// A stream bound to one session rejects retain and take from a foreign
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

    // Take with a foreign session is rejected before touching the slot.
    let error = worker
        .take_pending_field_section(stream, foreign)
        .expect_err("take with a foreign session must fail");
    assert!(
        matches!(
            error,
            HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected,
                actual
            } if s == stream && expected == foreign && actual == session
        ),
        "take typed mismatch expected, got {error:?}"
    );

    // Retain with a foreign session is rejected the same way, and the
    // section is not left in the slot.
    let error = worker
        .retain_pending_field_section(stream, foreign, b"zz".to_vec())
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
        worker.take_pending_field_section(stream, session).unwrap(),
        None,
        "the rejected retain left no section in the slot"
    );

    // The stream still accepts its own session afterwards.
    worker
        .retain_pending_field_section(stream, session, b"hi".to_vec())
        .expect("retain with the bound session still works");
    assert_eq!(
        worker.take_pending_field_section(stream, session).unwrap(),
        Some(b"hi".to_vec()),
        "the bound session retains and takes normally"
    );
}

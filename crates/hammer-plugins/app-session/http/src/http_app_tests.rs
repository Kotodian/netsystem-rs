//! Focused tests for the HTTP plugin descriptor and the builtin HTTP Session
//! App registration seam (VPP `http_app_cb_vft` attach, http.c:1004-1063),
//! including the `accept` callback that branches on the accepted Session's
//! metadata (VPP `http_ts_accept_callback`, http.c:733-740): one
//! `ConnectionContext` is bound to a root lower QUIC Session
//! (`http_ts_accept_connection`, http.c:586) and one `StreamContext` is bound
//! to a stream child's parent connection (`http_ts_accept_stream`, http.c:675).

use std::sync::Arc;

use hammer_infra::pool::Index;
use hammer_runtime::app::{AppSessionConfig, ApplicationId, SessionAppId, SessionFlags};
use hammer_runtime::session::SessionStreamDirection;
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine, RuntimeError, RuntimeRegistry,
    SessionTransportRegistration,
};
use hammer_service::session::protocol::SessionAppCallbacks;
use hammer_service::session::runtime::{SessionMain, SessionWorker};
use hammer_service::session::{ApplicationMain, SessionId};

use super::http_app::{
    CALLBACKS, HTTP_SESSION_APP, HttpAppError, NAME, accept, accept_on, destroy, install,
};
use super::listener::{HTTP_MAIN, HttpMain};
use crate::worker::{ContextId, HTTP_CONTEXT_CAPACITY, HttpWorkerError, StreamContextId};

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
    let sessions = SessionWorker::<Index>::new(
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
    (main, sessions, application, session_app)
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
fn accept_is_idempotent_when_context_is_already_published() {
    let (main, mut sessions, application, session_app) = test_harness();
    let session = construct_session(&mut sessions, application, session_app, 2);

    accept_on(&main, &mut sessions, session, 0).expect("first accept");
    // The dispatch layer passes the published nonzero context back on a
    // duplicate accept; it must allocate nothing further.
    accept_on(&main, &mut sessions, session, 1).expect("duplicate accept is a no-op");
    assert_eq!(worker_len(&main), 1);
}

#[test]
fn accept_rolls_back_context_when_publication_fails() {
    let (main, mut sessions, _application, _session_app) = test_harness();
    // A session outside the 64-slot worker pool: the context allocation
    // succeeds, but set_app_session fails with the session subsystem's
    // SessionMissing, so the context must be rolled back and the primary
    // error preserved.
    let bogus = SessionId::from_raw(123);
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

use std::time::Instant;

use hammer_infra::pool::Index;
use hammer_runtime::app::{SessionAppContext, SessionAppRegistration};
use hammer_runtime::{DataWorkerId, Engine, RuntimeError, RuntimeResult};
use hammer_service::session::SessionId;
use hammer_service::session::protocol::SessionAppCallbacks;
use hammer_service::session::runtime::SessionWorker;

use crate::listener::QUIC_MAIN;
use crate::worker::ContextId;

pub(crate) const NAME: &str = "quic";

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, thiserror::Error)]
enum QuicSessionError {
    #[error("QUIC Session App context {context:?} does not own Session {session:?}")]
    ContextSessionMismatch {
        context: ContextId,
        session: SessionId,
    },
    #[error("QUIC Session App context is missing for Session {session:?}")]
    ContextMissing { session: SessionId },
}

fn with_quic_worker<R>(
    worker: &mut SessionWorker<Index>,
    operation: impl FnOnce(&mut crate::worker::QuicWorker) -> RuntimeResult<R>,
) -> RuntimeResult<R> {
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?
        .with_worker(worker.worker(), operation)
}

fn publish_context(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: ContextId,
) -> RuntimeResult<()> {
    if let Err(error) = worker.set_app_session(session, context.into()) {
        // Rollback is best effort and must not replace the primary
        // set_app_session error; QUIC_MAIN may already be gone during teardown.
        let _ = with_quic_worker(worker, |quic| quic.remove_context(context));
        return Err(error);
    }
    Ok(())
}

fn accept(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    let listener = if context == 0 {
        worker
            .session_app_endpoint(session)
            .and_then(|(_, _, opaque, _)| opaque)
            .ok_or_else(|| QuicSessionError::ContextMissing { session })?
    } else {
        context
    };
    let listener_id = ContextId::from(listener);
    let listener = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?
        .listener_context(listener_id)
        .ok_or_else(|| QuicSessionError::ContextMissing { session })?;
    let connection = with_quic_worker(worker, |quic| {
        quic.accept_connection(session, listener_id, &listener)
    })?;
    publish_context(worker, session, connection)
}

fn connected(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    _: SessionAppContext,
) -> RuntimeResult<()> {
    let (_, _, opaque, _) = worker
        .session_app_endpoint(session)
        .ok_or_else(|| QuicSessionError::ContextMissing { session })?;
    let context = opaque
        .map(ContextId::from)
        .ok_or(QuicSessionError::ContextMissing { session })?;
    let connection = with_quic_worker(worker, |quic| {
        quic.connect_connection(context, session, Instant::now())
    })?;
    publish_context(worker, session, connection)?;
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?;
    main.with_worker_and_sessions(worker, |sessions, quic| {
        quic.send_packets(sessions, connection, Instant::now())
    })
    .map_err(|error| {
        // Rollback is best effort and must not replace the primary
        // send_packets error; QUIC_MAIN may already be gone during teardown.
        let _ = with_quic_worker(worker, |quic| quic.remove_context(connection));
        let _ = worker.set_app_session(session, 0);
        error
    })
}

fn builtin_rx(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context == 0 {
        return Err(QuicSessionError::ContextMissing { session }.into());
    }
    let context = ContextId::from(context);
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?;
    let listener =
        main.with_worker_and_sessions(worker, |_, quic| Ok(quic.listener_context_id(context)))?;
    if let Some(listener) = listener
        && main.listener_context(listener).is_none()
    {
        main.with_worker_and_sessions(worker, |sessions, quic| {
            quic.remove_context(context)?;
            sessions.set_app_session(session, 0)?;
            Ok(())
        })?;
        return Ok(());
    }
    main.with_worker_and_sessions(worker, |sessions, quic| {
        quic.process_udp_rx(sessions, session, context, Instant::now())
    })
}

fn builtin_tx(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context == 0 {
        return Err(QuicSessionError::ContextMissing { session }.into());
    }
    let context = ContextId::from(context);
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?
        .with_worker_and_sessions(worker, |sessions, quic| {
            quic.send_packets(sessions, context, Instant::now())
        })
}

fn close_lower_connection(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context == 0 {
        // Late lifecycle notification after finalize_connection cleared the
        // lower Session's app context: teardown is already complete.
        return Ok(());
    }
    let context = ContextId::from(context);
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?;
    main.with_worker_and_sessions(worker, |sessions, quic| {
        let Some(lower) = quic.lower_session_if_present(context) else {
            return Ok(());
        };
        if lower != session {
            return Err(QuicSessionError::ContextSessionMismatch { context, session }.into());
        }
        quic.transport_closed(sessions, context)
    })
}

fn install(engine: &mut Engine) -> RuntimeResult<()> {
    let main = engine
        .registry
        .require::<hammer_service::session::runtime::SessionMain>()?;
    main.install_session_app(&engine.runtime, NAME, &CALLBACKS)
}

fn destroy(worker: DataWorkerId, context: SessionAppContext) {
    if context == 0 {
        return;
    }
    let context = ContextId::from(context);
    // A nonzero stale context is a no-op like VPP's invalid-context early
    // return in quic_quicly_on_app_closed; QUIC_MAIN may already be gone
    // during teardown, and destroy cannot report a Result, so removal is
    // best effort and idempotent.
    let Some(main) = QUIC_MAIN.get() else {
        return;
    };
    let _ = main.with_worker(worker, |quic| quic.remove_context(context));
}

pub(crate) static CALLBACKS: SessionAppCallbacks = SessionAppCallbacks {
    accept: Some(accept),
    connected: Some(connected),
    disconnect: Some(close_lower_connection),
    reset: Some(close_lower_connection),
    transport_closed: Some(close_lower_connection),
    cleanup: Some(close_lower_connection),
    builtin_rx: Some(builtin_rx),
    builtin_tx: Some(builtin_tx),
    ..SessionAppCallbacks::all_none()
};

pub(crate) static QUIC_SESSION_APP: SessionAppRegistration =
    SessionAppRegistration::new(NAME, install, destroy);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn quic_session_app_uses_manual_static_callback_table() {
        assert_eq!(QUIC_SESSION_APP.name(), NAME);
        assert!(CALLBACKS.accept.is_some());
        assert!(CALLBACKS.connected.is_some());
        assert!(CALLBACKS.builtin_rx.is_some());
        assert!(CALLBACKS.builtin_tx.is_some());
        assert!(CALLBACKS.cleanup.is_some());
    }

    #[test]
    fn zero_context_lower_lifecycle_callback_is_idempotent() {
        // QUIC_MAIN is unset in this unit test, so this also proves the
        // zero-context path succeeds without requiring QUIC_MAIN.
        let applications = hammer_service::session::ApplicationMain::new(4);
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )
        .expect("test SessionWorker");
        let close = CALLBACKS
            .transport_closed
            .expect("transport_closed callback");
        close(&mut sessions, SessionId::from_raw(123), 0).expect("zero-context close succeeds");
    }

    #[test]
    fn publish_context_keeps_set_app_session_error_when_rollback_unavailable() {
        // set_app_session fails for a session outside the worker's pool, and
        // the rollback path cannot reach QUIC_MAIN, so publish_context must
        // return the original set_app_session error instead of panicking.
        let applications = hammer_service::session::ApplicationMain::new(4);
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )
        .expect("test SessionWorker");
        let error = publish_context(
            &mut sessions,
            SessionId::from_raw(123),
            ContextId::from(1u64),
        )
        .expect_err("set_app_session fails for an unknown session");
        assert!(
            matches!(
                error,
                RuntimeError::Subsystem {
                    subsystem: "session",
                    ..
                }
            ),
            "original set_app_session error expected, got {error:?}"
        );
    }

    #[test]
    fn destroy_with_stale_context_is_noop_without_quic_main() {
        // destroy cannot return a Result, so a stale nonzero context must be
        // dropped silently when QUIC_MAIN is unavailable, mirroring VPP's
        // invalid-context no-op in quic_quicly_on_app_closed.
        destroy(DataWorkerId::new(0), 42);
    }
}

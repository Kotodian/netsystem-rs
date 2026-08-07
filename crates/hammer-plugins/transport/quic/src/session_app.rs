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
        if let Err(cleanup_error) = with_quic_worker(worker, |quic| quic.remove_context(context)) {
            tracing::error!(
                ?session,
                ?context,
                %cleanup_error,
                "QUIC Session App context rollback failed"
            );
        }
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
    let listener = ContextId::from(listener);
    let connection = with_quic_worker(worker, |quic| quic.accept_connection(session, listener))?;
    publish_context(worker, session, connection)
}

fn connected(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    _: SessionAppContext,
) -> RuntimeResult<()> {
    let connection = with_quic_worker(worker, |quic| quic.connect_connection(session))?;
    publish_context(worker, session, connection)
}

fn lifecycle(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context == 0 {
        return Err(QuicSessionError::ContextMissing { session }.into());
    }
    let context = ContextId::from(context);
    with_quic_worker(worker, |quic| {
        let owner = quic.context_session(context)?;
        if owner != session {
            return Err(QuicSessionError::ContextSessionMismatch { context, session }.into());
        }
        Ok(())
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
    QUIC_MAIN
        .get()
        .expect("QUIC Main exists during Session App destruction")
        .with_worker(worker, |quic| quic.remove_context(context))
        .expect("QUIC Session App context is removed exactly once");
}

pub(crate) static CALLBACKS: SessionAppCallbacks = SessionAppCallbacks {
    accept: Some(accept),
    connected: Some(connected),
    disconnect: Some(lifecycle),
    reset: Some(lifecycle),
    transport_closed: Some(lifecycle),
    cleanup: Some(lifecycle),
    builtin_rx: Some(lifecycle),
    builtin_tx: Some(lifecycle),
    ..SessionAppCallbacks::all_none()
};

pub(crate) static QUIC_SESSION_APP: SessionAppRegistration =
    SessionAppRegistration::new(NAME, install, destroy);

#[cfg(test)]
mod tests {
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
}

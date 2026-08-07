use std::sync::OnceLock;

use hammer_infra::pool::Index;
use hammer_runtime::app::{
    ApplicationId, SessionAppContext, SessionAppContexts, SessionAppId, SessionAppRegistration,
};
use hammer_runtime::{Engine, RuntimeResult};
use hammer_service::session::protocol::{SessionApp, SessionAppCallbacks};
use hammer_service::session::runtime::SessionWorker;
use hammer_service::session::{SessionId, SessionQueueError};

pub(crate) const NAME: &str = "quic";

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, thiserror::Error)]
#[error("QUIC Session App has no owning Application for Session {session:?}")]
struct QuicSessionApplicationMissing {
    session: SessionId,
}

/// Initial QUIC Session App state. The sans-I/O engine and stream ownership
/// fields are added here in the next skeleton slice; the identity/config facts
/// already prove the callback boundary without exposing QUIC to service.
#[derive(Debug)]
struct QuicSessionState {
    application: ApplicationId,
    configuration: Option<u64>,
    session: Option<SessionId>,
}

impl SessionApp for QuicSessionState {
    const CONTEXT_CAPACITY: usize = 1_024;

    fn create(
        application: Option<ApplicationId>,
        _: Option<SessionAppId>,
        config: Option<u64>,
        _: Option<&str>,
    ) -> RuntimeResult<Self> {
        Ok(Self {
            application: application.ok_or(QuicSessionApplicationMissing {
                session: SessionId::from_raw(0),
            })?,
            configuration: config,
            session: None,
        })
    }

    fn accept(
        &mut self,
        _: &mut SessionWorker<Index>,
        session: SessionId,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        self.session = Some(session);
        Ok(())
    }

    fn connected(
        &mut self,
        _: &mut SessionWorker<Index>,
        session: SessionId,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        self.session = Some(session);
        Ok(())
    }
}

static CONTEXTS: OnceLock<SessionAppContexts<QuicSessionState>> = OnceLock::new();

fn create_context(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
) -> RuntimeResult<SessionAppContext> {
    let (application, app, config, server_name) = worker
        .session_app_facts(session)
        .ok_or(SessionQueueError::SessionAppContextCreateUnsupported)?;
    let state = QuicSessionState::create(Some(application), app, config, server_name)?;
    let context = CONTEXTS
        .get_or_init(|| {
            SessionAppContexts::new(worker.worker_count(), QuicSessionState::CONTEXT_CAPACITY)
        })
        .insert(worker.worker(), worker.worker_count(), state)?;
    if let Err(error) = worker.set_app_session(session, context) {
        CONTEXTS
            .get()
            .expect("QUIC Session App contexts exist after insertion")
            .remove(worker.worker(), context);
        return Err(error);
    }
    Ok(context)
}

fn with_context(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
    operation: impl FnOnce(
        &mut QuicSessionState,
        &mut SessionWorker<Index>,
        SessionId,
        SessionAppContext,
    ) -> RuntimeResult<()>,
) -> RuntimeResult<()> {
    let context = if context == 0 {
        create_context(worker, session)?
    } else {
        context
    };
    CONTEXTS
        .get()
        .ok_or(SessionQueueError::SessionAppContextCreateUnsupported)?
        .with_mut(worker.worker(), context, |state| {
            operation(state, worker, session, context)
        })
}

fn accept(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    with_context(
        worker,
        session,
        context,
        |state, worker, session, context| state.accept(worker, session, context),
    )
}

fn connected(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    with_context(
        worker,
        session,
        context,
        |state, worker, session, context| state.connected(worker, session, context),
    )
}

fn lifecycle(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    with_context(worker, session, context, |state, _, _, _| {
        let _ = (state.application, state.configuration);
        Ok(())
    })
}

fn install(engine: &mut Engine) -> RuntimeResult<()> {
    let main = engine
        .registry
        .require::<hammer_service::session::runtime::SessionMain>()?;
    main.install_session_app(&engine.runtime, NAME, &CALLBACKS)
}

fn destroy(worker: hammer_runtime::DataWorkerId, context: SessionAppContext) {
    CONTEXTS
        .get()
        .expect("QUIC Session App contexts exist during destruction")
        .remove(worker, context);
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

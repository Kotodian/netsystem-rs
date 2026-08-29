use std::time::Instant;

use hammer_runtime::{DataWorkerId, RuntimeError, RuntimeResult};
use hammer_service::session::protocol::SessionAppVft;
use hammer_service::session::runtime::SessionWorker;

use crate::listener::QUIC_MAIN;

pub(crate) const NAME: &str = "quic";

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, thiserror::Error)]
enum QuicSessionError {
    #[error("QUIC Session App context {context:?} does not own Session {session:?}")]
    ContextSessionMismatch { context: u32, session: u32 },
    #[error("QUIC Session App context is missing for Session {session:?}")]
    ContextMissing { session: u32 },
    #[error("QUIC Session App context {context:?} does not fit a Pool index")]
    ContextOutOfRange { context: u64 },
}

fn with_quic_worker<R>(
    worker: &mut SessionWorker,
    operation: impl FnOnce(&mut crate::worker::QuicWorker) -> RuntimeResult<R>,
) -> RuntimeResult<R> {
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?
        .with_worker(worker.worker(), operation)
}

fn publish_context(worker: &mut SessionWorker, session: u32, context: u32) -> RuntimeResult<()> {
    if let Err(error) = worker.set_app_session(session, context.into()) {
        // Rollback is best effort and must not replace the primary
        // set_app_session error; QUIC_MAIN may already be gone during teardown.
        let _ = with_quic_worker(worker, |quic| quic.remove_context(context));
        return Err(error);
    }
    Ok(())
}

fn accept(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    let listener = if context == 0 {
        worker
            .session_app_endpoint(session)
            .and_then(|(_, _, opaque, _)| opaque)
            .ok_or_else(|| QuicSessionError::ContextMissing { session })?
    } else {
        context
    };
    let listener_id = u32::try_from(listener)
        .map_err(|_| QuicSessionError::ContextOutOfRange { context: listener })?;
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

fn connected(worker: &mut SessionWorker, session: u32, _: u64) -> RuntimeResult<()> {
    let (_, _, opaque, _) = worker
        .session_app_endpoint(session)
        .ok_or_else(|| QuicSessionError::ContextMissing { session })?;
    let context = opaque.ok_or(QuicSessionError::ContextMissing { session })?;
    let context =
        u32::try_from(context).map_err(|_| QuicSessionError::ContextOutOfRange { context })?;
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

fn builtin_rx(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    if context == 0 {
        return Err(QuicSessionError::ContextMissing { session }.into());
    }
    let context =
        u32::try_from(context).map_err(|_| QuicSessionError::ContextOutOfRange { context })?;
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

fn builtin_tx(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    if context == 0 {
        return Err(QuicSessionError::ContextMissing { session }.into());
    }
    let context =
        u32::try_from(context).map_err(|_| QuicSessionError::ContextOutOfRange { context })?;
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?
        .with_worker_and_sessions(worker, |sessions, quic| {
            quic.send_packets(sessions, context, Instant::now())
        })
}

fn close_lower_connection(
    worker: &mut SessionWorker,
    session: u32,
    context: u64,
) -> RuntimeResult<()> {
    if context == 0 {
        // Late lifecycle notification after finalize_connection cleared the
        // lower Session's app context: teardown is already complete.
        return Ok(());
    }
    let context =
        u32::try_from(context).map_err(|_| QuicSessionError::ContextOutOfRange { context })?;
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

pub(crate) const VFT: SessionAppVft = SessionAppVft {
    name: NAME,
    accept: Some(accept),
    connected: Some(connected),
    disconnect: Some(close_lower_connection),
    reset: Some(close_lower_connection),
    transport_closed: Some(close_lower_connection),
    cleanup: Some(close_lower_connection),
    builtin_rx: Some(builtin_rx),
    builtin_tx: Some(builtin_tx),
    ..SessionAppVft::all_none(NAME)
};

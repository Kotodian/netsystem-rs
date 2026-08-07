//! QUIC's worker-local transport state and registration.

use hammer_infra::pool::{Index, Pool};
use hammer_infra::thread_owned::ThreadOwnedError;
use hammer_runtime::{DataWorkerId, RuntimeResult, SessionListenerId};
use hammer_service::session::SessionId;

use crate::config::ConfigId;

pub(super) const QUIC_CONTEXT_CAPACITY: usize = 4_096;

/// Generation-checked identity for one QUIC listener, connection, or stream
/// context. Main Thread listener contexts and Data Worker transport contexts
/// use the same packed pool identity, but each owner retains its own pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(super) struct ContextId(u64);

impl From<u64> for ContextId {
    #[inline]
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

impl From<ContextId> for u64 {
    #[inline]
    fn from(context: ContextId) -> Self {
        context.0
    }
}

impl From<Index> for ContextId {
    #[inline]
    fn from(index: Index) -> Self {
        Self(u64::from(index.slot()) | (u64::from(index.generation()) << 32))
    }
}

impl From<ContextId> for Index {
    #[inline]
    fn from(context: ContextId) -> Self {
        Self::new(context.0 as u32, (context.0 >> 32) as u32)
    }
}

/// Worker-local role state stored in one QUIC context slot.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct ListenerContext {
    pub(crate) outer_listener: SessionListenerId,
    pub(crate) inner_application_listener: hammer_runtime::app::ApplicationListenerId,
    pub(crate) inner_session_listener: SessionListenerId,
    pub(crate) configuration: ConfigId,
    pub(crate) reserved: [u8; 16],
}

#[repr(u8)]
enum ConnectionState {
    Handshaking,
    Established,
    Closing,
}

#[repr(C)]
struct ConnectionContext {
    connection: Option<Box<quinn_proto::Connection>>,
    lower_session: SessionId,
    upper_session: Option<SessionId>,
    listener: Option<ContextId>,
    state: ConnectionState,
    reserved: [u8; 7],
}

#[repr(C)]
struct StreamContext {
    parent: Index,
    session: SessionId,
    stream: quinn_proto::StreamId,
    bytes_written: u64,
    reserved: [u8; 24],
}

#[repr(C)]
enum ContextRole {
    Listener(ListenerContext),
    Connection(ConnectionContext),
    Stream(StreamContext),
}

/// Cache-line-aligned QUIC worker context.
///
/// The pool handle supplies the generation identity. Role payloads retain
/// only identities and hot lifecycle facts; the sans-I/O Connection remains
/// out of line behind its owning connection slot.
#[repr(align(64))]
pub(super) struct Context {
    role: ContextRole,
}

impl Context {
    pub(super) fn listener(
        outer_listener: SessionListenerId,
        inner_application_listener: hammer_runtime::app::ApplicationListenerId,
        inner_session_listener: SessionListenerId,
        configuration: ConfigId,
    ) -> Self {
        Self {
            role: ContextRole::Listener(ListenerContext {
                outer_listener,
                inner_application_listener,
                inner_session_listener,
                configuration,
                reserved: [0; 16],
            }),
        }
    }

    pub(super) fn listener_facts(&self) -> Option<ListenerContext> {
        match &self.role {
            ContextRole::Listener(listener) => Some(*listener),
            ContextRole::Connection(_) | ContextRole::Stream(_) => None,
        }
    }

    pub(super) fn connection(lower_session: SessionId, listener: Option<ContextId>) -> Self {
        Self {
            role: ContextRole::Connection(ConnectionContext {
                connection: None,
                lower_session,
                upper_session: None,
                listener,
                state: ConnectionState::Handshaking,
                reserved: [0; 7],
            }),
        }
    }

    pub(super) fn connection_facts(&self) -> Option<(SessionId, Option<ContextId>)> {
        match &self.role {
            ContextRole::Connection(connection) => {
                Some((connection.lower_session, connection.listener))
            }
            ContextRole::Listener(_) | ContextRole::Stream(_) => None,
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Context>() == 64);
    assert!(std::mem::align_of::<Context>() == 64);
};

/// Data Worker-owned QUIC context pool.
#[hammer_component_macros::session_transport(
    name = "quic",
    start_listen = crate::listener::start_listen,
    stop_listen = crate::listener::stop_listen,
)]
pub struct QuicWorker {
    worker: DataWorkerId,
    contexts: Pool<Context>,
}

impl QuicWorker {
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            contexts: Pool::with_capacity(QUIC_CONTEXT_CAPACITY),
        }
    }

    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    pub(super) fn accept_connection(
        &mut self,
        lower_session: SessionId,
        listener: ContextId,
    ) -> RuntimeResult<ContextId> {
        self.contexts
            .insert(Context::connection(lower_session, Some(listener)))
            .map(ContextId::from)
            .ok_or_else(|| {
                QuicWorkerError::ContextCapacityExhausted {
                    capacity: self.contexts.capacity(),
                }
                .into()
            })
    }

    pub(super) fn connect_connection(
        &mut self,
        lower_session: SessionId,
    ) -> RuntimeResult<ContextId> {
        self.contexts
            .insert(Context::connection(lower_session, None))
            .map(ContextId::from)
            .ok_or_else(|| {
                QuicWorkerError::ContextCapacityExhausted {
                    capacity: self.contexts.capacity(),
                }
                .into()
            })
    }

    pub(super) fn connection_facts(
        &self,
        context: ContextId,
    ) -> RuntimeResult<(SessionId, Option<ContextId>)> {
        self.contexts
            .get(context.into())
            .and_then(Context::connection_facts)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context }.into())
    }

    pub(super) fn remove_context(&mut self, context: ContextId) -> RuntimeResult<()> {
        self.contexts
            .remove(context.into())
            .map(drop)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context }.into())
    }

    #[cfg(test)]
    pub(super) fn context_count(&self) -> usize {
        self.contexts.len()
    }
}

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, thiserror::Error)]
pub(super) enum QuicWorkerError {
    #[error("QUIC worker context capacity {capacity} is exhausted")]
    ContextCapacityExhausted { capacity: usize },
    #[error("QUIC context {context:?} is not a worker connection or stream")]
    ContextMissing { context: ContextId },
    #[error("QUIC worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("QUIC worker {worker} is already installed")]
    WorkerAlreadyInstalled { worker: usize },
    #[error("QUIC worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: ThreadOwnedError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_roles_fit_one_cache_line_and_pool_identity_is_generation_checked() {
        assert_eq!(std::mem::size_of::<Context>(), 64);
        assert_eq!(std::mem::align_of::<Context>(), 64);

        let mut contexts: Pool<Context> = Pool::with_capacity(1);
        let first = contexts
            .insert(Context {
                role: ContextRole::Listener(ListenerContext {
                    outer_listener: SessionListenerId::new(1, 1),
                    inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(
                        2, 1,
                    ),
                    inner_session_listener: SessionListenerId::new(3, 1),
                    configuration: ConfigId::from_raw(5),
                    reserved: [0; 16],
                }),
            })
            .expect("listener context capacity");
        assert!(contexts.contains_key(first));
        let removed = contexts.remove(first).expect("remove listener context");
        assert!(matches!(removed.role, ContextRole::Listener(_)));

        let replacement = contexts
            .insert(Context {
                role: ContextRole::Stream(StreamContext {
                    parent: Index::new(5, 1),
                    session: SessionId::from_raw(6),
                    stream: quinn_proto::StreamId::new(
                        quinn_proto::Side::Client,
                        quinn_proto::Dir::Bi,
                        0,
                    ),
                    bytes_written: 0,
                    reserved: [0; 24],
                }),
            })
            .expect("stream context capacity");
        assert_ne!(first, replacement);
        assert!(!contexts.contains_key(first));
        assert!(contexts.contains_key(replacement));
    }

    #[test]
    fn worker_owns_one_context_pool() {
        let worker = QuicWorker::new(DataWorkerId::new(2));
        assert_eq!(worker.worker(), DataWorkerId::new(2));
        assert_eq!(worker.contexts.capacity(), QUIC_CONTEXT_CAPACITY);
    }
}

//! Per-data-worker HTTP/3 connection context ownership.
//!
//! Each data worker owns a bounded, cache-line-aligned pool of connection
//! contexts, mirroring VPP's `http_worker_t` with its per-thread `ctx_pool`
//! (http_private.h:477-482), populated by `http_ctx_alloc_w_thread` through
//! `pool_get_aligned_safe (wrk->ctx_pool, hc, CLIB_CACHE_LINE_BYTES)`
//! (http.c:170-180). Lookups validate liveness before dereferencing
//! (`http_ctx_get_w_thread_if_valid`, http.c:184-189) and free returns the
//! slot to the owning worker's pool (`http_ctx_free`, http.c:198-204). A
//! context binds the exact lower QUIC session: VPP stores the session index
//! in `http_ctx_t::c_s_index` (http.c:752) and resolves it via `session_get`
//! (http.c:876-899); HTTP3 request/stream contexts reference their
//! connection context index and worker (http3.c:35-48).
//!
//! `HttpMain` owns one `HttpWorker` per data worker in a
//! `CacheLine<ThreadOwned<HttpWorker>>` slot, mirroring `QuicMain.workers`
//! (quic listener.rs:58) and VPP's `http_main.wrk`, a fixed array sized by
//! thread count and indexed per thread by `http_worker_get` (http.c:1073,
//! http_private.h:1275-1278). Each worker installs itself once via the
//! `http_worker_init` worker init function, ordered after session/QUIC
//! worker init; `install_worker`/`with_worker` are O(1) slot lookups with
//! typed out-of-range, not-installed, and wrong-thread errors.
//!
//! Identities follow the Hammer QUIC conventions: `ContextId` packs
//! `slot | generation << 32` exactly like the QUIC `ContextId`
//! (quic worker.rs:178-205) and `SessionId` (hammer-service
//! session/id.rs:6-25), with standard `From` conversions to and from `Index`
//! and the packed `u64`. The underlying `Pool` (hammer-infra pool.rs) keeps
//! allocate/get/remove O(1) and generation-checked, with slots aligned to
//! `CACHE_LINE` by the pool itself. Session App callbacks, HTTP3 engine
//! dispatch, QPACK, request publication, and stop_listen are later slices.

use hammer_infra::pool::{Index, Pool};
use hammer_infra::thread_owned::ThreadOwnedError;
use hammer_runtime::DataWorkerId;
use hammer_service::session::SessionId;

/// Default connection-context capacity of one data worker's pool, matching
/// the QUIC per-worker context capacity (quic worker.rs:42).
pub(crate) const HTTP_CONTEXT_CAPACITY: usize = 4_096;

/// Generation-checked identity for one HTTP/3 connection context in the
/// owning data worker's pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct ContextId(u64);

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

/// Cold per-connection state bound to one data-worker context slot.
///
/// Holds exactly the lower QUIC `SessionId` the context was allocated for;
/// hot HTTP/3 connection state (frames, QPACK, streams) belongs to later
/// slices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectionContext {
    /// Lower QUIC session this connection context is bound to.
    pub(crate) session: SessionId,
}

/// Typed errors for per-worker connection context operations and container
/// install/lookup, mirroring `QuicWorkerError`'s container variants
/// (quic worker.rs:3556-3567).
#[hammer_component_macros::runtime_error(subsystem = "http")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HttpWorkerError {
    #[error("http connection context pool is full (capacity {capacity})")]
    ContextCapacityExhausted { capacity: usize },
    #[error("http connection context {context:?} is not live")]
    ContextMissing { context: ContextId },
    #[error(
        "http connection context {context:?} is bound to session {actual:?}, expected {expected:?}"
    )]
    SessionMismatch {
        context: ContextId,
        expected: SessionId,
        actual: SessionId,
    },
    #[error("http worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("http worker {worker} is already installed")]
    WorkerAlreadyInstalled { worker: usize },
    #[error("http worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: ThreadOwnedError,
    },
}

/// Data-worker-owned bounded pool of HTTP/3 connection contexts.
///
/// Owns `ConnectionContext` slots exactly as VPP's `http_worker_t::ctx_pool`
/// does; callers resolve identities by `ContextId`, never by raw index.
/// The container (worker installation/attachment) is deferred until Session
/// App callbacks need it.
#[derive(Debug)]
pub(crate) struct HttpWorker {
    contexts: Pool<ConnectionContext>,
}

impl HttpWorker {
    /// Constructs the worker for one data worker id.
    ///
    /// Called once per data worker by the `http_worker_init` worker init
    /// function (listener.rs), mirroring `QuicWorker::new` (quic
    /// worker.rs:684).
    pub(crate) fn new(_worker: DataWorkerId) -> Self {
        Self::with_capacity(HTTP_CONTEXT_CAPACITY)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            contexts: Pool::with_capacity(capacity),
        }
    }

    /// Allocates a context slot bound to the exact lower QUIC `session`.
    ///
    /// O(1); fails with `ContextCapacityExhausted` when the pool is full.
    pub(crate) fn allocate(&mut self, session: SessionId) -> Result<ContextId, HttpWorkerError> {
        self.contexts
            .insert(ConnectionContext { session })
            .map(ContextId::from)
            .ok_or(HttpWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            })
    }

    /// Resolves a live context by its generation-checked identity.
    ///
    /// O(1); rejects stale or out-of-range identities with
    /// `ContextMissing`.
    pub(crate) fn get(&self, context: ContextId) -> Result<&ConnectionContext, HttpWorkerError> {
        self.contexts
            .get(context.into())
            .ok_or(HttpWorkerError::ContextMissing { context })
    }

    /// Resolves a context and verifies it is bound to the exact `session`.
    ///
    /// O(1); rejects mismatched bindings with `SessionMismatch` so a stale
    /// context can never be attributed to a different lower session.
    pub(crate) fn get_for_session(
        &self,
        context: ContextId,
        session: SessionId,
    ) -> Result<&ConnectionContext, HttpWorkerError> {
        let connection = self.get(context)?;
        if connection.session != session {
            return Err(HttpWorkerError::SessionMismatch {
                context,
                expected: session,
                actual: connection.session,
            });
        }
        Ok(connection)
    }

    /// Releases a context slot back to the pool.
    ///
    /// O(1); the slot's generation advances, so previously issued identities
    /// become stale. Fails with `ContextMissing` for non-live identities.
    pub(crate) fn remove(&mut self, context: ContextId) -> Result<(), HttpWorkerError> {
        self.contexts
            .remove(context.into())
            .map(|_| ())
            .ok_or(HttpWorkerError::ContextMissing { context })
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.contexts.len()
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.contexts.capacity()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.contexts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(slot: u32, generation: u32) -> SessionId {
        SessionId::from_raw(u64::from(slot) | (u64::from(generation) << 32))
    }

    #[test]
    fn allocate_returns_generation_safe_id_bound_to_exact_session() {
        let mut worker = HttpWorker::with_capacity(4);
        let lower = session(3, 1);
        let context = worker.allocate(lower).expect("allocate succeeds");
        assert_eq!(worker.get(context).expect("live context").session, lower);
        assert_eq!(ContextId::from(Index::from(context)), context);
        assert_eq!(ContextId::from(u64::from(context)), context);
        let index = Index::from(context);
        assert_eq!(index.slot(), 0);
        assert_eq!(index.generation(), 1);
        assert_eq!(worker.len(), 1);
        assert_eq!(worker.capacity(), 4);
        assert!(!worker.is_empty());
    }

    #[test]
    fn distinct_allocations_keep_distinct_bindings() {
        let mut worker = HttpWorker::with_capacity(4);
        let first = worker.allocate(session(1, 1)).expect("allocate first");
        let second = worker.allocate(session(2, 1)).expect("allocate second");
        assert_ne!(first, second);
        assert_eq!(
            worker.get(first).expect("first live").session,
            session(1, 1)
        );
        assert_eq!(
            worker.get(second).expect("second live").session,
            session(2, 1)
        );
    }

    #[test]
    fn stale_id_rejected_after_remove_and_reuse() {
        let mut worker = HttpWorker::with_capacity(4);
        let first = worker.allocate(session(5, 1)).expect("allocate");
        worker.remove(first).expect("remove");
        let second = worker.allocate(session(7, 2)).expect("reuse slot");
        assert_ne!(first, second);
        assert_eq!(Index::from(second).slot(), Index::from(first).slot());
        assert_ne!(
            Index::from(second).generation(),
            Index::from(first).generation()
        );
        assert_eq!(
            worker.get(first),
            Err(HttpWorkerError::ContextMissing { context: first })
        );
        assert_eq!(
            worker.get(second).expect("new identity live").session,
            session(7, 2)
        );
    }

    #[test]
    fn capacity_exhaustion_is_typed_error() {
        let mut worker = HttpWorker::with_capacity(1);
        worker.allocate(session(1, 1)).expect("first slot");
        assert_eq!(
            worker.allocate(session(2, 1)),
            Err(HttpWorkerError::ContextCapacityExhausted { capacity: 1 })
        );
        let mut empty = HttpWorker::with_capacity(0);
        assert!(empty.is_empty());
        assert_eq!(
            empty.allocate(session(3, 1)),
            Err(HttpWorkerError::ContextCapacityExhausted { capacity: 0 })
        );
    }

    #[test]
    fn session_mismatch_rejected_by_direct_id_lookup() {
        let mut worker = HttpWorker::with_capacity(2);
        let bound = session(1, 1);
        let other = session(2, 1);
        let context = worker.allocate(bound).expect("allocate");
        assert_eq!(
            worker.get_for_session(context, other),
            Err(HttpWorkerError::SessionMismatch {
                context,
                expected: other,
                actual: bound,
            })
        );
        assert_eq!(
            worker
                .get_for_session(context, bound)
                .expect("exact session")
                .session,
            bound
        );
    }

    #[test]
    fn remove_missing_or_stale_identity_is_typed_error() {
        let mut worker = HttpWorker::with_capacity(2);
        let bogus = ContextId::from(u64::MAX);
        assert_eq!(
            worker.remove(bogus),
            Err(HttpWorkerError::ContextMissing { context: bogus })
        );
        let context = worker.allocate(session(1, 1)).expect("allocate");
        worker.remove(context).expect("remove");
        assert_eq!(
            worker.remove(context),
            Err(HttpWorkerError::ContextMissing { context })
        );
    }
}

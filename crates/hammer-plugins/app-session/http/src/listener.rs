//! Main Thread-owned HTTP listener authority: nested QUIC listen bootstrap.
//!
//! VPP owns the HTTP transport proto on the main thread: `http_transport_init`
//! (http.c:1867-1903) registers the proto against the session layer and
//! initializes the main-thread `http_main_t`, and `http_transport_enable`
//! (http.c:1023-1063) runs the main-thread attach of the builtin HTTP app.
//! `http_start_listen` (http.c:1287) then creates a real listener: it
//! allocates the `http_ctx_t` listener context, drives `vnet_listen` on the
//! lower QUIC transport (http.c:1381-1388), links the resulting transport
//! listener to the HTTP listener by writing `ts_listener->opaque = hl_index`
//! (`http_listener_link_with_tl`, http.c:1275-1284), and only at the end
//! links the outer Application listener. The listen path rolls back in
//! reverse order on failure: the lower QUIC listen is torn down with
//! `vnet_unlisten` before the `http_ctx_t` is freed (http.c:1405-1415).
//!
//! Hammer mirrors that authority with `HttpMain`, initialized once by
//! `init_http_transport` ordered after QUIC/session init, and nests the lower
//! QUIC listen inside the same Main Thread worker barrier transaction:
//! register an inner Application listener owned by HTTP's builtin Session
//! App, call `SessionMain::listen` on the fetched QUIC transport, then store
//! the direct listener context in the O(1) outer-listener slot map as the
//! final publication. The inner Application listener's opaque is deliberately
//! left as the forwarded config — QUIC's `start_listen` consumes it as its
//! registered server config (quic listener.rs:135-139) — so the context
//! identity never overwrites the config opaque (contrast QUIC, which
//! registers its own inner listener with a `None` opaque and publishes its
//! context id there, quic listener.rs:142-187). A missing outer config is a
//! typed prerequisite error before any side effect, and HTTP has no QUIC
//! config registration seam in this slice. `HttpMain` also owns the
//! per-data-worker HTTP worker
//! container, mirroring `QuicMain.workers` (quic listener.rs:58) and VPP's
//! `http_main.wrk` array (http.c:1073); each worker installs itself once
//! via the `http_worker_init` worker init function ordered after
//! session/QUIC worker init, exactly as QUIC binds its workers (quic
//! listener.rs:568-577). `stop_listen` (http.c:1453-1490) is mirrored with
//! the same strict ordering: lower QUIC Session unlisten first (a typed
//! failure preserves the context), inner Application listener removal
//! second, outer HTTP context clear last. The HTTP3 engine, FIFO, QPACK,
//! and Session App lifecycle are later slices.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use hammer_infra::align::CacheLine;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::SessionHandle;
use hammer_runtime::{DataWorkerId, Engine, RuntimeError, RuntimeResult, SessionListenEndpoint};
use hammer_service::session::application_main;
use hammer_service::session::runtime::session_main;
use hammer_service::transport::{TransportVft, register_transport};

use crate::http_app;
use crate::worker::{HttpWorker, HttpWorkerError};

/// Bounds the HTTP listener-context table.
///
/// The table is indexed by the outer `SessionHandle`'s session-pool slot,
/// and the session pool is bounded by `DEFAULT_SESSION_POOL_CAPACITY`
/// (session/runtime.rs:49), so every live outer listener's slot fits. A
/// listener whose slot exceeds the bound is rejected by the capacity preflight.
/// preflight before any side effect.
const HTTP_LISTENER_CONTEXT_CAPACITY: usize = 1024;

#[hammer_component_macros::runtime_error(subsystem = "http")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum HttpListenerError {
    #[error("HTTP listener context capacity {capacity} is exhausted")]
    ListenerCapacityExhausted { capacity: usize },
    #[error("HTTP listener {listener:?} is not registered")]
    ListenerMissing { listener: SessionHandle },
    #[error(
        "HTTP listen requires a QUIC server config, but QUIC config registration \
         is not installed in this slice"
    )]
    ConfigRequired,
}

/// Direct HTTP listener context: outer session identity plus the nested
/// inner Application listener and the lower QUIC `SessionListener` it owns.
///
/// `Copy`: `stop_listen` copies the validated context out of the O(1) slot
/// table and then tears down the nested identities by value.
#[derive(Clone, Copy)]
struct ListenerContext {
    outer_listener: SessionHandle,
    outer_application: u32,
    outer_config: Option<u64>,
    inner_application_listener: u32,
    inner_session_listener: SessionHandle,
}

/// Bounded O(1) outer-listener to listener-context table.
///
/// Indexed by the outer `SessionHandle`'s session-pool slot: live listeners
/// own their pool slot, so a slot index resolves the context in O(1) with no
/// pool scan, unlike QUIC's context `Pool`.
/// a fixed contiguous `Vec` of `Option` slots, allocated up front; capacity
/// is preflighted before any Session state is published.
struct HttpListenerContexts {
    slots: UnsafeCell<Vec<Option<ListenerContext>>>,
    by_connection: UnsafeCell<HashMap<u32, usize>>,
}

impl HttpListenerContexts {
    fn new() -> Self {
        Self {
            slots: UnsafeCell::new((0..HTTP_LISTENER_CONTEXT_CAPACITY).map(|_| None).collect()),
            by_connection: UnsafeCell::new(HashMap::new()),
        }
    }

    #[inline]
    fn get(&self) -> &Vec<Option<ListenerContext>> {
        // SAFETY: Main Thread mutates the table only while the worker barrier
        // is held. Later slices read it from Data Workers only after that
        // publication barrier and never while a writer can access it.
        unsafe { &*self.slots.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn get_mut(&self) -> &mut Vec<Option<ListenerContext>> {
        // SAFETY: callers must be the Main Thread and hold the worker barrier,
        // which is enforced at each Main Thread call site.
        unsafe { &mut *self.slots.get() }
    }

    #[inline]
    fn connection_slot(&self, connection_index: u32) -> Option<usize> {
        // SAFETY: reads occur after the Main Thread publication barrier.
        unsafe { (&*self.by_connection.get()).get(&connection_index).copied() }
    }

    #[inline]
    fn publish_connection_slot(&self, connection_index: u32, slot: usize) {
        // SAFETY: writes occur only on the Main Thread under the worker barrier.
        unsafe {
            (&mut *self.by_connection.get()).insert(connection_index, slot);
        }
    }

    #[inline]
    fn remove_connection_slot(&self, connection_index: u32) {
        // SAFETY: writes occur only on the Main Thread under the worker barrier.
        unsafe {
            (&mut *self.by_connection.get()).remove(&connection_index);
        }
    }
}

// SAFETY: access to the context table is ordered by the Session worker
// barrier; future Data Worker reads observe only barrier-published state.
unsafe impl Send for HttpListenerContexts {}
unsafe impl Sync for HttpListenerContexts {}

/// Main Thread-owned HTTP listener authority.
///
/// VPP reference: `http_main_t` is owned by the main vlib thread
/// (http.c:1867-1903). HTTP rides over QUIC, so the authority retains the
/// plus the inner Application attached at init and the builtin HTTP Session
/// App id. The lower QUIC VFT remains owned by the process-global transport
/// slot table and is resolved by protocol at each Session operation.
pub struct HttpMain {
    protocol: u8,
    session_app: u32,
    inner_application: u32,
    contexts: HttpListenerContexts,
    /// One cache-line-aligned, thread-owned `HttpWorker` per configured data
    /// worker, mirroring `QuicMain.workers` (quic listener.rs:58).
    workers: Box<[CacheLine<ThreadOwned<HttpWorker>>]>,
}

impl HttpMain {
    /// Session-layer transport identity for HTTP.
    pub(crate) fn new(
        protocol: u8,
        session_app: u32,
        inner_application: u32,
        worker_count: usize,
    ) -> Self {
        Self {
            protocol,
            session_app,
            inner_application,
            contexts: HttpListenerContexts::new(),
            workers: (0..worker_count)
                .map(|_| CacheLine::new(ThreadOwned::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    pub const fn protocol(&self) -> u8 {
        self.protocol
    }

    /// Nests the lower QUIC listen inside the current Main Thread worker
    /// barrier transaction, then publishes the context only last.
    ///
    /// Mirrors `http_start_listen` (http.c:1287-1440): the HTTP listener
    /// context is created before the lower listen and linked to it only after
    /// the listen succeeds. Rollback is reverse order on failure — the lower
    /// QUIC `SessionListener` is removed by the session layer itself on a
    /// failed `SessionMain::listen` (session/runtime.rs:925-933), then the
    /// inner Application listener is removed here; the context is stored only
    /// after the listen succeeded, and the table capacity was preflighted, so
    /// the final store is invariant-guarded exactly as in QUIC
    /// (quic listener.rs:159-187). The inner Application listener's opaque is
    /// never overwritten: it remains the forwarded config consumed by the
    /// lower QUIC listen. The primary typed error is preserved; cleanup steps
    /// are infallible invariants, so no aggregate error variant is needed in
    /// this path (the `WorkerGraphRollbackFailed` aggregation pattern belongs
    /// to the worker-graph slice).
    pub(crate) fn start_listen(
        &self,
        outer_listener: SessionHandle,
        outer_application: u32,
        outer_config: Option<u64>,
        endpoint: SessionListenEndpoint,
    ) -> RuntimeResult<u32> {
        hammer_runtime::ensure_main_thread_with_barrier()?;

        // Capacity preflight before any Session state is published: a single
        // O(1) bounds check on the outer listener's session-pool slot.
        if outer_listener.session_index as usize >= HTTP_LISTENER_CONTEXT_CAPACITY {
            return Err(HttpListenerError::ListenerCapacityExhausted {
                capacity: HTTP_LISTENER_CONTEXT_CAPACITY,
            }
            .into());
        }
        // The lower QUIC listen consumes the forwarded config as its own
        // registered server config (quic listener.rs:135-139). HTTP has no
        // QUIC config registration seam in this slice, so a missing config is
        // a typed prerequisite error before any side effect — never a
        // reported listener that does not exist.
        let config = outer_config.ok_or(HttpListenerError::ConfigRequired)?;

        let applications = application_main();
        let quic_protocol = hammer_plugin_quic::protocol()?;
        let inner_application_listener = applications
            .register_listener(self.inner_application, Some(self.session_app), Some(config))
            .map_err(RuntimeError::from)?;
        let inner_session_listener =
            match session_main().listen(inner_application_listener, quic_protocol, endpoint) {
                Ok(listener) => listener,
                Err(error) => {
                    applications
                        .remove_listener(self.inner_application, inner_application_listener)
                        .expect("failed QUIC inner listen leaves Application listener available");
                    return Err(error.into());
                }
            };

        // Capacity was checked before any Session state was published, so a
        // failed store would be an impossible table invariant violation.
        hammer_runtime::ensure_main_thread_with_barrier()?;
        self.contexts.get_mut()[outer_listener.session_index as usize] = Some(ListenerContext {
            outer_listener,
            outer_application,
            outer_config,
            inner_application_listener,
            inner_session_listener,
        });
        self.contexts.publish_connection_slot(
            inner_session_listener.session_index,
            outer_listener.session_index as usize,
        );

        // The context store above is the final publication step, and it is
        // deliberately the only one: the inner Application listener's opaque
        // stays the forwarded config that the lower QUIC listen consumed as
        // its registered server config (quic listener.rs:135-139), so the
        // context identity never overwrites the config opaque.
        Ok(inner_session_listener.session_index)
    }

    /// Tears down one nested listener in strict reverse order of
    /// `start_listen`, mirroring `http_stop_listen` (http.c:1453-1490): the
    /// lower QUIC Session unlisten runs first, the inner Application listener
    /// is removed second, and the outer HTTP context is cleared last.
    ///
    /// The context is resolved by an O(1) lookup on the outer listener's
    /// session-pool slot with full identity validation (`slot` and
    /// `generation`), unlike QUIC's pool scan (quic listener.rs:190-201); a
    /// missing or stale outer listener is a typed `ListenerMissing` error
    /// before any side effect. A failed lower unlisten returns the typed
    /// error and preserves the whole context — the nested identities remain
    /// published for a retry, exactly as `http_stop_listen` warns and keeps
    /// its `http_ctx_t` alive when `vnet_unlisten` fails (http.c:1464-1485).
    /// Inner Application removal is fallible and its typed error is
    /// propagated (contrast QUIC's invariant `expect`, quic
    /// listener.rs:204-209); the context is then still cleared only after
    /// both lower teardown steps succeeded, so the final slot store is an
    /// infallible indexed write into the slot the lookup already validated.
    pub(crate) fn stop_listen(&self, connection_index: u32) -> RuntimeResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let context = self
            .contexts
            .connection_slot(connection_index)
            .and_then(|slot| {
                self.contexts
                    .get()
                    .get(slot)
                    .and_then(Option::as_ref)
                    .copied()
            })
            .ok_or(HttpListenerError::ListenerMissing {
                listener: SessionHandle::new(connection_index, 0),
            })?;

        // Lower QUIC Session unlisten first; a typed failure preserves the
        // context (the slot store below never runs).
        session_main()
            .unlisten(context.inner_session_listener)
            .map_err(RuntimeError::from)?;
        // Inner Application listener removal second.
        application_main()
            .remove_listener(self.inner_application, context.inner_application_listener)
            .map_err(RuntimeError::from)?;
        // Outer HTTP context clear last, after both lower teardowns
        // succeeded. The lookup validated the slot, so the indexed store is
        // infallible.
        hammer_runtime::ensure_main_thread_with_barrier()?;
        self.contexts.get_mut()[context.outer_listener.session_index as usize] = None;
        self.contexts.remove_connection_slot(connection_index);
        Ok(())
    }

    /// Installs the data worker's `HttpWorker` once, bound to the current
    /// thread.
    ///
    /// O(1). The slot is addressed by the exact `DataWorkerId`; out-of-range
    /// ids and duplicate or cross-thread installs are typed errors. Mirrors
    /// `QuicMain::install_worker` (quic listener.rs:234-246).
    pub(crate) fn install_worker(&self, worker: DataWorkerId) -> RuntimeResult<()> {
        let slot =
            self.workers
                .get(worker.slot())
                .ok_or_else(|| HttpWorkerError::WorkerOutOfRange {
                    worker: worker.slot(),
                })?;
        slot.install(HttpWorker::new(worker)).map_err(|_| {
            RuntimeError::from(HttpWorkerError::WorkerAlreadyInstalled {
                worker: worker.slot(),
            })
        })
    }

    /// Runs `operation` on the exact data worker's installed `HttpWorker`.
    ///
    /// O(1). Out-of-range ids fail with `WorkerOutOfRange`; access from any
    /// thread other than the installing one, or before install, fails with
    /// `WorkerAccess`. Mirrors `QuicMain::with_worker` (quic
    /// listener.rs:248-264).
    pub(crate) fn with_worker<R>(
        &self,
        worker: DataWorkerId,
        operation: impl FnOnce(&mut HttpWorker) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let slot = self.workers.get(worker.slot()).ok_or_else(|| {
            RuntimeError::from(HttpWorkerError::WorkerOutOfRange {
                worker: worker.slot(),
            })
        })?;
        slot.with_mut(operation).map_err(|source| {
            RuntimeError::from(HttpWorkerError::WorkerAccess {
                worker: worker.slot(),
                source,
            })
        })?
    }
}

/// Process-wide authority published by `init_http_transport`; `pub(crate)` so
/// the Session App `accept` callback resolves the owning worker by Session
/// worker id (http_app.rs).
pub(crate) static HTTP_MAIN: OnceLock<HttpMain> = OnceLock::new();

pub fn protocol() -> RuntimeResult<u8> {
    HTTP_MAIN
        .get()
        .map(HttpMain::protocol)
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "http" })
}

/// Registered `TransportStartListen` callback.
///
/// Checks the main thread and worker barrier, then authority initialization,
/// returning typed errors before any side effect. Mirrors the QUIC transport
/// callback gate (quic listener.rs:466-477).
pub(crate) fn start_listen(
    listener: SessionHandle,
    application: u32,
    config: Option<u64>,
    endpoint: SessionListenEndpoint,
) -> RuntimeResult<u32> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "http" })?
        .start_listen(listener, application, config, endpoint)
}

/// Registered `TransportStopListen` callback.
///
/// Mirrors the `start_listen` gate: checks the main thread and worker
/// barrier, then the authority, returning typed errors before any side
/// effect. `SessionMain::unlisten` dispatches here with the lower transport's
/// `connection_index` (session/runtime.rs:959-982).
pub(crate) fn stop_listen(connection_index: u32) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "http" })?
        .stop_listen(connection_index)
}

/// Publish the main-thread HTTP listener authority.
///
/// Ordered after QUIC/session init so the QUIC transport registration is
/// already resolvable from the plugin main (mirroring `init_quic`,
/// quic listener.rs:487-520). Resolves the builtin HTTP Session App and
/// attaches the inner Application exactly as QUIC does for its inner
/// listener (quic listener.rs:493-504). Duplicate initialization is a typed
/// error and detaches the just-attached inner Application; the OnceLock keeps
/// the authority single-owner with no lock.
#[hammer_component_macros::init_function(
    name = "http_transport_init",
    runs_after = ["quic_init", "session_init"]
)]
fn init_http_transport(engine: &mut Engine) -> RuntimeResult<()> {
    if HTTP_MAIN.get().is_some() {
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "http" });
    }
    let quic_protocol = hammer_plugin_quic::protocol()?;
    let inner_application = application_main().attach().map_err(RuntimeError::from)?;
    let session_app = match http_app::register(inner_application) {
        Ok(session_app) => session_app,
        Err(error) => {
            application_main()
                .detach(inner_application)
                .expect("failed HTTP Session App registration leaves no inner Application");
            return Err(error);
        }
    };
    let protocol = match register_transport(TransportVft::new(
        Some(start_listen),
        Some(stop_listen),
        None,
        None,
        None,
        None,
        None,
        None,
    )) {
        Ok(protocol) => protocol,
        Err(error) => {
            application_main()
                .detach(inner_application)
                .expect("failed HTTP VFT registration leaves no inner Application");
            return Err(error.into());
        }
    };
    let main = HttpMain::new(
        protocol,
        session_app,
        inner_application,
        engine.configured_worker_count(),
    );
    if HTTP_MAIN.set(main).is_err() {
        application_main()
            .detach(inner_application)
            .expect("duplicate HTTP initialization leaves no published inner Application");
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "http" });
    }
    Ok(())
}

/// Install the current data worker's HTTP worker.
///
/// Runs once per data worker, ordered after session and QUIC worker init so
/// lower transport state exists before HTTP contexts can bind to it,
/// mirroring `init_quic_worker` (quic listener.rs:568-577) and VPP's
/// per-thread `http_worker_t` slot in `http_main.wrk` (http.c:1073).
#[hammer_component_macros::worker_init_function(
    name = "http_worker_init",
    runs_after = ["session_worker_init", "quic_worker_init"]
)]
fn init_http_worker(engine: &mut Engine) -> RuntimeResult<()> {
    let main = HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "http" })?;
    main.install_worker(engine.data_worker_id()?)
}

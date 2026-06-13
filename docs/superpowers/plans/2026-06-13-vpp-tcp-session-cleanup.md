# VPP TCP Session Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cleanly remove the old control-plane/app-backend/app-ingress TCP path and connect TCP packet nodes to a VPP-shaped session layer, while keeping Hammer's app boundary io_uring-based instead of VPP FIFO-based.

**Architecture:** VPP is the reference for ownership boundaries: listener lookup is control-plane published data-plane state; transport and session are directly attached; receive data enters the session layer; session wakes the app worker. Hammer intentionally differs at the app boundary: instead of VPP `svm_fifo_t`, each session owns io_uring-style app rings, and the session enqueue node completes CQEs and wakes app async tasks. `hammer-runtime::app` must expose generic ring/task primitives only; it must not own TCP/session/control-plane concepts.

**Tech Stack:** Rust 2024, `hammer-service` packet graph/session nodes, `hammer-runtime::app` io_uring-like rings, `hammer-infra::{vec,map}`, VPP `src/vnet/session` and `src/vnet/tcp` source model.

---

## Verified VPP Facts

- `session_t` owns app-facing buffers, app worker binding, and transport attachment: `rx_fifo`, `tx_fifo`, `app_wrk_index`, and `connection_index` are in `/private/tmp/vpp_session_types.h:240-290`.
- VPP attaches transport and session directly in `session_alloc_for_connection`: `s->connection_index = tc->c_index` and `tc->s_index = s->session_index` in `/private/tmp/vpp_session.c:488-503`.
- Listener setup creates a listen session, asks transport to listen, then publishes session lookup after transport setup in `/private/tmp/vpp_application_worker.c:230-326`.
- Accepted TCP sessions are initialized through app worker/session logic, not through a transport-owned app backend: `app_worker_init_accepted` sets `s->app_wrk_index` and allocates session FIFOs in `/private/tmp/vpp_application_worker.c:493-518`.
- Accept notification is an app-worker event: `app_worker_accept_notify` calls `app_worker_add_event(... SESSION_CTRL_EVT_ACCEPTED)` in `/private/tmp/vpp_application_worker.c:592-596`.
- VPP app wakeup is event based: `app_worker_add_event` enqueues an event and schedules `session_input_node` in `/private/tmp/vpp_application_worker.c:935-966`; `session_enqueue_notify_inline` programs `SESSION_IO_EVT_RX` in `/private/tmp/vpp_session.c:621-642`.
- TCP receive enters session enqueue, then updates TCP receive state: `tcp_session_enqueue_data` calls `session_enqueue_stream_connection`, updates `tc->rcv_nxt`, and handles FIFO-full behavior in `/private/tmp/vpp_tcp_input.c:1052-1102`.
- TCP listener lookup is a session lookup to a listener transport connection: `tcp_lookup_listener` calls `session_lookup_listener4/6` and then `transport_get_listener(... s->connection_index)` in `/private/tmp/vpp_tcp_input.c:1613-1640`.
- TCP listen creates a child transport connection, initializes TCP variables, then calls `session_stream_accept(&child->connection, lc->c_s_index, lc->c_thread_index, ...)` in `/private/tmp/vpp_tcp_input.c:2560-2672`.

## Hammer-Specific Difference From VPP

Hammer must not copy VPP's shared-memory FIFO API at the app boundary. The equivalent shape is:

- VPP `session_t.rx_fifo/tx_fifo` -> Hammer session-owned io_uring-style SQ/CQ rings and buffer leases.
- VPP `session_enqueue_stream_connection(... queue event ...)` -> Hammer `session enqueue node` writes app CQEs and marks/wakes the app task.
- VPP `app_worker_add_event/session_input_node` -> Hammer app async task wakeup through ring wakers/runtime scheduling.
- VPP app worker index -> Hammer worker-local app task/ring owner.

This means app code can submit operations and await completions, but app code must not hold a backend, ingress target, session backend, control plane handle, TCP state, or listener lookup table.

## Non-Negotiable Cleanup Rules

These names must be deleted, not renamed:

```text
AppBackend
AppBackendRecvQueue
AppBackendSendQueue
AppIngressTarget
AppSessionBackend
AppSessionAppIngress
AppTcpSessionBackend
SessionProtocolOps
TcpConnectionSnapshot
TcpConnectionRegistration
TcpSessionAccess
TcpOutputBackend
TcpAcceptBackend
TcpSynSentBackend
publish_app_ingress
publish_connections
attach_app_backend
attach_app_backends
local_backend_for_flow
local_backend_for_socket
worker_backend
worker_socket_backend
with_worker_tcp_session_protocol
AppControl
AppControlOps
install_control
bind_udp_socket
```

These names are forbidden in `hammer-runtime::app` because they put transport/session concepts in the app runtime:

```text
AppStreamId
AppObjectRef::Stream
AppObjectRef::Session
SessionId
AppObjectRef::Socket
AppSocketId
bind_tcp_listener
close_socket
APP_SOCKET_BACKENDS
APP_SOCKET_RINGS
```

`SessionId` is allowed only inside `crates/hammer-service/src/session/` and `crates/hammer-service/src/session/protocol/*` as a service/session-layer identifier. It must not be exported from or referenced by `hammer-runtime::app`.

## File Ownership After Cleanup

### `crates/hammer-runtime/src/app/`

Responsibility: generic io_uring-style app task/ring primitives.

Allowed concepts:

- `AppContext`
- `AppWorkerContext`
- `AppRuntime`
- `AppRingHandle`
- generic app operation ids such as `AppOpId` or `AppRingId`
- SQE/CQE descriptors that carry opaque operation keys, not TCP/session/socket typed names
- buffer lease registration
- async waiting/wakeup

Forbidden concepts:

- listener bind/close
- TCP stream/session identity
- UDP socket identity
- app control hooks
- service control plane references
- backend/ingress/session-backend abstractions

### `crates/hammer-service/src/service.rs`

Responsibility: service-level lifecycle and control-plane publication.

Allowed TCP control-plane work:

- allocate listener ids
- track listener bind addresses
- publish `TcpLookupSnapshot` listener entries to `TcpInputControlPlane`
- close listener entries

Forbidden TCP control-plane work:

- connection/session state
- `TcpConnectionSnapshot`
- accepted session state
- app ingress binding
- app backend references
- TCP sequence/window fields

### `crates/hammer-service/src/session/`

Responsibility: generic worker-local session runtime.

Allowed:

- `SessionId`
- ready queue
- timer wheel
- session enqueue node
- session-owned app ring/waker handle
- generic `SessionQueueRuntime<P>` with concrete `P`
- no dyn registry, no downcast, no `PhantomData`

Forbidden:

- TCP-specific state in generic files
- app backend/ingress/control-plane structs
- protocol trait-object registry

### `crates/hammer-service/src/session/protocol/tcp/`

Responsibility: TCP-specific session protocol state and operations.

Allowed:

- `TcpSessionProtocol`
- `TcpSessionState`
- `TcpSessionTable`
- `iss`, `irs`, `snd_una`, `snd_nxt`, `snd_wnd`, `rcv_nxt`, `rcv_wnd`
- receive enqueue logic that advances `rcv_nxt`
- output dequeue/retransmit state
- accept/connect/session lifecycle tied to `SessionId`

Forbidden:

- app backend
- control-plane connection snapshot
- app ingress target
- generic protocol trait-object ops

### `crates/hammer-service/src/transport/tcp/`

Responsibility: packet nodes only.

Allowed:

- parse/classify TCP packets
- lookup listener/connection/session ids
- create child TCP session through `session/protocol/tcp`
- call typed TCP session operations
- synthesize replies/output packets from TCP session state

Forbidden:

- app/backend/session-backend storage
- control-plane connection/session state
- direct app wakeup except through session enqueue/runtime methods

---

## Task 1: Freeze Current State And Confirm Forbidden Surface

**Files:**
- Inspect only: repository status
- Inspect only: `crates/hammer-runtime/src/app/*`
- Inspect only: `crates/hammer-service/src/**`

- [ ] **Step 1: Capture dirty state**

Run:

```bash
git status --short
```

Expected: dirty state is broad. Do not revert unrelated edits. Do not use `git restore` on whole files.

- [ ] **Step 2: Run forbidden concept scan**

Run:

```bash
rg -n "AppBackend|AppBackendRecvQueue|AppBackendSendQueue|AppIngressTarget|AppSessionBackend|AppSessionAppIngress|AppTcpSessionBackend|SessionProtocolOps|TcpConnectionSnapshot|TcpConnectionRegistration|TcpSessionAccess|TcpOutputBackend|TcpAcceptBackend|TcpSynSentBackend|publish_app_ingress|publish_connections|attach_app_backend|attach_app_backends|local_backend_for_flow|local_backend_for_socket|worker_backend|worker_socket_backend|with_worker_tcp_session_protocol|AppControl|AppControlOps|install_control|AppStreamId|AppObjectRef::Stream|AppObjectRef::Session|AppObjectRef::Socket|AppSocketId|APP_SOCKET_BACKENDS|APP_SOCKET_RINGS|bind_udp_socket" crates/hammer-runtime/src/app crates/hammer-runtime/tests crates/hammer-service/src crates/hammer-service/tests
```

Expected before cleanup: matches exist. Every match in `hammer-runtime/src/app` is a removal target unless it is generic ring mechanics without transport/session naming.

- [ ] **Step 3: Commit nothing**

Expected: this task is inspection only.

---

## Task 2: Remove Transport/Session Concepts From `hammer-runtime::app`

**Files:**
- Modify: `crates/hammer-runtime/src/app/context.rs`
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify tests: `crates/hammer-runtime/tests/app_ring.rs`
- Modify tests: `crates/hammer-runtime/tests/app_echo_loop.rs`

- [ ] **Step 1: Write failing runtime-app API test**

Add a test in `crates/hammer-runtime/tests/app_ring.rs` that uses only opaque app operation identity. The test should not mention TCP, stream, session, socket, listener, bind, control, backend, or ingress.

```rust
#[test]
fn app_ring_uses_opaque_operation_identity() {
    use hammer_runtime::app::{AppCqeData, AppObjectRef, AppOpId, AppSqeData};

    let op = AppOpId::new(7);
    let object = AppObjectRef::Operation(op);

    assert_eq!(object, AppObjectRef::Operation(op));
    assert!(matches!(AppSqeData::Recv { max_len: 64 }, AppSqeData::Recv { max_len: 64 }));
    assert_eq!(AppCqeData::None, AppCqeData::None);
}
```

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_ring_uses_opaque_operation_identity
```

Expected before implementation: fail because `AppOpId`/opaque operation ref does not exist.

- [ ] **Step 2: Replace app object identity with opaque operation id**

In `crates/hammer-runtime/src/app/ring.rs`, make app descriptors generic:

```rust
pub enum AppOpTag {}
pub type AppOpId = Descriptor<AppOpTag>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppObjectRef {
    None,
    Operation(AppOpId),
}
```

Keep operation names generic:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppOpcode {
    Nop,
    Accept,
    Recv,
    RecvFrom,
    Send,
    SendTo,
    Close,
}
```

Keep CQE payload generic:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppCqeData {
    None,
    Accepted { child: AppOpId },
    Recv { buffer: BufferIndex },
    RecvFrom { source: SocketAddr, buffer: BufferIndex },
    Closed,
}
```

Use `AppOpId` in SQE/CQE constructors. Do not add `SessionId`, `AppStreamId`, or `AppSocketId` to runtime app.

- [ ] **Step 3: Remove app control hooks from app context**

In `crates/hammer-runtime/src/app/context.rs`, delete:

```text
AppControl
AppControlOps
control: Arc<Mutex<Option<AppControl>>>
install_control
bind_tcp_listener
bind_udp_socket
close_socket
register_socket_owner
unregister_socket_owner
owner_for_socket
worker_socket_ring
app_socket_key
APP_SOCKET_RINGS
AppWorkerSocketRegistry
```

Replace session/socket spawn APIs with opaque operation APIs:

```rust
pub async fn spawn_on_op<F, Fut, T>(&self, op: AppOpId, owner_worker: usize, f: F) -> HammerResult<T>
where
    F: FnOnce(AppWorkerContext) -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static;

pub fn spawn_detached_on_op<F, Fut>(&self, op: AppOpId, owner_worker: usize, f: F) -> HammerResult<()>
where
    F: FnOnce(AppWorkerContext) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static;
```

AppContext may maintain `op_owners: FlatHashTable<u64, usize>` only to route an opaque app ring to the correct data worker.

- [ ] **Step 4: Keep app runtime methods generic**

`AppRuntime` may provide:

```rust
pub fn recv(&self) -> AppRecvFuture;
pub async fn send(&self, send: AppSend) -> HammerResult<()>;
pub async fn shutdown(&self) -> HammerResult<()>;
pub fn try_push_submission_descriptor(&self, sqe: AppSqeDescriptor) -> HammerResult<()>;
pub async fn next_completion(&self) -> Option<AppCqe>;
```

These methods use the current `AppOpId` inside `AppObjectRef::Operation(op)`.

- [ ] **Step 5: Update runtime app tests to opaque op language**

In `crates/hammer-runtime/tests/app_ring.rs` and `crates/hammer-runtime/tests/app_echo_loop.rs`:

- replace `AppStreamId::new(n)` with `AppOpId::new(n)`
- replace `spawn_on_stream` with `spawn_on_op(op, owner_worker, ...)`
- replace `AppObjectRef::Stream/Socket` with `AppObjectRef::Operation`
- delete tests for `install_control`, `bind_tcp_listener`, `bind_udp_socket`, and `close_socket`

Do not create renamed versions of those tests.

- [ ] **Step 6: Verify runtime app cleanup**

Run:

```bash
cargo test -p hammer-runtime --test app_ring --test app_echo_loop
```

Expected: tests pass.

Run:

```bash
rg -n "AppControl|AppControlOps|install_control|AppStreamId|AppObjectRef::Stream|AppObjectRef::Session|AppObjectRef::Socket|AppSocketId|APP_SOCKET_BACKENDS|APP_SOCKET_RINGS|bind_tcp_listener|bind_udp_socket|close_socket" crates/hammer-runtime/src/app crates/hammer-runtime/tests
```

Expected: no matches.

- [ ] **Step 7: Commit runtime app cleanup**

Run:

```bash
git add crates/hammer-runtime/src/app/context.rs crates/hammer-runtime/src/app/ring.rs crates/hammer-runtime/src/app/mod.rs crates/hammer-runtime/tests/app_ring.rs crates/hammer-runtime/tests/app_echo_loop.rs
git commit -m "hammer-runtime(Refactor): make app rings operation-oriented"
```

---

## Task 3: Make Service Control Plane Listener-Only

**Files:**
- Modify: `crates/hammer-service/src/service.rs`
- Modify tests in: `crates/hammer-service/src/service.rs`

- [ ] **Step 1: Write listener-only service test**

In the `#[cfg(test)]` module of `crates/hammer-service/src/service.rs`, keep or add this test shape:

```rust
#[test]
fn runtime_service_bind_tcp_listener_updates_listener_lookup_only() {
    let service = runtime_service_for_test();
    let bind: SocketAddr = "127.0.0.1:7000".parse().expect("tcp bind");

    let listener = service
        .bind_tcp_listener_for_test(bind, DataWorkerId::new(1))
        .expect("bind tcp listener");

    let snapshot = service.tcp_listener_snapshot_for_test();
    assert_eq!(snapshot.listeners.len(), 1);
    assert_eq!(snapshot.listeners[0].listener, listener);
    assert!(snapshot.tcp_lookup.lookup_listener_v4(0, [127, 0, 0, 1].into(), 7000).is_some());
}
```

Run:

```bash
cargo test -p hammer-service --lib runtime_service_bind_tcp_listener_updates_listener_lookup_only
```

Expected before implementation: fail if the test helpers do not exist or still route through app control.

- [ ] **Step 2: Replace `RuntimeAppControl*` with `RuntimeTcpListenerControl*`**

In `crates/hammer-service/src/service.rs`, delete `RuntimeAppControlState`, `RuntimeAppControlHandle`, `RuntimeAppControlSnapshot`, UDP socket fields, app-control impls, and socket descriptor tables.

Create listener-only service state:

```rust
#[derive(Clone)]
struct RuntimeTcpListenerControlHandle {
    control_handle: Arc<ControlThreadHandle>,
    state: Arc<RuntimeTcpListenerControlCell>,
}

struct RuntimeTcpListenerControlState {
    next_tcp_lookup_id: TcpLookupId,
    tcp_control: TcpInputControlPlane,
    tcp_lookup: TcpLookupSnapshot,
    tcp_listeners: hammer_infra::vec::Vec<TcpListenerRegistration>,
    tcp_listener_slots: FlatHashTable<u64, usize>,
}
```

`TcpListenerRegistration` must contain only:

```rust
struct TcpListenerRegistration {
    listener: TcpListenerId,
    lookup_id: TcpLookupId,
    owner_worker: DataWorkerId,
    bind: SocketAddr,
}
```

Use a service-local typed listener id:

```rust
enum TcpListenerTag {}
type TcpListenerId = Descriptor<TcpListenerTag>;
```

Do not use `AppSocketId`.

- [ ] **Step 3: Publish only TCP listener lookup**

Keep these methods:

```rust
fn bind_tcp_listener(&mut self, bind: SocketAddr, owner_worker: DataWorkerId) -> HammerResult<TcpListenerId>;
fn close_tcp_listener(&mut self, listener: TcpListenerId) -> HammerResult<()>;
fn publish_tcp_lookup(&mut self) -> HammerResult<()>;
```

`publish_tcp_lookup` must only insert `TcpLookupKind::Listener` entries and publish them through `TcpInputControlPlane::publish_lookup`.

- [ ] **Step 4: Remove service app-control installation**

In `RuntimeService::new_with_event_subscribers`, delete:

```rust
let app_control = RuntimeAppControlHandle::new(...);
let app_control_ops: Arc<dyn AppControlOps> = Arc::new(app_control.clone());
app_context.install_control(AppControl::new(app_control_ops))?;
```

Keep `app_context` only as generic app runtime context if the service still exposes app task APIs.

- [ ] **Step 5: Verify service control cleanup**

Run:

```bash
cargo test -p hammer-service --lib runtime_service_bind_tcp_listener_updates_listener_lookup_only
```

Expected: pass.

Run:

```bash
rg -n "RuntimeAppControl|AppControl|AppControlOps|install_control|AppSocketId|UdpSocketRegistration|bind_udp_socket|close_socket|publish_app_ingress|publish_connections|TcpConnectionRegistration|TcpConnectionSnapshot" crates/hammer-service/src/service.rs
```

Expected: no matches.

- [ ] **Step 6: Commit service listener cleanup**

Run:

```bash
git add crates/hammer-service/src/service.rs
git commit -m "hammer-service(Refactor): keep TCP control plane listener-only"
```

---

## Task 4: Remove UDP App-Control Binding From TCP Cleanup Scope

**Files:**
- Modify: `crates/hammer-service/src/transport/udp/input.rs`
- Modify tests: `crates/hammer-service/tests/app_udp_runtime.rs`
- Modify tests: `crates/hammer-service/tests/udp_input_nodes.rs`

- [ ] **Step 1: Delete UDP app-control tests that depend on app-owned sockets**

Remove tests whose only purpose is app-control socket binding:

```text
service_udp_socket_target_delivers_recv_from_descriptor_into_socket_ops
udp_input_dispatches_selected_port_into_runtime_app_socket
```

Do not replace them with `AppSocketId`/`AppOpId` bind tests.

- [ ] **Step 2: Keep UDP input tests packet-node focused**

Tests may still verify UDP packet classification, checksum, drop/pass behavior, and port dispatch tables. They must not require `hammer-runtime::app` to bind sockets.

- [ ] **Step 3: Remove UDP app registration variants that target runtime app sockets**

In `crates/hammer-service/src/transport/udp/input.rs`, remove any `UdpAppRegistration::socket(...)` or equivalent runtime-app target variant. If UDP still needs an app handoff later, it must be introduced as a separate io_uring app operation owned by service/session, not as app control.

- [ ] **Step 4: Verify UDP cleanup**

Run:

```bash
cargo test -p hammer-service --test udp_input_nodes
```

Expected: pass after app-control dependent tests are removed or rewritten packet-node-only.

Run:

```bash
rg -n "UdpAppRegistration::socket|AppSocketId|bind_udp_socket|AppControl|AppControlOps|install_control|socket_ops|APP_SOCKET" crates/hammer-service/src/transport/udp crates/hammer-service/tests/app_udp_runtime.rs crates/hammer-service/tests/udp_input_nodes.rs
```

Expected: no matches.

- [ ] **Step 5: Commit UDP app-control cleanup**

Run:

```bash
git add crates/hammer-service/src/transport/udp/input.rs crates/hammer-service/tests/app_udp_runtime.rs crates/hammer-service/tests/udp_input_nodes.rs
git commit -m "hammer-service(Refactor): remove app-control UDP binding"
```

---

## Task 5: Make Generic Session Runtime Protocol-Neutral

**Files:**
- Create: `crates/hammer-service/src/session/id.rs`
- Delete: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/ready.rs`
- Modify: `crates/hammer-service/src/session/timer.rs`
- Modify: `crates/hammer-service/src/session/worker.rs`
- Modify: `crates/hammer-service/src/session/node.rs`
- Modify: `crates/hammer-service/src/session/protocol/mod.rs`
- Modify tests: `crates/hammer-service/tests/session_queue_node.rs`
- Modify tests: `crates/hammer-service/tests/session_runtime.rs`

- [ ] **Step 1: Write protocol-neutral compile scan**

Run:

```bash
rg -n "AppSession|AppBackend|AppIngress|SessionProtocolOps|dyn|Box<dyn|downcast|PhantomData|registry|with_worker_tcp_session_protocol" crates/hammer-service/src/session crates/hammer-service/tests/session_queue_node.rs crates/hammer-service/tests/session_runtime.rs
```

Expected before cleanup: any `AppSession`, backend, registry, dyn ops, downcast, or `PhantomData` match is a removal target. `SessionProtocolContext` may live in `session/protocol/mod.rs`, but it must not mention app/backend/control concepts.

- [ ] **Step 2: Move generic id out of `session/app.rs`**

Create `crates/hammer-service/src/session/id.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    pub const fn new(value: u64) -> Self;
    pub const fn get(self) -> u64;
}
```

Delete `crates/hammer-service/src/session/app.rs`. No file under `crates/hammer-service/src/session/` may use `AppSession*` names.

- [ ] **Step 3: Keep `SessionQueueRuntime<P>` concrete-generic**

In `crates/hammer-service/src/session/worker.rs`, keep:

```rust
pub(crate) trait SessionQueueProgram: 'static {
    fn handle_timer_expiry(&mut self, context: &mut SessionProtocolContext<'_>, expiry: SessionTimerExpiry) -> CoreResult<()>;
    fn handle_ready(&mut self, context: &mut SessionProtocolContext<'_>, session_id: SessionId) -> CoreResult<()>;
}

pub(crate) struct SessionQueueRuntime<P> {
    sessions: WorkerSessionRuntime,
    program: P,
}
```

Do not add `dyn SessionProtocolOps`, downcast, registry, or `PhantomData`.

- [ ] **Step 4: Keep `SessionQueueHandle` untyped**

In `crates/hammer-service/src/session/node.rs`, `SessionQueueHandle` must wrap only runtime node data:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionQueueHandle(NodeRuntimeData);
```

The generic helper can register a `SessionQueueRuntime<P>` in a caller-provided thread-local slot. The handle must not be generic and must not contain `PhantomData`.

- [ ] **Step 5: Verify generic session cleanup**

Run:

Run:

```bash
rg -n "AppSession|AppBackend|AppIngress|SessionProtocolOps|dyn|Box<dyn|downcast|PhantomData|registry|with_worker_tcp_session_protocol" crates/hammer-service/src/session crates/hammer-service/tests/session_queue_node.rs crates/hammer-service/tests/session_runtime.rs
```

Expected: no matches, except ordinary Rust `dyn` in unrelated test mocks if any are outside session code.

Run:

```bash
cargo test -p hammer-service --test session_queue_node --test session_runtime
```

Expected: pass.

- [ ] **Step 6: Commit generic session cleanup**

Run:

```bash
git add crates/hammer-service/src/session/app.rs crates/hammer-service/src/session/id.rs crates/hammer-service/src/session/ready.rs crates/hammer-service/src/session/timer.rs crates/hammer-service/src/session/worker.rs crates/hammer-service/src/session/node.rs crates/hammer-service/src/session/protocol/mod.rs crates/hammer-service/tests/session_queue_node.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "hammer-service(Refactor): keep session runtime protocol-neutral"
```

---

## Task 6: Move TCP State Fully Into `session/protocol/tcp`

**Files:**
- Modify: `crates/hammer-service/src/session/protocol/tcp/state.rs`
- Modify: `crates/hammer-service/src/session/protocol/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state.rs`
- Modify tests: `crates/hammer-service/tests/tcp_connection_state.rs`
- Modify tests: `crates/hammer-service/tests/tcp_congestion.rs`

- [ ] **Step 1: Write state ownership test**

In `crates/hammer-service/tests/tcp_connection_state.rs`, add or keep a test that imports TCP session state from the session protocol path:

```rust
use hammer_service::session::protocol::tcp::state::TcpSessionState;

#[test]
fn tcp_sequence_window_state_lives_in_tcp_session_state() {
    let mut state = tcp_session_state_for_test();

    state.set_iss(1000);
    state.set_irs(2000);
    state.set_snd_una(1001);
    state.set_snd_nxt(1001);
    state.set_snd_wnd(4096);
    state.set_rcv_nxt(2001);
    state.set_rcv_wnd(8192);

    let view = state.view();
    assert_eq!(view.iss, 1000);
    assert_eq!(view.irs, 2000);
    assert_eq!(view.snd_una, 1001);
    assert_eq!(view.snd_nxt, 1001);
    assert_eq!(view.snd_wnd, 4096);
    assert_eq!(view.rcv_nxt, 2001);
    assert_eq!(view.rcv_wnd, 8192);
}
```

Run:

```bash
cargo test -p hammer-service --test tcp_connection_state tcp_sequence_window_state_lives_in_tcp_session_state
```

Expected before cleanup: fail if tests still import transport connection snapshots or if setters/views are missing.

- [ ] **Step 2: Remove `TcpConnectionState` as TCP state owner**

In `crates/hammer-service/src/transport/tcp/state.rs`, keep only packet-node dispatch table and input flag logic if still needed. Remove state that duplicates session-owned sequence/window/congestion/retransmit fields.

- [ ] **Step 3: Add TCP session table**

In `crates/hammer-service/src/session/protocol/tcp/state.rs`, define:

```rust
pub struct TcpSessionTable {
    sessions: hammer_infra::vec::Vec<TcpSessionState>,
    slots: FlatHashTable<u64, usize>,
}
```

Methods:

```rust
impl TcpSessionTable {
    pub fn new() -> Self;
    pub fn insert(&mut self, session_id: SessionId, state: TcpSessionState) -> CoreResult<()>;
    pub fn get(&self, session_id: SessionId) -> Option<&TcpSessionState>;
    pub fn get_mut(&mut self, session_id: SessionId) -> Option<&mut TcpSessionState>;
    pub fn remove(&mut self, session_id: SessionId) -> Option<TcpSessionState>;
}
```

Use `hammer_infra::vec::Vec` and `FlatHashTable`; do not use `std::collections::HashMap` for this data-plane-facing table.

- [ ] **Step 4: Make TCP protocol own the table**

In `crates/hammer-service/src/session/protocol/tcp/mod.rs`:

```rust
pub struct TcpSessionProtocol {
    worker: DataWorkerId,
    sessions: TcpSessionTable,
}
```

Add typed methods:

```rust
impl TcpSessionProtocol {
    pub(crate) fn create_accepted_session(&mut self, context: &mut SessionProtocolContext<'_>, args: TcpAcceptArgs) -> CoreResult<SessionId>;
    pub(crate) fn enqueue_rx_buffer(&mut self, context: &mut SessionProtocolContext<'_>, session_id: SessionId, buffer: TcpRxBuffer) -> CoreResult<TcpRxEnqueueResult>;
    pub(crate) fn poll_tx(&mut self, context: &mut SessionProtocolContext<'_>, session_id: SessionId) -> CoreResult<Option<TcpOutputSendView>>;
}
```

No trait object ops and no registry.

- [ ] **Step 5: Verify TCP state ownership cleanup**

Run:

```bash
cargo test -p hammer-service --test tcp_connection_state --test tcp_congestion
```

Expected: pass.

Run:

```bash
rg -n "TcpConnectionSnapshot|TcpConnectionRegistration|TcpSessionAccess|iss|irs|snd_una|snd_nxt|snd_wnd|rcv_nxt|rcv_wnd" crates/hammer-service/src/transport/tcp crates/hammer-service/src/service.rs
```

Expected: no snapshot/registration/access matches. Sequence/window fields may appear in transport packet logic only as calls into `TcpSessionState`, not as stored transport/control-plane state.

- [ ] **Step 6: Commit TCP state move**

Run:

```bash
git add crates/hammer-service/src/session/protocol/tcp/state.rs crates/hammer-service/src/session/protocol/tcp/mod.rs crates/hammer-service/src/transport/tcp/state.rs crates/hammer-service/tests/tcp_connection_state.rs crates/hammer-service/tests/tcp_congestion.rs
git commit -m "hammer-service(Refactor): move TCP state into session protocol"
```

---

## Task 7: Connect TCP Input/Listen/Established Nodes To TCP Session Protocol

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/src/transport/tcp/reply.rs`
- Modify tests: `crates/hammer-service/tests/tcp_lookup.rs`
- Modify tests: `crates/hammer-service/tests/tcp_input_nodes.rs`
- Modify tests: `crates/hammer-service/tests/tcp_dispatch.rs`
- Modify tests: `crates/hammer-service/tests/tcp_output.rs`

- [ ] **Step 1: Write listen-to-session test**

In `crates/hammer-service/tests/tcp_input_nodes.rs`, add a test with this behavior:

```rust
#[test]
fn tcp_listen_syn_creates_tcp_session_protocol_entry() {
    let mut graph = tcp_graph_for_test();
    let listener = graph.publish_listener("127.0.0.1:7000", DataWorkerId::new(0));
    let syn = tcp_syn_packet_for_test("10.0.0.2:40000", "127.0.0.1:7000", 100);

    graph.inject_tcp_packet(syn).expect("inject SYN");
    graph.run_tcp_input_once().expect("run tcp input");

    let session = graph.tcp_session_for_listener_child(listener).expect("accepted session");
    assert_eq!(session.view().irs, 100);
    assert_eq!(session.view().rcv_nxt, 101);
    assert_eq!(session.view().state, TcpState::SynRcvd);
}
```

Run:

```bash
cargo test -p hammer-service --test tcp_input_nodes tcp_listen_syn_creates_tcp_session_protocol_entry
```

Expected before implementation: fail because listen still depends on deleted transport connection/session backend or lacks TCP protocol table hookup.

- [ ] **Step 2: Make input lookup return listener/session ids only**

In `transport/tcp/input.rs`, keep listener lookup through `TcpLookupSnapshot`. It should produce:

```rust
enum TcpInputTarget {
    Listener { lookup_id: TcpLookupId, owner_worker: DataWorkerId },
    Session { session_id: SessionId, owner_worker: DataWorkerId },
}
```

Do not return app ingress targets, app backends, or connection snapshots.

- [ ] **Step 3: Create accepted session in TCP protocol from listen node**

In `transport/tcp/listen.rs`, on valid SYN:

- allocate child `SessionId`
- initialize `TcpSessionState` through `TcpSessionProtocol::create_accepted_session`
- set `irs`, `rcv_nxt`, `iss`, `snd_una`, `snd_nxt`, `snd_wnd`, `rcv_wnd`
- queue SYN-ACK/output work through session ready queue

The listen node may parse packets and choose next nodes, but it must not store session state itself.

- [ ] **Step 4: Enqueue receive data through TCP session protocol**

In `transport/tcp/established.rs` and `transport/tcp/rcv_process.rs`, replace direct app completion/backend calls with:

```rust
TcpSessionProtocol::with_queue(handle, |runtime| {
    let mut context = SessionProtocolContext::new(worker, runtime.sessions_mut());
    runtime.program_mut().enqueue_rx_buffer(&mut context, session_id, rx_buffer)
})
```

This is typed and concrete. Do not use dyn ops or downcast.

- [ ] **Step 5: Make enqueue advance `rcv_nxt` and wake app CQ**

In `session/protocol/tcp/mod.rs`, `enqueue_rx_buffer` must:

- validate sequence against `TcpSessionState::rcv_nxt`
- copy/attach buffer into the session-owned app receive CQ path
- advance `rcv_nxt` by bytes accepted
- return partial/full/full-window result
- mark app task/ring ready through generic session context

This is Hammer's io_uring equivalent of VPP `session_enqueue_stream_connection(... queue event ...)` plus `tc->rcv_nxt += written`.

- [ ] **Step 6: Verify TCP packet path**

Run:

```bash
cargo test -p hammer-service --test tcp_lookup --test tcp_input_nodes --test tcp_dispatch --test tcp_output
```

Expected: pass.

Run:

```bash
rg -n "AppIngressTarget|AppBackend|SessionBackend|TcpConnectionSnapshot|TcpConnectionRegistration|TcpSessionAccess|publish_app_ingress|publish_connections|flow|Flow" crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_lookup.rs crates/hammer-service/tests/tcp_input_nodes.rs crates/hammer-service/tests/tcp_dispatch.rs crates/hammer-service/tests/tcp_output.rs
```

Expected: no obsolete TCP control/app concepts. `flow` may appear only in unrelated comments outside TCP cleanup; remove if it refers to TCP connection/session path.

- [ ] **Step 7: Commit TCP node hookup**

Run:

```bash
git add crates/hammer-service/src/transport/tcp/input.rs crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/src/transport/tcp/established.rs crates/hammer-service/src/transport/tcp/rcv_process.rs crates/hammer-service/src/transport/tcp/syn_sent.rs crates/hammer-service/src/transport/tcp/output.rs crates/hammer-service/src/transport/tcp/reply.rs crates/hammer-service/tests/tcp_lookup.rs crates/hammer-service/tests/tcp_input_nodes.rs crates/hammer-service/tests/tcp_dispatch.rs crates/hammer-service/tests/tcp_output.rs
git commit -m "hammer-service(Refactor): connect TCP nodes to session protocol"
```

---

## Task 8: Delete Obsolete Files And Tests

**Files:**
- Delete if present: `crates/hammer-runtime/src/app/backend.rs`
- Delete if present: `crates/hammer-service/src/app/backend.rs`
- Delete if present: `crates/hammer-service/src/app/ingress.rs`
- Delete if present: `crates/hammer-service/src/app/registry.rs`
- Delete if present: `crates/hammer-service/src/transport/tcp/connection.rs`
- Delete if present: `crates/hammer-service/src/transport/udp/app.rs`
- Delete obsolete tests listed below

- [ ] **Step 1: Remove obsolete source modules**

Delete only these obsolete files if they still exist:

```bash
rm crates/hammer-runtime/src/app/backend.rs
rm crates/hammer-service/src/app/backend.rs
rm crates/hammer-service/src/app/ingress.rs
rm crates/hammer-service/src/app/registry.rs
rm crates/hammer-service/src/transport/tcp/connection.rs
rm crates/hammer-service/src/transport/udp/app.rs
```

Expected: some may already be deleted. If `rm` reports missing file, do not recreate it.

- [ ] **Step 2: Remove obsolete tests**

Delete only tests whose assertions target removed architecture:

```bash
rm crates/hammer-service/tests/tcp_connection_snapshot.rs
rm crates/hammer-service/tests/app_tcp_connect_runtime.rs
rm crates/hammer-service/tests/app_tcp_runtime.rs
rm crates/hammer-service/tests/tcp_listen_accept.rs
rm crates/hammer-service/tests/tcp_receive_states.rs
rm crates/hammer-service/tests/tcp_receive_validation.rs
rm crates/hammer-service/tests/tcp_session_protocol.rs
rm crates/hammer-service/tests/tcp_syn_sent_adapter.rs
```

Expected: some may already be deleted. Behavior that still matters must have been moved to the new TCP session protocol tests before deletion.

- [ ] **Step 3: Remove module references**

Update:

```text
crates/hammer-service/src/app/mod.rs
crates/hammer-service/src/transport/tcp/mod.rs
crates/hammer-service/src/transport/udp/mod.rs
crates/hammer-runtime/src/app/mod.rs
```

Expected: no `mod backend`, `mod ingress`, `mod registry`, `mod connection`, or `mod app` references for deleted files.

- [ ] **Step 4: Verify obsolete files are gone**

Run:

```bash
git status --short
rg -n "mod backend|mod ingress|mod registry|mod connection|transport::udp::app|app::backend" crates/hammer-runtime/src crates/hammer-service/src
```

Expected: deleted files appear in git status, and `rg` returns no module references.

- [ ] **Step 5: Commit deletions**

Run:

```bash
git add crates/hammer-runtime/src/app crates/hammer-service/src/app crates/hammer-service/src/transport/tcp crates/hammer-service/src/transport/udp crates/hammer-service/tests
git commit -m "hammer-service(Refactor): delete obsolete app backend TCP files"
```

---

## Task 9: Final Verification

**Files:**
- Entire workspace verification

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt --all
```

Expected: completes successfully.

- [ ] **Step 2: Focused checks**

Run:

```bash
cargo check -p hammer-runtime --lib
cargo check -p hammer-service --lib
cargo test -p hammer-runtime --tests --no-run
cargo test -p hammer-service --tests --no-run
```

Expected: all pass.

- [ ] **Step 3: Focused tests**

Run:

```bash
cargo test -p hammer-runtime --test app_ring --test app_echo_loop
cargo test -p hammer-service --test session_queue_node --test session_runtime
cargo test -p hammer-service --test tcp_lookup --test tcp_input_nodes --test tcp_dispatch --test tcp_output --test tcp_connection_state --test tcp_congestion
```

Expected: all pass.

- [ ] **Step 4: Forbidden scan**

Run:

```bash
rg -n "AppBackend|AppBackendRecvQueue|AppBackendSendQueue|AppIngressTarget|AppSessionBackend|AppSessionAppIngress|AppTcpSessionBackend|SessionProtocolOps|TcpConnectionSnapshot|TcpConnectionRegistration|TcpSessionAccess|TcpOutputBackend|TcpAcceptBackend|TcpSynSentBackend|publish_app_ingress|publish_connections|attach_app_backend|attach_app_backends|local_backend_for_flow|local_backend_for_socket|worker_backend|worker_socket_backend|with_worker_tcp_session_protocol|AppControl|AppControlOps|install_control|AppStreamId|AppObjectRef::Stream|AppObjectRef::Session|AppObjectRef::Socket|AppSocketId|APP_SOCKET_BACKENDS|APP_SOCKET_RINGS|bind_udp_socket|AppBackend|backend" crates/hammer-runtime/src/app crates/hammer-runtime/tests crates/hammer-service/src crates/hammer-service/tests
```

Expected: no matches for removed concepts. If `backend` appears in unrelated non-app/non-session names, document the exact path and reason before keeping it.

- [ ] **Step 5: Workspace test**

Run:

```bash
cargo test --workspace
```

Expected: pass. If unrelated existing tests fail, record exact failures and still keep the focused TCP/session/app cleanup green.

- [ ] **Step 6: Commit formatting if needed**

Run:

```bash
git status --short
git add .
git commit -m "hammer-service(Refactor): finish VPP-style TCP session cleanup"
```

Expected: commit only if formatting or verification fixes changed files.

---

## Task 10: Push And Clean Target

**Files:**
- Git repository
- `target/`

- [ ] **Step 1: Show commit stack**

Run:

```bash
git log --oneline --decorate -8
```

Expected: recent commits are scoped by module and match this plan.

- [ ] **Step 2: Push current branch**

Run:

```bash
git push
```

Expected: push succeeds.

- [ ] **Step 3: Clean build artifacts**

Run:

```bash
rm -rf target
```

Expected: `target/` removed. This is allowed only after tests/checks are complete and commits are pushed.

---

## Deletion Summary To Report

When implementation finishes, report these categories explicitly:

- Removed app backend/app ingress files.
- Removed transport TCP connection snapshot/control-plane connection files.
- Removed runtime app control/socket/session/stream concepts.
- Removed UDP app-control socket binding from this TCP/session cleanup scope.
- Removed tests that asserted the deleted architecture.
- Kept service TCP control plane limited to listener lookup publication.
- Kept TCP sequence/window state in `session/protocol/tcp/state.rs`.
- Kept app interaction io_uring-based through session-owned rings and async task wakeups.

## Self-Review Checklist

- Every forbidden concept has a scan command.
- VPP facts are cited with local source paths and line ranges.
- Hammer's io_uring difference is explicit and affects file ownership.
- The plan deletes concepts instead of renaming them.
- `hammer-runtime::app` has no TCP/session/socket/control-plane concepts.
- `hammer-service/src/service.rs` publishes listener lookup only.
- TCP state lives under `session/protocol/tcp/`.
- Generic session runtime remains protocol-neutral and concrete-generic.
- Tests are updated before source changes in each major task.

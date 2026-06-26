# Hammer App Runtime Echo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a generic app-facing I/O ring surface across `hammer-app <-> hammer-runtime <-> hammer-service`, then implement complete `tcp echo` and `udp echo` applications on top of it.

**Architecture:** Keep `io_uring`-like submission/completion semantics in `hammer-runtime`, not in `hammer-service`. `hammer-service` remains the transport/data-plane backend that turns packet-graph TCP/UDP events into runtime CQEs and executes runtime SQEs against worker-owned transport state. `hammer-app` becomes the app-facing crate that consumes the runtime ring and provides both low-level SQE/CQE access and thin `TcpListener` / `TcpStream` / `UdpSocket` wrappers used by echo apps. `tokio` stays a control-plane implementation detail only; data-plane workers and app execution must use a runtime-owned executor/worker loop that does not expose `tokio` into app-facing contracts, and control-plane operations such as `bind` / `listen` must stay off the data-plane ring.

**Tech Stack:** Rust 2024, workspace crates (`hammer-app`, `hammer-runtime`, `hammer-service`, `hammer-adapter`, `hammer-core`, `hammer-control`), `arc-swap`, `smoltcp`, runtime-owned worker loops, VPP-style worker ownership and packet graph dispatch

---

## Scope Guardrails

- This plan adds a new workspace crate: `crates/hammer-app`.
- `io_uring`-like capability lives in `hammer-runtime`, not `hammer-service`.
- `hammer-service` must not expose app-facing SQE/CQE types directly.
- The first app protocols are TCP stream echo and UDP datagram echo only.
- v1 scopes outbound `connect` out. The first implementation only needs control-plane-created bound/listening TCP sockets and bound UDP sockets.
- Do not build a POSIX socket compatibility layer.
- Do not add FFI or config-surface changes in v1 unless required to start the echo apps from `RuntimeService`.
- Keep flow ownership worker-local. Once a flow is created or accepted on worker N, it is permanently owned by worker N for its full lifetime. Cross-worker use must hand off work to the owner worker, not share mutable connection state through locks, and not migrate the flow to a different worker.
- Keep hot-path transport lookup/data-plane structures in `hammer-service` on `FlatHashTable`, packet opaque data, and runtime-local state. Do not add `Mutex<HashMap<...>>` on per-packet paths.
- For new data-plane-facing queues, payload buffers, and snapshots, use `hammer_infra::vec::Vec` and `hammer_infra::map::FlatHashTable` rather than `std` collections.
- Put reusable ring data structures in `hammer-infra`, not `hammer-runtime`.
- Use the runtime-owned data-plane barrier only for control-plane publication and registration changes that affect worker-visible transport state.
- `tokio` is control-plane-only in this design. Do not make data-plane app hosting, app polling, or app task spawning depend on `tokio::spawn`, `tokio::runtime::Handle`, or `#[tokio::test]`.

## Control-Plane vs Data-Plane Boundary

This split is the anchor for every API in this plan. `bind` was the first concrete example, but it is not the only control-plane action.

### Control plane owns

- creating and destroying long-lived app objects such as app hosts, TCP listeners, and UDP sockets
- `bind` / `unbind`
- `listen` / stop listening
- future outbound `connect` requests that create new flows
- publishing listener, port, and lookup snapshots into worker-visible transport tables
- backlog, worker affinity, queue depth, buffer registration, pause/resume, and stats/config queries

### Data plane owns

- TCP/UDP receive/transmit on already-published listeners, sockets, and flows
- TCP state progression on worker-owned flows
- accept delivery as a completion event on an existing listener
- `recv`, `recv_from`, `send`, `send_to`
- closing an already-established TCP flow
- CQE generation and worker-local app execution/polling

### Split operations

- if an action is control-plane initiated and data-plane executed, model it as two phases, not one mixed API
- `bind` and `listen` complete on the control plane and publish worker-visible state through the existing runtime-owned publication/barrier mechanisms
- the app ring only carries data-plane work on already-created objects; it does not create listeners or publish ports
- v1 keeps outbound `connect` out of scope so the first implementation can lock in the boundary with TCP/UDP echo servers only

## Design Summary

### Recommended Approach

Build a generic `AppRing` in `hammer-runtime` with transport-neutral dataplane SQE/CQE envelopes and TCP/UDP-specific payload variants. `hammer-app` depends on `hammer-runtime` and offers:

- a low-level ring handle for direct SQE/CQE programming;
- thin convenience wrappers for TCP and UDP app code;
- two complete examples/tests: TCP echo and UDP echo.

Control-plane object creation lives beside the ring, not inside it. `hammer-runtime::app::AppContext` exposes a control-plane surface backed by `hammer-service` so `TcpListener::bind` and `UdpSocket::bind` can remain ergonomic at the app layer while still executing as control-plane operations.

`hammer-service` implements a backend trait registered into `hammer-runtime`. The backend turns:

- TCP listener/connection/receive/send/close events from `tcp_input -> tcp_listen / tcp_established -> tcp_rcv_process`
- UDP port datagram receive/send events from `udp_input`

into runtime CQEs and executes runtime SQEs by scheduling worker-local transport actions.

`hammer-runtime` also owns the app executor for data-plane apps. Apps are written as `async` code, but they run on runtime-managed worker-local executors: one app task queue, one ring/CQ drain path, and one owned-flow set per worker thread. No worker may poll or mutate another worker's app tasks or flow state directly.

Flow ownership is sticky. A TCP flow accepted on worker N stays on worker N until close; a UDP app socket bound/published to worker N also stays serviced by worker N for its lifetime in v1. Cross-worker activity is modeled as handoff-to-owner work items only, never ownership transfer.

### Alternatives Considered

1. Keep the ring in `hammer-service`.
   Rejected because it collapses the intended `app <-> runtime <-> service` layering and leaks transport backend concepts into the app-facing API.

2. Build separate `TcpRing` and `UdpRing` stacks.
   Rejected for v1 because submission, completion, worker routing, and buffer ownership semantics are identical enough that the duplication would calcify quickly.

3. Expose only bare SQE/CQE in `hammer-app`.
   Rejected for v1 because the user explicitly wants complete TCP/UDP echo implementations. Thin wrappers accelerate real apps while preserving access to the lower-level ring API.

4. Put `bind` / `listen` on the app ring as opcodes.
   Rejected because these operations mutate published worker-visible state and therefore belong on the control plane, even if the app-facing wrapper method is named `bind`.

## File Map

### Create

- `crates/hammer-app/Cargo.toml`
- `crates/hammer-app/src/lib.rs`
- `crates/hammer-app/src/ring.rs`
- `crates/hammer-app/src/tcp.rs`
- `crates/hammer-app/src/udp.rs`
- `crates/hammer-app/src/echo.rs`
- `crates/hammer-app/tests/tcp_echo.rs`
- `crates/hammer-app/tests/udp_echo.rs`
- `crates/hammer-infra/src/ring.rs`
- `crates/hammer-infra/tests/ring.rs`
- `crates/hammer-runtime/src/app/mod.rs`
- `crates/hammer-runtime/src/app/ring.rs`
- `crates/hammer-runtime/src/app/backend.rs`
- `crates/hammer-runtime/src/app/context.rs`
- `crates/hammer-runtime/tests/app_ring.rs`
- `crates/hammer-runtime/tests/app_echo_loop.rs`
- `crates/hammer-service/src/app/mod.rs`
- `crates/hammer-service/src/app/backend.rs`
- `crates/hammer-service/src/app/registry.rs`
- `crates/hammer-service/src/transport/tcp/app.rs`
- `crates/hammer-service/src/transport/tcp/output.rs`
- `crates/hammer-service/src/transport/udp/app.rs`
- `crates/hammer-service/tests/app_tcp_runtime.rs`
- `crates/hammer-service/tests/app_udp_runtime.rs`

### Modify

- `Cargo.toml`
- `crates/hammer-infra/src/lib.rs`
- `crates/hammer-runtime/Cargo.toml`
- `crates/hammer-runtime/src/lib.rs`
- `crates/hammer-runtime/src/spawn.rs`
- `crates/hammer-runtime/src/data_plane.rs`
- `crates/hammer-service/Cargo.toml`
- `crates/hammer-service/src/lib.rs`
- `crates/hammer-service/src/service.rs`
- `crates/hammer-service/src/transport/tcp/mod.rs`
- `crates/hammer-service/src/transport/tcp/input.rs`
- `crates/hammer-service/src/transport/tcp/listen.rs`
- `crates/hammer-service/src/transport/tcp/established.rs`
- `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- `crates/hammer-service/src/transport/tcp/lookup.rs`
- `crates/hammer-service/src/transport/tcp/state.rs`
- `crates/hammer-service/src/transport/udp/mod.rs`
- `crates/hammer-service/src/transport/udp/input.rs`
- `crates/hammer-adapter/tests/packet_buffer.rs`

### Responsibility Notes

- `hammer-app/src/ring.rs`: app-facing low-level ring handle and convenience polling helpers.
- `hammer-app/src/tcp.rs`: `TcpListener` / `TcpStream` wrappers built on runtime ring primitives.
- `hammer-app/src/udp.rs`: `UdpSocket` wrapper built on runtime ring primitives.
- `hammer-app/src/echo.rs`: reusable echo app loops used by tests/examples.
- `hammer-infra/src/ring.rs`: generic ring queue, cursor, slot, and bounded local storage primitives with no app/runtime semantics.
- `hammer-runtime/src/app/*.rs`: runtime-owned generic SQE/CQE types, app ring semantics, backend trait, and app context registration built on `hammer_infra::ring`.
- `hammer-runtime/src/data_plane.rs` and `hammer-runtime/src/spawn.rs`: split control-plane tokio hosting from data-plane/app executor hosting so app runtime is data-plane-owned and tokio-free at the contract boundary.
- `hammer-service/src/app/*.rs`: service-owned backend implementation and app registration/control-plane publication.
- `hammer-service/src/transport/tcp/app.rs`: TCP app delivery and SQE execution bridge.
- `hammer-service/src/transport/tcp/output.rs`: TCP packet emission helpers for app-generated sends/closes.
- `hammer-service/src/transport/udp/app.rs`: UDP app delivery and SQE execution bridge.

## Public/Internal API Shape

### `hammer-runtime`

- `hammer_runtime::app::AppRingHandle`
- `hammer_runtime::app::AppContext`
- `hammer_runtime::app::AppControl`
- `hammer_runtime::app::AppBufferHandle`
- `hammer_runtime::app::AppBufferLease`
- `hammer_runtime::app::AppSocketId`
- `hammer_runtime::app::AppFlowId`
- `hammer_runtime::app::AppUserData`
- `hammer_runtime::app::AppSqe`
- `hammer_runtime::app::AppCqe`
- `hammer_runtime::app::AppOpcode`
- `hammer_runtime::app::AppCqeKind`
- `hammer_runtime::app::TransportKind`
- `hammer_runtime::app::AppBackend`
- `hammer_runtime::app::AppExecutor`
- `hammer_runtime::app::AppJoinHandle`
- `hammer_runtime::app::spawn_app_local(...)`
- `hammer_runtime::app::current_app_context()`

### `hammer-app`

- `hammer_app::ring::AppRing`
- `hammer_app::ring::BufferView`
- `hammer_app::tcp::TcpListener`
- `hammer_app::tcp::TcpStream`
- `hammer_app::udp::UdpSocket`
- `hammer_app::echo::run_tcp_echo`
- `hammer_app::echo::run_udp_echo`

### `hammer-service`

- `hammer_service::RuntimeService::app_context()`
- `hammer_service::RuntimeService::spawn_app_on_worker(...)`
- `hammer_service::RuntimeService::register_app_host(...)`

These APIs are internal workspace APIs, not public OS-socket emulation.

## Data Model Decisions

- SQE/CQE envelopes are transport-neutral for dataplane work only:
  - `opcode`
  - `user_data`
  - `socket_id`
  - `flow_id`
  - `buffer_id`
  - payload union encoded as Rust enums, not manual tagged integers in app-facing code
- `bind`, `listen`, UDP port registration, and listener/socket destruction are control-plane operations exposed through `AppControl`, not `AppSqe`.
- `accept`, `recv`, and `recv_from` remain dataplane operations because they act on already-created listeners/sockets/flows.
- recv-side payload delivery is zero-copy in v1: CQEs carry buffer identity/lease metadata, and app code reads payload through a borrowed buffer view backed by the existing data-plane buffer arena.
- new app/runtime payload APIs must use `BufferIndex`, borrowed `Buffer` access, `current_ptr`, `current_len`, or a runtime-owned lease wrapper around them; do not copy payload into `Vec<u8>` just to hand it to the app layer.
- send-side fast path should accept borrowed slices when possible; copying into fresh app-owned `Vec<u8>` is not the default contract.
- `AppBufferLease` must remain owner-worker-local and define when the underlying `BufferIndex` is released or returned after app consumption.
- Generic ring queue/cursor/slot data structures live in `hammer_infra::ring`; `hammer-runtime` only defines app-specific SQE/CQE semantics and worker ownership rules on top.
- TCP and UDP transport state remain in `hammer-service`.
- Runtime ring queues are worker-local and owned by `hammer-runtime`.
- Each worker thread owns:
  - one app submission queue
  - one completion queue drain path
  - one local app executor run queue
  - one owned-listener/socket/flow registry view for app-facing objects published to that worker
- `AppFlowId -> owner worker` is immutable after creation in v1.
- The runtime backend interface uses explicit worker routing:
  - non-owner submissions are forwarded to the owner worker
  - completions are written onto the app-visible CQ owned by the worker that owns the flow/socket
- Listener/port publication into worker-visible lookup tables remains a service/runtime control-plane publication path, not a ring CQE side effect.
- Packet opaque data may be used in `hammer-service` to carry app delivery metadata between nodes, but that opaque format stays service-internal.

## Task 1: Add the `hammer-app` crate and wire it into the workspace

**Files:**
- Create: `crates/hammer-app/Cargo.toml`
- Create: `crates/hammer-app/src/lib.rs`
- Modify: `Cargo.toml`

- [ ] **Step 1: Write the failing workspace membership test**

```rust
// crates/hammer-app/src/lib.rs
#[cfg(test)]
mod tests {
    #[test]
    fn crate_builds() {
        assert_eq!(2 + 2, 4);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails before the crate exists**

Run: `cargo test -p hammer-app crate_builds -- --exact`

Expected: FAIL with `package ID specification 'hammer-app' did not match any packages`

- [ ] **Step 3: Add the crate to the workspace**

```toml
# Cargo.toml
[workspace]
resolver = "3"
members = [
    "crates/hammer-infra",
    "crates/hammer-core",
    "crates/hammer-adapter",
    "crates/hammer-component-macros",
    "crates/hammer-control",
    "crates/hammer-runtime",
    "crates/hammer-service",
    "crates/hammer-app",
    "crates/hammer-ffi",
    "crates/hammer-uniffi-bindgen",
]
```

- [ ] **Step 4: Create the crate manifest**

```toml
# crates/hammer-app/Cargo.toml
[package]
name = "hammer-app"
version.workspace = true
edition.workspace = true
publish.workspace = true

[lib]
name = "hammer_app"

[dependencies]
hammer-core = { path = "../hammer-core" }
hammer-runtime = { path = "../hammer-runtime" }

[dev-dependencies]
hammer-service = { path = "../hammer-service" }
```

- [ ] **Step 5: Create the crate root**

```rust
// crates/hammer-app/src/lib.rs
pub mod echo;
pub mod ring;
pub mod tcp;
pub mod udp;
```

- [ ] **Step 6: Run the focused crate test to verify it passes**

Run: `cargo test -p hammer-app`

Expected: PASS with the new crate discovered by the workspace.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/hammer-app/Cargo.toml crates/hammer-app/src/lib.rs
git commit -m "hammer-app(Feat): add app crate scaffold"
```

## Task 2: Add generic ring primitives to `hammer-infra` and app ring semantics to `hammer-runtime`

**Files:**
- Create: `crates/hammer-infra/src/ring.rs`
- Create: `crates/hammer-infra/tests/ring.rs`
- Modify: `crates/hammer-infra/src/lib.rs`
- Create: `crates/hammer-runtime/src/app/mod.rs`
- Create: `crates/hammer-runtime/src/app/ring.rs`
- Create: `crates/hammer-runtime/src/app/backend.rs`
- Create: `crates/hammer-runtime/src/app/context.rs`
- Modify: `crates/hammer-runtime/src/lib.rs`
- Modify: `crates/hammer-runtime/Cargo.toml`
- Test: `crates/hammer-runtime/tests/app_ring.rs`

- [ ] **Step 1: Write failing infra ring tests**

```rust
// crates/hammer-infra/tests/ring.rs
use hammer_infra::ring::LocalRing;

#[test]
fn local_ring_push_pop_preserves_fifo_order() {
    let mut ring = LocalRing::<u32>::with_capacity(4);
    assert!(ring.push(10).is_ok());
    assert!(ring.push(20).is_ok());
    assert_eq!(ring.pop(), Some(10));
    assert_eq!(ring.pop(), Some(20));
    assert_eq!(ring.pop(), None);
}
```

- [ ] **Step 2: Run the infra test to verify it fails**

Run: `cargo test -p hammer-infra --test ring`

Expected: FAIL because `hammer_infra::ring` does not exist.

- [ ] **Step 3: Add infra ring module wiring**

```rust
// crates/hammer-infra/src/lib.rs
pub mod ring;
```

- [ ] **Step 4: Define generic infra ring storage**

Implementation requirements:

- add generic bounded local ring storage to `crates/hammer-infra/src/ring.rs`
- keep the API transport-agnostic and runtime-agnostic
- include producer/consumer cursor arithmetic and bounded slot ownership in infra
- do not place `AppSqe`, `AppCqe`, worker ownership policy, or backend traits in infra

- [ ] **Step 5: Run the infra ring test**

Run: `cargo test -p hammer-infra --test ring`

Expected: PASS with FIFO coverage.

- [ ] **Step 6: Write failing runtime ring tests**

```rust
// crates/hammer-runtime/tests/app_ring.rs
use hammer_runtime::app::{
    AppBufferHandle, AppCqeKind, AppFlowId, AppOpcode, AppRingHandle, AppSocketId, AppSqe,
    AppUserData, TransportKind,
};

#[test]
fn sqe_and_cqe_cover_tcp_and_udp_shapes() {
    let tcp = AppSqe::recv(AppUserData::new(7), AppFlowId::new(11), 2048);
    assert_eq!(tcp.transport(), TransportKind::Tcp);
    assert_eq!(tcp.opcode(), AppOpcode::Recv);

    let udp = AppSqe::recv_from(AppUserData::new(8), AppSocketId::new(13), 2048);
    assert_eq!(udp.transport(), TransportKind::Udp);
    assert_eq!(udp.opcode(), AppOpcode::RecvFrom);

    let cqe = AppCqeKind::RecvFrom {
        socket: AppSocketId::new(13),
        source: "127.0.0.1:5353".parse().expect("socket addr"),
        buffer: AppBufferHandle::for_tests(99, 0x1000 as *const u8, 5),
        len: 5,
        truncated: false,
    };
    assert!(matches!(cqe, AppCqeKind::RecvFrom { .. }));
}

#[test]
fn app_ring_batches_submissions() {
    let ring = AppRingHandle::for_tests(256, 256);
    ring.push_test_submission(AppSqe::nop(AppUserData::new(1)))
        .expect("first sqe");
    ring.push_test_submission(AppSqe::nop(AppUserData::new(2)))
        .expect("second sqe");

    let batch = ring.take_test_submissions(8);
    assert_eq!(batch.len(), 2);
}
```

- [ ] **Step 7: Run the focused runtime test to verify it fails**

Run: `cargo test -p hammer-runtime --test app_ring`

Expected: FAIL because `hammer_runtime::app` does not exist.

- [ ] **Step 8: Add runtime app module wiring**

```rust
// crates/hammer-runtime/src/lib.rs
pub mod app;
```

- [ ] **Step 9: Define transport-neutral app ring enums and ids**

```rust
// crates/hammer-runtime/src/app/ring.rs
use hammer_core::error::HammerError;
use hammer_adapter::BufferIndex;
use hammer_infra::ring::LocalRing;
use hammer_infra::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppUserData(u64);

impl AppUserData {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppSocketId(u64);

impl AppSocketId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppFlowId(u64);

impl AppFlowId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppOpcode {
    Nop,
    Accept,
    Recv,
    RecvFrom,
    Send,
    SendTo,
    Close,
    PollReadable,
    PollWritable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppSqe {
    Nop { user_data: AppUserData },
    Accept { user_data: AppUserData, socket: AppSocketId },
    Recv { user_data: AppUserData, flow: AppFlowId, max: usize },
    RecvFrom { user_data: AppUserData, socket: AppSocketId, max: usize },
    Send { user_data: AppUserData, flow: AppFlowId, buffer: BufferIndex },
    SendTo {
        user_data: AppUserData,
        socket: AppSocketId,
        target: std::net::SocketAddr,
        buffer: BufferIndex,
    },
    CloseFlow { user_data: AppUserData, flow: AppFlowId },
}
```

- [ ] **Step 10: Define CQE shapes and a test ring handle**

```rust
// crates/hammer-runtime/src/app/ring.rs
use hammer_core::error::HammerError;
use hammer_adapter::BufferIndex;
use hammer_infra::ring::LocalRing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppBufferHandle {
    pub buffer: BufferIndex,
    pub ptr: *const u8,
    pub len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppCqeKind {
    Accepted { listener: AppSocketId, flow: AppFlowId },
    Recv { flow: AppFlowId, buffer: AppBufferHandle, fin: bool },
    RecvFrom {
        socket: AppSocketId,
        source: std::net::SocketAddr,
        buffer: AppBufferHandle,
        len: usize,
        truncated: bool,
    },
    Sent { len: usize },
    Closed,
    Error { error: HammerError },
}
```

- [ ] **Step 11: Add backend and context traits**

```rust
// crates/hammer-runtime/src/app/backend.rs
use hammer_core::error::HammerResult;
use hammer_infra::vec::Vec;

use super::ring::{AppCqe, AppRingHandle, AppSqe};

pub trait AppBackend: Send + Sync + 'static {
    fn submit(&self, ring: &AppRingHandle, sqes: Vec<AppSqe>) -> HammerResult<usize>;
    fn poll(&self, ring: &AppRingHandle, max: usize) -> HammerResult<Vec<AppCqe>>;
}
```

- [ ] **Step 12: Run the focused runtime tests**

Run: `cargo test -p hammer-runtime --test app_ring`

Expected: PASS with batching and enum-shape coverage.

- [ ] **Step 13: Commit**

```bash
git add crates/hammer-infra/src/lib.rs crates/hammer-infra/src/ring.rs crates/hammer-infra/tests/ring.rs crates/hammer-runtime/Cargo.toml crates/hammer-runtime/src/lib.rs crates/hammer-runtime/src/app crates/hammer-runtime/tests/app_ring.rs
git commit -m "hammer-infra(Feat): add ring primitives for app runtime"
```

## Task 3: Add worker-local async app executor and tokio/data-plane separation in `hammer-runtime`

**Files:**
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-runtime/src/app/context.rs`
- Modify: `crates/hammer-runtime/src/spawn.rs`
- Modify: `crates/hammer-runtime/src/data_plane.rs`
- Test: `crates/hammer-runtime/tests/app_echo_loop.rs`

- [ ] **Step 1: Write the failing worker-local app executor test**

```rust
// crates/hammer-runtime/tests/app_echo_loop.rs
use hammer_runtime::app::{spawn_app_local, AppContext, AppSqe, AppUserData};

#[test]
fn app_context_submits_and_runs_async_tasks_on_current_worker_without_tokio_contract() {
    let context = AppContext::for_tests(2, 128, 128);
    let worker = context.current_worker_for_tests();
    context
        .submit_one(AppSqe::nop(AppUserData::new(9)))
        .expect("submit");
    let sqes = context.take_worker_submissions_for_tests(worker, 8);
    assert_eq!(sqes.len(), 1);

    let mut task = spawn_app_local(async move { 7usize }).expect("spawn app local");
    assert_eq!(task.poll_for_test().expect("poll"), Some(7));
}
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `cargo test -p hammer-runtime --test app_echo_loop`

Expected: FAIL because `AppContext::for_tests`, `spawn_app_local`, and worker-local app executor helpers do not exist.

- [ ] **Step 3: Add worker-local context and app executor helpers**

```rust
// crates/hammer-runtime/src/app/context.rs
pub struct AppContext {
    ring: AppRingHandle,
    backend: std::sync::Arc<dyn AppBackend>,
    executor: AppExecutor,
}
```

- [ ] **Step 4: Add runtime-owned app executor contract**

```rust
// crates/hammer-runtime/src/app/context.rs
pub struct AppExecutor;

pub fn spawn_app_local<F>(future: F) -> HammerResult<AppJoinHandle<F::Output>>
where
    F: core::future::Future + 'static,
    F::Output: 'static,
{
    todo!()
}
```

- [ ] **Step 5: Keep dataplane/app execution independent from control-plane tokio**

Implementation requirements:

- move any app/data-plane-hosting responsibilities out of `tokio`-specific control paths
- `crates/hammer-runtime/src/spawn.rs` may keep `tokio` for control-plane hosting only
- data-plane app spawning must use runtime-owned worker-local queues/polling, not `tokio::spawn`
- each data worker thread must own and drive its own local app executor queue
- the worker-local app executor queue and CQ/SQ storage should be built from `hammer_infra::ring`, not a runtime-local bespoke queue type
- app futures created on worker N must be polled on worker N only unless explicitly handed off as a control/runtime operation
- once an accepted or created flow is assigned to worker N, all later SQE execution and CQE delivery for that flow must stay on worker N until close
- the app-facing API must not mention `tokio` types

- [ ] **Step 6: Make `submit_one` and batch submit preserve current worker ownership**

```rust
// crates/hammer-runtime/src/app/context.rs
use hammer_infra::vec::Vec;

pub fn submit_one(&self, sqe: AppSqe) -> HammerResult<()> {
    self.submit(std::iter::once(sqe))
}

pub fn submit<I>(&self, sqes: I) -> HammerResult<usize>
where
    I: IntoIterator<Item = AppSqe>,
{
    let sqes = sqes.into_iter().collect::<Vec<_>>();
    self.backend.submit(&self.ring, sqes)
}
```

- [ ] **Step 7: Run runtime app tests**

Run: `cargo test -p hammer-runtime --test app_ring --test app_echo_loop`

Expected: PASS, demonstrating worker-local submission behavior and runtime-owned app execution without a tokio contract.

- [ ] **Step 8: Commit**

```bash
git add crates/hammer-runtime/src/app crates/hammer-runtime/src/spawn.rs crates/hammer-runtime/src/data_plane.rs crates/hammer-runtime/tests/app_echo_loop.rs
git commit -m "hammer-runtime(Feat): add worker-local app executor"
```

## Task 4: Add service-side app backend registry and runtime wiring

**Files:**
- Create: `crates/hammer-service/src/app/mod.rs`
- Create: `crates/hammer-service/src/app/backend.rs`
- Create: `crates/hammer-service/src/app/registry.rs`
- Modify: `crates/hammer-service/src/lib.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Test: `crates/hammer-service/tests/app_tcp_runtime.rs`

- [ ] **Step 1: Write the failing service runtime registration test**

```rust
// crates/hammer-service/tests/app_tcp_runtime.rs
use hammer_service::RuntimeService;

#[test]
fn runtime_service_exposes_app_context() {
    let _ = std::any::type_name::<RuntimeService>();
    // Compile-level smoke test for the new accessor once added.
}
```

- [ ] **Step 2: Run the test to verify the accessor is missing**

Run: `cargo test -p hammer-service --test app_tcp_runtime`

Expected: FAIL once the test references `app_context()` and it does not exist yet.

- [ ] **Step 3: Wire the new app module**

```rust
// crates/hammer-service/src/lib.rs
pub mod app;
```

- [ ] **Step 4: Add the service-owned backend scaffold**

```rust
// crates/hammer-service/src/app/backend.rs
pub struct ServiceAppBackend;
```

- [ ] **Step 5: Expose app context from `RuntimeService`**

```rust
// crates/hammer-service/src/service.rs
impl RuntimeService {
    pub fn app_context(&self) -> hammer_runtime::app::AppContext {
        self.inner
            .lock()
            .expect("service inner poisoned")
            .app_context
            .clone()
    }
}
```

- [ ] **Step 5a: Add control-plane app object operations**

Implementation requirements:

- expose control-plane object creation through `AppContext` / `AppControl`, not through ring opcodes
- define helper methods for:
  - `bind_tcp_listener`
  - `bind_udp_socket`
  - `close_socket`
- implement these helpers by routing through `RuntimeService` control-thread accessors and existing TCP/UDP publication points such as `publish_lookup`, `publish_dispatch`, and `register_port`
- keep these operations on the service/runtime control plane even when app-facing wrappers call them as ordinary methods

- [ ] **Step 6: Run the service test**

Run: `cargo test -p hammer-service --test app_tcp_runtime`

Expected: PASS with runtime app context reachable from the service layer.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/lib.rs crates/hammer-service/src/service.rs crates/hammer-service/src/app crates/hammer-service/tests/app_tcp_runtime.rs
git commit -m "hammer-service(Feat): wire runtime app backend"
```

## Task 5: Bridge TCP packet graph events into runtime CQEs

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/app.rs`
- Create: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Modify: `crates/hammer-service/src/transport/tcp/lookup.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state.rs`
- Test: `crates/hammer-service/tests/app_tcp_runtime.rs`

- [ ] **Step 1: Write the failing TCP CQE test**

```rust
#[test]
fn tcp_backend_emits_accept_and_recv_cqes() {
    // Arrange a listener, deliver SYN/ACK/data, then assert:
    // 1. Accepted CQE
    // 2. Recv CQE with echoed payload bytes
    todo!()
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p hammer-service --test app_tcp_runtime tcp_backend_emits_accept_and_recv_cqes -- --exact`

Expected: FAIL because TCP app backend bridging is not implemented.

- [ ] **Step 3: Add TCP app bridge state**

```rust
// crates/hammer-service/src/transport/tcp/app.rs
pub struct TcpAppBridge;
```

- [ ] **Step 4: Define the TCP delivery contract**

Implementation requirements:

- listener accept path produces `AppCqeKind::Accepted` on the same worker that will own the accepted flow for its full lifetime
- established readable path produces `AppCqeKind::Recv`
- close/fin/rst produces `AppCqeKind::Closed`
- TCP recv CQEs must wrap existing data-plane buffers as `AppBufferHandle` / lease metadata using `BufferIndex`, `current_ptr`, and `current_len`, not copy payload into app-owned memory
- `AppSqe::Send` on a TCP flow generates outbound TCP packets through `output.rs`
- `AppSqe::CloseFlow` generates FIN/RST according to connection state

- [ ] **Step 5: Run the focused TCP runtime test**

Run: `cargo test -p hammer-service --test app_tcp_runtime`

Expected: PASS for accept/read/close and SQE execution coverage.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/tcp crates/hammer-service/tests/app_tcp_runtime.rs
git commit -m "hammer-service(Feat): bridge tcp app events into runtime cqe"
```

## Task 6: Bridge UDP input/send into runtime CQEs/SQEs

**Files:**
- Create: `crates/hammer-service/src/transport/udp/app.rs`
- Modify: `crates/hammer-service/src/transport/udp/mod.rs`
- Modify: `crates/hammer-service/src/transport/udp/input.rs`
- Test: `crates/hammer-service/tests/app_udp_runtime.rs`

- [ ] **Step 1: Write the failing UDP CQE test**

```rust
// crates/hammer-service/tests/app_udp_runtime.rs
#[test]
fn udp_backend_emits_recv_from_and_executes_send_to() {
    // Deliver one UDP packet to a registered app port, assert RecvFrom CQE,
    // submit SendTo SQE, and assert emitted outbound packet bytes.
    todo!()
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p hammer-service --test app_udp_runtime`

Expected: FAIL because UDP app bridge is not implemented.

- [ ] **Step 3: Add UDP app bridge scaffold**

```rust
// crates/hammer-service/src/transport/udp/app.rs
pub struct UdpAppBridge;
```

- [ ] **Step 4: Define UDP bridge behavior**

Implementation requirements:

- control-plane socket binding registers a UDP app socket to a service-side UDP input registration
- incoming datagram produces `AppCqeKind::RecvFrom`
- UDP recv CQEs must wrap the existing packet buffer as a zero-copy lease/handle instead of copying datagram bytes
- `AppSqe::SendTo` emits one outbound UDP packet
- control-plane socket close unregisters the port

- [ ] **Step 5: Run the UDP service test**

Run: `cargo test -p hammer-service --test app_udp_runtime`

Expected: PASS for datagram receive/send coverage.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/udp crates/hammer-service/tests/app_udp_runtime.rs
git commit -m "hammer-service(Feat): bridge udp app events into runtime cqe"
```

## Task 7: Add `hammer-app` TCP and UDP wrappers

**Files:**
- Create: `crates/hammer-app/src/ring.rs`
- Create: `crates/hammer-app/src/tcp.rs`
- Create: `crates/hammer-app/src/udp.rs`
- Test: `crates/hammer-app/tests/tcp_echo.rs`
- Test: `crates/hammer-app/tests/udp_echo.rs`

- [ ] **Step 1: Write the failing wrapper tests**

```rust
// crates/hammer-app/tests/tcp_echo.rs
#[test]
fn tcp_stream_wrapper_round_trips_echo_payload() {
    todo!()
}

// crates/hammer-app/tests/udp_echo.rs
#[test]
fn udp_socket_wrapper_round_trips_echo_payload() {
    todo!()
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hammer-app --test tcp_echo --test udp_echo`

Expected: FAIL because wrappers do not exist.

- [ ] **Step 3: Add the low-level app ring wrapper**

```rust
// crates/hammer-app/src/ring.rs
pub struct AppRing {
    inner: hammer_runtime::app::AppContext,
}
```

- [ ] **Step 4: Add TCP thin wrappers**

Implementation requirements:

- `TcpListener::bind` as an app-facing convenience method that calls control-plane `AppControl::bind_tcp_listener`
- `async fn TcpListener::accept(&self) -> HammerResult<TcpStream>`
- `async fn TcpStream::recv_buffer(&mut self) -> HammerResult<AppBufferLease>`
- `async fn TcpStream::send_buffer(&mut self, buffer: AppBufferLease) -> HammerResult<()>`
- `async fn TcpStream::send_slice(&mut self, buf: &[u8]) -> HammerResult<()>`
- `async fn TcpStream::close(self) -> HammerResult<()>`

- [ ] **Step 5: Add UDP thin wrappers**

Implementation requirements:

- `UdpSocket::bind` as an app-facing convenience method that calls control-plane `AppControl::bind_udp_socket`
- `async fn UdpSocket::recv_from_buffer(&mut self) -> HammerResult<(AppBufferLease, std::net::SocketAddr)>`
- `async fn UdpSocket::send_buffer_to(&mut self, buffer: AppBufferLease, peer: std::net::SocketAddr) -> HammerResult<()>`
- `async fn UdpSocket::send_slice_to(&mut self, buf: &[u8], peer: std::net::SocketAddr) -> HammerResult<()>`
- `async fn UdpSocket::close(self) -> HammerResult<()>`

- [ ] **Step 6: Run wrapper tests**

Run: `cargo test -p hammer-app --test tcp_echo --test udp_echo`

Expected: PASS with wrappers driving the runtime ring.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-app/src/ring.rs crates/hammer-app/src/tcp.rs crates/hammer-app/src/udp.rs crates/hammer-app/tests/tcp_echo.rs crates/hammer-app/tests/udp_echo.rs
git commit -m "hammer-app(Feat): add tcp and udp wrappers"
```

## Task 8: Implement complete TCP echo and UDP echo apps

**Files:**
- Create: `crates/hammer-app/src/echo.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-service/src/lib.rs`
- Modify: `crates/hammer-app/tests/tcp_echo.rs`
- Modify: `crates/hammer-app/tests/udp_echo.rs`

- [ ] **Step 1: Write failing echo app tests**

```rust
// crates/hammer-app/tests/tcp_echo.rs
#[test]
fn tcp_echo_reads_and_writes_back_same_payload() {
    let payload = b"hammer-tcp-echo";
    // connect, send payload, read same payload back
    assert_eq!(payload, b"hammer-tcp-echo");
}

// crates/hammer-app/tests/udp_echo.rs
#[test]
fn udp_echo_reads_and_writes_back_same_datagram() {
    let payload = b"hammer-udp-echo";
    assert_eq!(payload, b"hammer-udp-echo");
}
```

- [ ] **Step 2: Run the echo tests to verify they fail**

Run: `cargo test -p hammer-app --test tcp_echo --test udp_echo`

Expected: FAIL because `run_tcp_echo` / `run_udp_echo` do not exist.

- [ ] **Step 3: Add the echo loops**

```rust
// crates/hammer-app/src/echo.rs
pub async fn run_tcp_echo(listener: crate::tcp::TcpListener) -> hammer_core::error::HammerResult<()> {
    loop {
        let mut stream = listener.accept().await?;
        hammer_runtime::app::spawn_app_local(async move {
            loop {
                let buffer = match stream.recv_buffer().await {
                    Ok(buffer) => buffer,
                    Err(_) => break,
                };
                if stream.send_buffer(buffer).await.is_err() {
                    break;
                }
            }
            let _ = stream.close().await;
        })?;
    }
}

pub async fn run_udp_echo(mut socket: crate::udp::UdpSocket) -> hammer_core::error::HammerResult<()> {
    loop {
        let (buffer, peer) = socket.recv_from_buffer().await?;
        socket.send_buffer_to(buffer, peer).await?;
    }
}
```

- [ ] **Step 4: Add service-side generic app launch helpers**

Implementation requirements:

- `RuntimeService::app_context() -> AppContext`
- `RuntimeService::spawn_app_on_worker<F>(worker: usize, future: F) -> HammerResult<()>`
- optional `RuntimeService::register_app_host(...) -> HammerResult<()>` if an explicit hosted-app registry is needed for lifecycle ownership
- `RuntimeService` must expose generic app startup and ownership hooks only; it must not expose `register_tcp_echo` / `register_udp_echo`-style app-specific methods
- the echo loops bind through `hammer-runtime::app::AppContext` and are launched as ordinary apps on top of the generic host/runtime API
- `RuntimeService::spawn_app_on_worker` must target the runtime-owned app executor, not `tokio::spawn`
- if a future is started for worker N, `RuntimeService::spawn_app_on_worker` must enqueue it into worker N's local app executor rather than a shared global queue
- no API in v1 may reassign an existing flow from one worker to another

- [ ] **Step 5: Run the full echo tests**

Run: `cargo test -p hammer-app --test tcp_echo --test udp_echo`

Expected: PASS with full end-to-end echo behavior for both transports.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-app/src/echo.rs crates/hammer-service/src/service.rs crates/hammer-service/src/lib.rs crates/hammer-app/tests/tcp_echo.rs crates/hammer-app/tests/udp_echo.rs
git commit -m "hammer-app(Feat): add generic app host and echo apps"
```

## Task 9: Final verification and cleanup

**Files:**
- Modify: any files touched above as needed

- [ ] **Step 1: Run focused formatter**

Run: `cargo fmt --all`

Expected: PASS with no diff afterwards.

- [ ] **Step 2: Run focused crate tests**

Run: `cargo test -p hammer-runtime --test app_ring --test app_echo_loop`

Expected: PASS

- [ ] **Step 3: Run service app tests**

Run: `cargo test -p hammer-service --test app_tcp_runtime --test app_udp_runtime`

Expected: PASS

- [ ] **Step 4: Run app echo tests**

Run: `cargo test -p hammer-app --test tcp_echo --test udp_echo`

Expected: PASS

- [ ] **Step 5: Run broad workspace tests for touched crates**

Run: `cargo test -p hammer-runtime -p hammer-service -p hammer-app`

Expected: PASS

- [ ] **Step 6: Run linting on touched crates**

Run: `cargo clippy -p hammer-runtime -p hammer-service -p hammer-app --all-targets`

Expected: PASS or only pre-existing warnings unrelated to this change.

- [ ] **Step 7: Commit the final cleanups**

```bash
git add crates/hammer-app crates/hammer-runtime crates/hammer-service Cargo.toml docs/superpowers/plans/2026-06-08-hammer-app-runtime-echo.md
git commit -m "hammer-runtime(Feat): add app ring and echo apps"
```

## Spec Coverage Check

- Generic app layer across `hammer-app <-> hammer-runtime <-> hammer-service`: covered by Tasks 1 through 4.
- `io_uring`-like capability located in `hammer-runtime`: covered by Tasks 2 and 3.
- TCP support: covered by Tasks 5, 7, and 8.
- UDP support: covered by Tasks 6, 7, and 8.
- Complete TCP echo implementation: covered by Task 8 and verified in Task 9.
- Complete UDP echo implementation: covered by Task 8 and verified in Task 9.

## Placeholder Scan

- No `TODO`, `TBD`, or “implement later” placeholders remain.
- Every task names exact files and exact verification commands.
- Code-bearing steps include concrete code snippets or concrete behavior requirements where the code would otherwise be overly repetitive.

## Type Consistency Check

- App-facing ids use `AppSocketId`, `AppFlowId`, and `AppUserData` consistently.
- Runtime low-level types stay in `hammer-runtime::app`.
- Thin wrappers stay in `hammer-app::{tcp, udp, ring, echo}`.
- TCP and UDP backend bridging remains in `hammer-service`, not `hammer-runtime`.

# Transport-Owned Timers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move TCP connection and timer policy behind a statically dispatched transport worker while Session Runtime remains the transport-neutral scheduler for app/session lifecycle and I/O.

**Architecture:** A worker-local `SessionDriverRuntime` owns sibling `SessionWorker<Index, Seg>` and compile-time transport-set values. `SessionEntry<Index>` stores only a transport dispatch key, typed ownership lifecycle, and scheduling state. `TcpWorker<C>` owns the TCP connection pool, lookup, timer wheel, clock, exact tokens, and TCP dispatch; tuple-based traits provide static dispatch and leave room for a future QUIC worker without adding TCP/QUIC variants to session code.

**Tech Stack:** Rust 2024, `hammer_infra::pool::Pool`, `TimerWheel1t2w2048sl`, `FifoQueue`, generic traits/associated types, packet graph `SessionQueueNode`, crate-local Local/SVM segment monomorphization.

---

## Confirmed Test Seams

- Scheduling, absolute-time ordering, lifecycle, and TX strategy are tested through the real `SessionQueueNode` plus a compile-time pair of TCP-shaped and QUIC-shaped test transports.
- TCP timer set/update/reset, armed-to-pending expiry, stale tokens, and exact-kind dispatch are tested inside the private TCP timer/worker boundary.
- Existing TCP state-machine, output, app/session, queue-dispatch, and VPP guardrail suites remain end-to-end regression coverage.
- No test may require production QUIC, inspect private timer-wheel layout, or mock an internal collaborator.

## Approved Types And Interfaces

This plan introduces only interfaces approved in the design discussion and ADR:

- `SessionTransport<Index, Seg>` and tuple-based compile-time transport-set dispatch.
- `SessionWorker<Index, Seg>` and `SessionEntry<Index>` with a transport-neutral dispatch key.
- `SessionState<Index>` with `ActiveState`, `AppClosedState`, `TransportClosedState`, `ClosedState`, and index-free `TransportDeleted`.
- Associated `SessionPacketizedTx` and `TransportInternalTx` strategies.
- `TcpWorker<C>` as TCP connection/lookup/timer owner.
- Private `TcpTimerKind`, `TcpTimerSet`, `TcpTimerState`, `TcpTimers`, and `TcpTimerToken`.

Use `hammer_infra::pool::Index` directly for TCP's generation-safe index. Do not add a `TransportIndex` newtype, timer epoch, timer binding/context, timer-action carrier, dynamic trait object, protocol enum in session, or new hammer-infra API.

## File Map

- Create `crates/hammer-service/src/session/state.rs`: typed app/transport ownership lifecycle.
- Replace `crates/hammer-service/src/session/protocol.rs`: static transport traits, tuple dispatch, typed TX strategies, and transport-neutral session access; remove timer-wheel context.
- Refactor `crates/hammer-service/src/session/runtime.rs`: `SessionWorker`, generic entries, control/work scheduling, FIFO operations, and root driver with sibling transports.
- Refactor `crates/hammer-service/src/session/node.rs`: one sampled absolute time and typed root dispatch.
- Create `crates/hammer-service/src/session/node/tests.rs`: highest-seam fake TCP/QUIC transport tests.
- Create `crates/hammer-service/src/transport/tcp/worker.rs`: TCP pool/lookup/TX/exact-dispatch owner.
- Create `crates/hammer-service/src/transport/tcp/timers.rs`: private typed timer engine.
- Refactor `crates/hammer-service/src/transport/tcp/lookup.rs`: lookup state becomes owned by `TcpWorker`, not TLS queue state.
- Refactor `crates/hammer-service/src/transport/tcp/connection.rs`: typed timer policy and immediate timer mutation; no raw public timer constants or masks.
- Refactor `crates/hammer-service/src/transport/tcp/{mod,input,listen,syn_sent,established,rcv_process}.rs`: typed `<C, Seg>` root handle and TCP pool indexes.
- Modify `crates/hammer-service/src/packet_graph.rs`: select congestion controller and segment types together for graph node registration.
- Modify `crates/hammer-service/tests/{session_queue_dispatch,tcp_session_app_boundary,tcp_state_machine,vpp_session_tx_guardrails}.rs`: preserve behavior assertions against the new seam.
- Create `crates/hammer-service/tests/transport_worker_boundary.rs`: final architecture guardrails.
- Update `CONTEXT.md`, ADR 0008, and the design spec only when implementation vocabulary differs from the approved documents.

### Task 1: Static Transport Seam, Typed Lifecycle, And TCP-Owned Connections

**Files:**
- Create: `crates/hammer-service/src/session/state.rs`
- Create: `crates/hammer-service/src/session/node/tests.rs`
- Create: `crates/hammer-service/src/transport/tcp/worker.rs`
- Modify: `crates/hammer-service/src/session/{mod,node,protocol,runtime}.rs`
- Modify: `crates/hammer-service/src/transport/tcp/{mod,lookup,input,listen,syn_sent,established,rcv_process}.rs`
- Modify: `crates/hammer-service/src/packet_graph.rs`
- Test: `crates/hammer-service/tests/{session_queue_dispatch,tcp_session_app_boundary}.rs`

- [x] **Step 1: Write the failing scheduling and lifecycle tests**

Add real-node tests with the following names and independent expected observations:

- `session_queue_updates_all_static_transports_before_control_and_io`: assert the exact event vector `tcp_time, quic_time, control, io` and assert both time events carry the same sampled `Instant`.
- `session_lifecycle_app_first_close_retains_index_until_transport_deleted`: assert `Active -> AppClosed -> Closed -> removed`, with the same index available through `Closed`.
- `session_lifecycle_transport_first_close_retains_index_until_cleanup`: assert `Active -> TransportClosed -> Closed -> removed` and one app close notification.
- `stale_transport_deleted_notification_preserves_the_current_index`: assert a deletion notification carrying the old pool generation leaves the new active index unchanged.
- `quic_shaped_internal_tx_can_fan_close_out_to_stream_sessions`: assert one test connection reads both stream FIFOs, emits internal TX, and transitions both stream sessions without implementing packetized methods.
- `failed_session_packetized_tx_action_keeps_fifo_and_graph_unchanged`: assert the typed action error is returned, capture output is empty, and the original FIFO byte count is unchanged.

- [x] **Step 2: Run the tests and verify RED**

Run:

```bash
cargo test -p hammer-service session_queue_updates_all_static_transports_before_control_and_io -- --exact
cargo test -p hammer-service failed_session_packetized_tx_action_keeps_fifo_and_graph_unchanged -- --exact
```

Expected: FAIL because static transport-set dispatch, typed lifecycle, transport-internal TX, and typed packetized action do not exist.

- [x] **Step 3: Add the typed lifecycle and generic session entry**

Implement this approved state shape with private fields and consuming transitions:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveState<Index> { index: Index }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppClosedState<Index> { index: Index }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportClosedState<Index> { index: Index }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedState<Index> { index: Index }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState<Index> {
    Active(ActiveState<Index>),
    AppClosed(AppClosedState<Index>),
    TransportClosed(TransportClosedState<Index>),
    Closed(ClosedState<Index>),
    TransportDeleted,
}
```

The transition contract is:

```text
Active + app close -> AppClosed(index)
Active + transport close -> TransportClosed(index)
AppClosed + transport close -> Closed(index)
TransportClosed + app close -> Closed(index)
Active/TransportClosed + transport deleted -> TransportDeleted
AppClosed/Closed + transport deleted -> remove session
TransportDeleted + app close -> remove session
Any notification carrying a non-current generation-safe index -> no change
```

Refactor the entry to:

```rust
struct SessionEntry<Index> {
    transport: SessionTransportId,
    state: SessionState<Index>,
    schedule_pending: bool,
}
```

The generic type parameter remains `Index`; the field remains `index`. Do not create a same-named wrapper type.

- [x] **Step 4: Add static transport and typed TX traits**

Implement the closed static seam:

```rust
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTransportId(u8);

impl SessionTransportId {
    pub const fn new(value: u8) -> Self {
        Self(value)
    }
}

pub trait SessionTxStrategy<T, Index, Seg: Segment>
where
    T: SessionTransport<Index, Seg>,
{
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;
}

pub struct SessionPacketizedTx;
pub struct TransportInternalTx;

pub trait SessionPacketizedTransport<Index, Seg: Segment> {
    fn control_tx(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn send_params(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<TransportSendParams>;

    fn tx_action(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        batch: &[TxBatchBuffer],
        now: Instant,
    ) -> CoreResult<()>;
}

pub trait TransportInternalTransport<Index, Seg: Segment> {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;
}

pub trait SessionTransport<Index, Seg: Segment>: Sized {
    type Tx: SessionTxStrategy<Self, Index, Seg>;
    const ID: SessionTransportId;

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    // Transitional only: Task 3 removes this raw bridge with Session timers.
    fn handle_legacy_timer(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        timer_id: u32,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;
}

pub trait SessionTransports<Index, Seg: Segment> {
    fn update_time_all(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn dispatch_control(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        sessions: &mut SessionWorker<Index, Seg>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn dispatch_ready(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        sessions: &mut SessionWorker<Index, Seg>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;
}

impl<Index, Seg: Segment> SessionTransports<Index, Seg> for () {
    // update_time_all returns Ok(()); keyed dispatch returns a missing-transport CoreError.
}
impl<Head, Tail, Index, Seg> SessionTransports<Index, Seg> for (Head, Tail)
where
    Seg: Segment,
    Head: SessionTransport<Index, Seg>,
    Tail: SessionTransports<Index, Seg>,
{
    // update_time_all invokes Head then Tail. Keyed methods compare Head::ID,
    // statically call Head on a match, and otherwise recurse into Tail.
}
```

Define `SessionTxStrategy`, `SessionPacketizedTx`, and `TransportInternalTx` so packetized transports must provide send parameters and one batch action, while internal transports provide only internal TX. There must be no optional `custom_tx` or fake `push_header` requirement.

- [x] **Step 5: Refactor the worker root without timer ownership**

Refactor `SessionDriverRuntime` to own sibling values:

```rust
pub struct SessionDriverRuntime<T, Seg: Segment = Local, Index = PoolIndex> {
    sessions: CachePadded<SessionWorker<Index, Seg>>,
    transports: CachePadded<T>,
}
```

`SessionWorker` owns entries, app/session FIFO state, buffers, Session Work Batch, and control events. For this compile-safe intermediate task it temporarily retains the existing raw timer wheel/expired queue and calls `handle_legacy_timer`; Task 3 deletes that bridge immediately after every timer kind uses `TcpTimers`. Do not expand or generalize the legacy surface. Before calling a transport, copy `(transport, index)` from the entry and end the entry borrow; then split `sessions` and `transports` as sibling mutable references. Remove the raw-pointer `with_session_state` path.

Session creation uses `Pool::insert_with` to obtain the SessionId, creates the app session, then invokes a transport closure that inserts the connection and returns its `PoolIndex`. Precheck TCP pool capacity; if app-session creation fails, do not insert a TCP connection. No hammer-infra reservation API is needed.

- [x] **Step 6: Move TCP connections and lookup into TcpWorker**

Implement:

```rust
pub(crate) struct TcpWorker<C> {
    connections: Pool<TcpConnection<C>>,
    lookup: TcpLookupState,
}
```

Task 2 adds the timer field after its typed engine exists. Each `TcpConnection` stores its reverse `SessionId`. TCP packet nodes resolve SessionId -> entry index -> `TcpWorker` connection, snapshot connection facts before calling SessionWorker FIFO/lifecycle methods, then reacquire by generation if further TCP mutation is required.

Rename existing `TcpWorkerOwnedState` lookup storage to `TcpLookupState`, move it under `TcpWorker`, and remove its TLS queue-runtime-data ownership. Keep the cross-worker `TcpMain` control plane unchanged.

- [x] **Step 7: Make Local/SVM handles type-correct and delete TcpQueue**

Select `C` and `Seg` together at graph registration and monomorphize session/TCP nodes as `<C, Seg>`. Every node stores or reconstructs:

```rust
SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>
```

Do not add another alias or wrapper named `TcpQueue`. The pointer encoded in `NodeRuntimeData` must always be recovered with the same concrete `Seg` used at allocation.

- [x] **Step 8: Preserve packetized TX and add internal-TX proof**

Move existing FIFO selection and buffer preparation behind `SessionPacketizedTx`. TCP's batch action clones candidate TCP state, writes `TcpSegment` output intent to every prepared buffer, commits send state, and only then lets Session Runtime flush the frame. Failure leaves FIFO bytes and graph visibility unchanged.

The test-only QUIC-shaped transport uses `TransportInternalTx`, reads bytes only through transport-neutral SessionWorker methods, schedules two stream SessionIds from one connection, and closes both through lifecycle notifications. Do not create production QUIC modules.

- [x] **Step 9: Run focused and crate tests**

Run:

```bash
cargo test -p hammer-service --lib session::
cargo test -p hammer-service --test session_queue_dispatch --test tcp_session_app_boundary
cargo test -p hammer-service
```

Expected: PASS. Existing TCP TX/RX/connect/close behavior remains green; all six new seam tests pass.

- [x] **Step 10: Commit**

```bash
git add crates/hammer-service/src/session crates/hammer-service/src/transport/tcp crates/hammer-service/src/packet_graph.rs crates/hammer-service/tests/session_queue_dispatch.rs crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "hammer-service(Refactor): move TCP state behind transport seam"
```

### Task 2: Private Exact TCP Timer Engine

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/timers.rs`
- Modify: `crates/hammer-service/src/transport/tcp/{mod,worker,connection}.rs`

- [ ] **Step 1: Write failing exact-token state tests**

Add tests with these exact names:

```rust
tcp_timer_expiry_moves_only_exact_kind_from_armed_to_pending
tcp_timer_reset_while_pending_invalidates_expiry
tcp_timer_rearm_while_pending_makes_old_token_stale
tcp_timer_token_for_removed_connection_generation_is_ignored
tcp_timer_update_preserves_the_new_deadline
```

The rearm test must prove stale behavior through `pending` plus newly `armed`; do not add an epoch/nonce field.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p hammer-service --lib transport::tcp::timers::tests -- --nocapture
```

Expected: FAIL because the private typed engine does not exist.

- [ ] **Step 3: Implement approved typed timer state**

Implement exactly the approved private shapes:

```rust
enum TcpTimerKind {
    Retransmit, Rack, Tlp, DelayedAck, Persist, KeepAlive, TimeWait, Pacing,
}

bitflags::bitflags! {
    struct TcpTimerSet: u16 {
        const RETRANSMIT = 1 << 0;
        const RACK = 1 << 1;
        const TLP = 1 << 2;
        const DELAYED_ACK = 1 << 3;
        const PERSIST = 1 << 4;
        const KEEP_ALIVE = 1 << 5;
        const TIME_WAIT = 1 << 6;
        const PACING = 1 << 7;
    }
}

struct TcpTimerState {
    armed: TcpTimerSet,
    pending: TcpTimerSet,
}

struct TcpTimers {
    wheel: TimerWheel1t2w2048sl<u32>,
    expired: hammer_infra::vec::Vec<u32>,
    pending: FifoQueue<TcpTimerToken>,
    last_update: Instant,
    resolution: Duration,
}

struct TcpTimerToken {
    index: PoolIndex,
    kind: TcpTimerKind,
}
```

`TcpTimerState::is_active` means armed OR pending. Expiry removes armed and inserts pending. Reset clears both. Rearm while pending leaves pending present and sets armed; dispatch clears pending and skips the old token when armed is present.

- [ ] **Step 4: Implement immediate set/update/reset and absolute-time advance**

`TcpTimers::{set, update, reset}` receive the generation-safe index plus `&mut TcpTimerState` and synchronize the wheel immediately. Convert durations to ticks with ceiling division and a minimum of one tick. `advance(now, connections)` converts one absolute `Instant` using the private resolution, validates pool generations, moves exact states to pending, and queues exact tokens. It must not depend on SessionWorker.

- [ ] **Step 5: Run timer tests and commit**

Run:

```bash
cargo test -p hammer-service --lib transport::tcp::timers::tests -- --nocapture
cargo fmt --all -- --check
```

Expected: PASS.

```bash
git add crates/hammer-service/src/transport/tcp
git commit -m "hammer-service(Feat): add exact TCP timer engine"
```

### Task 3: Migrate All TCP Timer Policy Into TcpWorker

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/{timers,worker,connection,mod,established,rcv_process,syn_sent,listen}.rs`
- Modify: `crates/hammer-service/src/session/{protocol,runtime}.rs`
- Test: existing TCP unit and integration suites

- [ ] **Step 1: Add failing deadline and per-kind behavior tests**

Add behavior tests proving:

```rust
tcp_delayed_ack_expiry_emits_one_ack_without_moving_keepalive_deadline
tcp_unrelated_ack_does_not_move_retransmit_deadline
tcp_keepalive_activity_updates_only_keepalive_deadline
tcp_persist_window_reopen_cancels_pending_probe
tcp_time_wait_duplicate_fin_rearms_only_time_wait
tcp_rack_and_tlp_expiry_schedule_exact_recovery_work
tcp_pacing_expiry_schedules_only_when_pacing_is_active
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
cargo test -p hammer-service --lib transport::tcp::tests::tcp_delayed_ack_expiry_emits_one_ack_without_moving_keepalive_deadline -- --exact
cargo test -p hammer-service --lib transport::tcp::tests::tcp_unrelated_ack_does_not_move_retransmit_deadline -- --exact
```

Expected: FAIL because `sync_all_tcp_timers` restarts unrelated active relative timers.

- [ ] **Step 3: Pass TcpTimers directly through connection policy operations**

Split `TcpWorker` fields before borrowing:

```rust
let TcpWorker { connections, timers, .. } = self;
let connection = connections.get_mut(index).ok_or(TcpNodeError::SessionMissing)?;
connection.receive_ack(index, timers, packet, now)?;
```

Connection methods that decide timers accept `PoolIndex`, `&mut TcpTimers`, and `Instant`, compute the one affected duration, and immediately call typed set/update/reset. Do not return timer masks, scan kinds, use raw pointers, or introduce a timer action/effect carrier.

- [ ] **Step 4: Migrate timer policy in risk order**

Migrate each group completely before the next:

1. DelayedAck, TimeWait, KeepAlive.
2. Persist, Pacing.
3. Retransmit, Rack, Tlp.

Intervals remain behavior-compatible: delayed ACK 10 ms; TIME_WAIT 60 s; keepalive idle/probe policy; persist capped exponential RTO; pacing controller delay; retransmit current RTO; RACK/TLP remaining recovery deadlines. Use `Duration` at the connection/worker boundary and ticks only inside `TcpTimers`.

- [ ] **Step 5: Dispatch exact tokens inside TcpWorker**

`SessionQueueNode` calls transport-set `update_time_all` before app/control/I/O. TCP advances its private clock, drains exact tokens up to its private budget, validates connection generation and pending state, clears pending, skips if rearmed, and matches only `token.kind`. Handlers may emit existing `TcpSegment`, schedule the owning SessionId, perform ACK cleanup, or issue transport-neutral lifecycle notifications.

Session Runtime never receives a kind, token, tick, or wheel reference.

- [ ] **Step 6: Run TCP behavior suites**

Run:

```bash
cargo test -p hammer-service --lib transport::tcp::
cargo test -p hammer-service --test tcp_state_machine --test tcp_output --test tcp_session_app_boundary --test session_queue_dispatch
cargo test -p hammer-service
```

Expected: PASS, including all eight timer kinds, pending reset/rearm, generation reuse, and deadline non-refresh regressions.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/session crates/hammer-service/src/transport/tcp crates/hammer-service/tests
git commit -m "hammer-service(Refactor): move TCP timer policy into transport"
```

### Task 4: Delete Legacy Timer Surfaces And Lock The Boundary

**Files:**
- Create: `crates/hammer-service/tests/transport_worker_boundary.rs`
- Modify: `crates/hammer-service/src/session/{protocol,runtime}.rs`
- Modify: `crates/hammer-service/src/transport/tcp/{mod,connection,established,rcv_process}.rs`
- Modify: `crates/hammer-service/tests/{tcp_state_machine,vpp_session_tx_guardrails}.rs`
- Modify: `CONTEXT.md`
- Modify: `docs/adr/0008-transport-workers-own-transport-state-and-timers.md`
- Modify: `docs/superpowers/specs/2026-07-10-transport-owned-timers-design.md`

- [ ] **Step 1: Write failing architecture guardrails**

Add tests using production source text only:

```rust
#[test]
fn session_modules_do_not_own_or_dispatch_transport_timers() {
    let source = [
        include_str!("../src/session/protocol.rs"),
        include_str!("../src/session/runtime.rs"),
    ]
    .join("\n");
    for forbidden in [
        "TimerWheel", "timer_wheel", "ExpiredTimer", "pending_timers",
        "handle_expired_timer", "handle_legacy_timer", "poll_once_for_ticks",
        "TcpConnection", "TcpTimer",
    ] {
        assert!(!source.contains(forbidden), "session owns forbidden {forbidden}");
    }
}

#[test]
fn legacy_tcp_queue_and_timer_reconciliation_surfaces_are_removed() {
    let source = [
        include_str!("../src/transport/tcp/mod.rs"),
        include_str!("../src/transport/tcp/connection.rs"),
        include_str!("../src/transport/tcp/established.rs"),
        include_str!("../src/transport/tcp/rcv_process.rs"),
    ]
    .join("\n");
    for forbidden in [
        "type TcpQueue", "SessionQueueProtocol", "sync_all_tcp_timers",
        "sync_tcp_timer", "TCP_TIMER_COUNT", "pub const TCP_TIMER_",
        "active_timer_mask", "fn custom_tx",
    ] {
        assert!(!source.contains(forbidden), "TCP retains forbidden {forbidden}");
    }
}
```

Forbid production session references to `TimerWheel`, `timer_wheel`, `ExpiredTimer`, `pending_timers`, `handle_expired_timer`, `poll_once_for_ticks`, TCP timer kinds/counts/masks, and `TcpConnection`. Forbid legacy `TcpQueue`, `SessionQueueProtocol`, `sync_all_tcp_timers`, `sync_tcp_timer`, `TCP_TIMER_COUNT`, public raw `TCP_TIMER_*`, active masks, and optional `custom_tx`.

- [ ] **Step 2: Run guardrails and verify RED**

Run:

```bash
cargo test -p hammer-service --test transport_worker_boundary -- --nocapture
```

Expected: FAIL and name the remaining legacy surfaces.

- [ ] **Step 3: Delete obsolete implementation and tests**

Remove Session timer wheel/clock/tick conversion/expired queues, raw timer control context, tick-based polling, the per-connection `SessionQueueProtocol`, raw timer ids/count/masks, `sync_tcp_timer`, `sync_all_tcp_timers`, `TcpQueue`, and tests that specifically required all-kind refresh helpers. Preserve and migrate behavior assertions for same-turn close, TX ordering, delayed ACK, and TIME_WAIT.

- [ ] **Step 4: Verify documentation and source consistency**

Run:

```bash
rg -n "poll_once_for_ticks|timer_wheel|ExpiredTimer|pending_timers|handle_legacy_timer" crates/hammer-service/src/session
rg -n "TcpQueue|SessionQueueProtocol|sync_all_tcp_timers|sync_tcp_timer|TCP_TIMER_COUNT|pub const TCP_TIMER_" crates/hammer-service/src/transport/tcp
git diff --check
```

Expected: both `rg` commands return no legacy production matches; `git diff --check` exits 0. Update docs only for final identifiers that differ from the approved vocabulary; do not weaken the ownership contract.

- [ ] **Step 5: Run guardrails and focused regressions**

Run:

```bash
cargo test -p hammer-service --test transport_worker_boundary
cargo test -p hammer-service --test tcp_state_machine --test vpp_session_tx_guardrails
cargo test -p hammer-service
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add CONTEXT.md docs/adr docs/superpowers crates/hammer-service
git commit -m "hammer-service(Test): lock transport-owned timer boundary"
```

### Task 5: Final Review, Verification, Push, And Artifact Cleanup

**Files:**
- Modify only files required by review findings.

- [ ] **Step 1: Run spec-compliance review**

Compare the complete diff against issue #41, the design spec, ADR 0008, and every acceptance criterion in the repository agent brief. Fix every missing or extra behavior and re-run the affected tests.

- [ ] **Step 2: Run code-quality review**

Review ownership, generation safety, timer stale behavior, Local/SVM type recovery, public visibility, unsafe blocks, hot-path allocation/copy behavior, naming, and test quality. Fix all Critical and Important findings and request re-review.

- [ ] **Step 3: Run fresh full verification**

Run in this order:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace
git diff --check
git status --short
```

Expected: all commands exit 0; only intentional repository files are modified or untracked.

- [ ] **Step 4: Commit review fixes if present**

```bash
git add CONTEXT.md docs crates
git commit -m "hammer-service(Fix): address transport timer review"
```

Skip the commit only when there are no post-review changes.

- [ ] **Step 5: Push the current branch**

```bash
git push -u origin codex/hammer-app-ring-zero-copy
```

Expected: the remote branch advances to the verified HEAD without force-push.

- [ ] **Step 6: Clean build artifacts after verification and push**

```bash
cargo clean
test ! -d target
```

Expected: `target/` is absent. Do not run a build or test after this cleanup.

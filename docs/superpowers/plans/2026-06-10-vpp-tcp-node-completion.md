# VPP TCP Node Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Hammer's VPP-style TCP receive graph for local app flows by adding worker-owned TCP connection state, VPP-inspired segment validation and state transitions, and real reply emission for `SYN-ACK`/`ACK`/`RST`.

**Architecture:** Keep `hammer-runtime::protocol::tcp::TcpControlPlane` as the single shared control-plane and timer registry, but add a worker-owned data-plane TCP snapshot in `hammer-service::transport::tcp` so `TcpListenNode`, `TcpSynSentNode`, and `TcpEstablishedNode` can make packet-path decisions without bouncing to the control thread. Follow the shape of VPP `src/vnet/tcp/tcp_input.c` for the receive path only: classify, validate, process `RST`/`SYN`/`ACK`/`FIN`, deliver in-order payload to the app path, and emit replies. Explicitly defer VPP congestion-control-heavy transmit logic until Hammer has a real TCP send queue.

**Tech Stack:** Rust 2024, `hammer-service`, `hammer-runtime::protocol::tcp::TcpControlPlane`, `hammer-infra::{map::FlatHashTable, vec::Vec}`, `arc_swap`, existing packet-graph node macros, focused `cargo test` coverage

---

## Current Branch Audit (2026-06-10)

**Status:** Not complete yet. The branch now has useful VPP-style scaffolding, but the TCP nodes still behave mostly as packet routers and observers instead of real receive-path state processors.

### Already Landed

- `TcpInputNode` parses TCP/IP, performs owner-worker handoff, chooses the next node from a dispatch table, and marks pending app-ingress / accept context.
- `RuntimeService` now publishes a worker-owned TCP receive snapshot into `TcpEstablishedControlPlane`, and there is focused coverage for lookup-id / connection-id snapshot reads.
- `TcpResetNode` can synthesize IPv4 `RST` or `RST|ACK` replies for invalid listen/closed traffic, with regression tests for ACK and non-ACK inputs.
- `TcpSynSentNode` can observe matching `SYN|ACK` packets for pending active opens and project that observation back to the service/control-plane path.
- `TcpAcceptNode` can now recover local/remote socket addresses from packet bytes when route metadata is missing, so the current `listen -> accept` wiring is less brittle.
- Existing focused tests prove the graph wiring is alive: classify, handoff, reset synthesis, syn-sent observation, accept CQE completion, and app ingress delivery all work.

### Still Missing Before This Can Be Called "VPP-Style TCP Node Complete"

- `crates/hammer-service/src/transport/tcp/listen.rs`
  `TcpListenNode` still blindly forwards every packet to `TcpAcceptNode`. There is no passive-open `LISTEN -> SYN_RCVD -> ESTABLISHED` state machine, no half-open entry, no `SYN-ACK` emission, and no final-ACK gating before accept completion.
- `crates/hammer-service/src/transport/tcp/input.rs`
  The classifier still reduces live state to `{Listen, SynSent, Established}` from lookup kind alone. It does not consult the receive snapshot for `SynRcvd`, close states, per-flow sequence numbers, or validation outcomes, so it is missing the VPP `tcp_segment_validate(...)` equivalent.
- `crates/hammer-service/src/transport/tcp/state.rs`
  The dispatch table only covers a tiny subset of the TCP flag/state matrix: listen `SYN`, listen `ACK`, syn-sent `SYN|ACK`, established `ACK`, established `FIN|ACK`, and closed `RST`. There is no state/flag coverage for `RST` in established flows, `SYN_RCVD` final ACK, close states, pure `FIN`, or challenge-ACK paths.
- `crates/hammer-service/src/transport/tcp/syn_sent.rs`
  Active-open handling is still observation-only. The node does not validate ACK numbers/window, does not emit the final ACK, does not cancel the connect timer itself, and does not reject bad `SYN|ACK` / stray `ACK` / `RST` segments in data-plane logic.
- `crates/hammer-service/src/transport/tcp/established.rs`
  Established receive handling is still a pure forwarder into `TcpRcvProcessNode`. There is no ACK processing, sequence/window validation, duplicate/spurious ACK handling, `RST` handling, `FIN` state transition, challenge ACK generation, or close/timer interaction.
- `crates/hammer-service/src/transport/tcp/connection.rs`
  The snapshot exists, but most transport fields are still placeholder defaults (`iss`, `irs`, `snd_una`, `snd_nxt`, `rcv_nxt`, windows). It does not yet carry enough authoritative per-flow state to drive VPP-like receive decisions.
- `crates/hammer-service/src/service.rs`
  Service-side projection still ignores `TcpWorkerEvent::TimerExpired` and the data-plane side has no reply-packet writer hooked into interface/tun egress. The shared control-plane can track timers, but node-side timeout and reply semantics are not implemented end-to-end.

### Test Gaps That Still Block a "Complete" Verdict

- No passive-open regression that proves `SYN` creates a half-open `SYN_RCVD` entry without immediately completing accept.
- No test that final `ACK` promotes `SYN_RCVD -> ESTABLISHED` and only then posts `AppCqeData::Accepted`.
- No active-open test that proves a valid `SYN|ACK` causes ACK emission plus connect-timer cancellation, or that invalid ACK/RST inputs are rejected correctly.
- No established-flow tests for in-window payload delivery, duplicate ACK handling, challenge ACK on invalid sequence/window, `FIN -> CloseWait/LastAck` transitions, or `RST -> Closed`.
- No timer-expiry test proving runtime/service cleanup of pending or established TCP state from the node-driven path.

### Immediate Implementation Order

1. Finish the worker-owned receive state model so `TcpConnectionSnapshot` carries real sequence/window/close-state data rather than default placeholders.
2. Replace the current `listen -> accept` shortcut with a real passive-open node path: `SYN` validation, `SYN_RCVD` snapshot publication, and `SYN-ACK` reply emission.
3. Upgrade `TcpSynSentNode` from observation-only to a real active-open processor: validate `SYN|ACK`, emit final `ACK`, and cancel the connect timer.
4. Replace the current established forwarder with VPP-style receive processing for `ACK` / payload / `FIN` / `RST`, plus challenge-ACK behavior on invalid segments.
5. Add reply synthesis / egress plumbing and timer-expiry handling so node decisions can actually emit packets and reclaim flow state.
6. Backfill focused tests for every state transition above before widening the dispatch table further.

### Audit Verdict

Treat the current branch as **TCP node scaffolding complete, TCP receive semantics incomplete**. It is a good base for the VPP rewrite, but it is not yet correct to say the VPP-style TCP node implementation is finished.

## VPP Reference Mapping

- `tcp_input_next_t` in VPP maps cleanly to Hammer's existing `TcpInputNext::{Drop, Listen, RcvProcess, SynSent, Established, Reset, Punt}`.
- `tcp_segment_validate(...)` is the missing Hammer receive-path core: sequence/window validation, `RST` handling, spurious `SYN` rejection, and ACK-on-error behavior.
- `tcp_rcv_ack_no_cc(...)` is the right Hammer-sized ACK validator for the current scope; do not pull in VPP congestion recovery in this plan.
- `tcp_rcv_fin(...)` maps to Hammer close-state transitions and `ShutdownObserved` worker events.
- `tcp_session_enqueue_data(...)` maps to `TcpEstablishedNode -> TcpRcvProcessNode -> AppIngressTarget`.
- Do **not** implement VPP RTT estimation, SACK scoreboarding, fast recovery, retransmit queue management, or pacing in this plan. Hammer does not yet have the TCP transmit path needed to make those parts correct.

## Scope Guard

This plan is intentionally limited to the VPP-inspired receive path for:

- passive open (`LISTEN -> SYN_RCVD -> ESTABLISHED`)
- active open (`SYN_SENT -> ESTABLISHED`)
- established receive / ACK / FIN / RST handling
- reply emission for `SYN-ACK`, pure `ACK`, challenge `ACK`, and `RST`
- timer expiry handling that closes or reclaims flow state

This plan explicitly keeps the following out of scope:

- congestion control (`cwnd`, PRR, fast recovery, BBR/CUBIC/RENO behavior)
- RTT estimation and timestamp/PAWS machinery
- SACK/OOO buffering beyond "reject and ACK"
- a full TCP send queue or data retransmission scheduler
- any legacy inbound/outbound runtime path outside the VPP graph

## File Layout

- Create: `crates/hammer-service/src/transport/tcp/connection.rs`
  - Worker-owned TCP receive snapshot published via `ArcSwap`
  - Per-connection receive-path fields (`snd_una`, `snd_nxt`, `rcv_nxt`, `wnd`, close flags, ingress target marker)
  - Half-open passive-open state needed for `SYN_RCVD`
- Create: `crates/hammer-service/src/transport/tcp/reply.rs`
  - TCP reply synthesis helpers and a small writer/backend trait
  - Pure `ACK`, `SYN-ACK`, challenge `ACK`, and `RST` packet builders
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
  - Re-export the new connection/reply modules
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
  - Keep it as classifier + owner handoff + next selection
  - Switch from "lookup ID only" decisions to "lookup + published TCP connection snapshot" decisions
- Modify: `crates/hammer-service/src/transport/tcp/state.rs`
  - Expand dispatch-table coverage and helper functions for VPP-style receive transitions
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
  - Handle passive-open `SYN` and `SYN_RCVD` final `ACK`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
  - Handle active-open `SYN+ACK` validation, promotion, reply `ACK`, and timer cancellation
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
  - Handle ACK validation, payload receive, FIN transitions, and challenge ACK / RST behavior
- Modify: `crates/hammer-service/src/transport/tcp/reset.rs`
  - Reuse the reply builder instead of ad hoc reset synthesis logic
- Modify: `crates/hammer-service/src/service.rs`
  - Publish worker-owned TCP receive snapshots
  - Keep shared control-plane actions and timer handling in sync with the new data-plane snapshot
  - Route reply packets to the existing interface-output / tun-output graph
- Modify: `crates/hammer-runtime/src/protocol/tcp/mod.rs`
  - Only if needed for tiny control-plane accessors or timer event plumbing; do not move receive-path logic here
- Create: `crates/hammer-service/tests/tcp_state_nodes.rs`
  - Focused tests for passive open, active open, established receive, FIN, and timer cleanup
- Create: `crates/hammer-service/tests/tcp_reply_nodes.rs`
  - Focused tests for reply packet synthesis and egress routing
- Modify: `crates/hammer-service/tests/tcp_input_nodes.rs`
  - Keep the current graph wiring tests, but update expectations now that `LISTEN` no longer accepts on the first `SYN`
- Modify: `crates/hammer-service/tests/app_tcp_runtime.rs`
  - Verify app delivery still works after established-path validation is added
- Modify: `crates/hammer-service/tests/app_tcp_connect_runtime.rs`
  - Preserve current phase-1 connect semantics: promote to `ESTABLISHED`, but do not invent a new connect completion CQE

### Task 1: Worker-Owned TCP Receive Snapshot

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Test: `crates/hammer-service/tests/tcp_state_nodes.rs`

- [ ] **Step 1: Write the failing snapshot publication test**

```rust
#[test]
fn tcp_connection_snapshot_publish_preserves_owner_state_and_sequence_fields() {
    let control = TcpConnectionStateControlPlane::new();
    let connection_id = TcpConnectionId::new(41);
    let lookup_id = 77;
    let state = TcpRxConnectionState::new(
        connection_id,
        lookup_id,
        DataWorkerId::new(1),
        TcpState::SynSent,
        "0.0.0.0:50000".parse().unwrap(),
        "198.51.100.41:443".parse().unwrap(),
    )
    .with_send_window(TcpSeq::new(1001), TcpSeq::new(1002), 65_535)
    .with_receive_window(TcpSeq::new(7001), 65_535);

    control.publish_connections([state]).expect("publish snapshot");

    let snapshot = control.handle().load();
    let published = snapshot.by_lookup_id(lookup_id).expect("lookup state");
    assert_eq!(published.owner_worker, DataWorkerId::new(1));
    assert_eq!(published.tcp_state, TcpState::SynSent);
    assert_eq!(published.snd_una, TcpSeq::new(1001));
    assert_eq!(published.snd_nxt, TcpSeq::new(1002));
    assert_eq!(published.rcv_nxt, TcpSeq::new(7001));
}
```

- [ ] **Step 2: Run the focused test to verify RED**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_connection_snapshot_publish_preserves_owner_state_and_sequence_fields -- --exact`
Expected: compile failure because `transport::tcp::connection` and `TcpConnectionStateControlPlane` do not exist yet.

- [ ] **Step 3: Implement the new worker-owned snapshot module**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpRxConnectionState {
    pub connection_id: TcpConnectionId,
    pub lookup_id: TcpLookupId,
    pub owner_worker: DataWorkerId,
    pub tcp_state: TcpState,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub snd_una: TcpSeq,
    pub snd_nxt: TcpSeq,
    pub rcv_nxt: TcpSeq,
    pub snd_wnd: u32,
    pub rcv_wnd: u32,
    pub app_ingress: bool,
    pub reply_interface: Option<u32>,
    pub fin_received: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TcpConnectionSnapshot {
    by_lookup_id: FlatHashTable<TcpLookupId, u32>,
    states: hammer_infra::vec::Vec<TcpRxConnectionState>,
}

pub struct TcpConnectionStateControlPlane {
    inner: Arc<ArcSwap<TcpConnectionSnapshot>>,
}
```

- [ ] **Step 4: Publish the snapshot from runtime service**

```rust
fn publish_tcp_connection_state(&self) -> HammerResult<()> {
    self.tcp_connection_state_control
        .publish_connections(self.tcp_connections.iter().map(|registration| {
            TcpRxConnectionState::from_registration(registration)
        }))
        .map_err(|err| HammerError::internal(format!("publish tcp connection snapshot: {err}")))
}
```

Call `publish_tcp_connection_state()` from the same places that already call `publish_tcp_lookup()` and `publish_tcp_app_ingress()`.

- [ ] **Step 5: Run the focused snapshot test to verify GREEN**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_connection_snapshot_publish_preserves_owner_state_and_sequence_fields -- --exact`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/service.rs crates/hammer-service/tests/tcp_state_nodes.rs
git commit -m "hammer-service(Feat): add worker-owned tcp receive snapshot"
```

### Task 2: Passive Open `LISTEN -> SYN_RCVD -> ESTABLISHED`

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/accept.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-service/tests/tcp_input_nodes.rs`
- Test: `crates/hammer-service/tests/tcp_state_nodes.rs`

- [ ] **Step 1: Write the failing passive-open tests**

```rust
#[test]
fn tcp_listen_syn_creates_syn_rcvd_without_immediate_accept() {}

#[test]
fn tcp_syn_rcvd_final_ack_completes_accept_and_publishes_established_lookup() {}
```

The first test must assert that a pure `SYN` queues a `SYN-ACK` reply and does **not** emit `AppCqeData::Accepted`.
The second test must assert that the final `ACK` transitions the half-open state to `ESTABLISHED` and only then completes `accept`.

- [ ] **Step 2: Run the passive-open tests to verify RED**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_listen_syn_creates_syn_rcvd_without_immediate_accept tcp_syn_rcvd_final_ack_completes_accept_and_publishes_established_lookup -- --exact`
Expected: FAIL because `TcpListenNode` currently forwards every listen hit directly to `TcpAcceptNode`.

- [ ] **Step 3: Change `TcpListenNode` from a blind forwarder into a passive-open state processor**

```rust
fn tcp_listen_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    snapshot: &TcpConnectionSnapshot,
    replies: &dyn TcpReplyWriter,
    accept_backend: &TcpAcceptBackendHandle,
) -> CoreResult<Option<NodeId>> {
    match snapshot.lookup_state(index)? {
        Some(state) if state.tcp_state == TcpState::SynRcvd => {
            process_syn_rcvd_final_ack(runtime, index, state, accept_backend)?;
            Ok(Some(drop_next))
        }
        _ => {
            process_listen_syn(runtime, index, replies)?;
            Ok(Some(drop_next))
        }
    }
}
```

- [ ] **Step 4: Expand the dispatch table so `SYN_RCVD` packets still land in the listen branch**

```rust
table.set(
    TcpState::SynRcvd,
    TcpInputFlags::ACK,
    TcpDispatchEntry::new(TcpInputNext::Listen, None),
);
```

Do not add a new graph next just for `SYN_RCVD`; keep the graph small and let `TcpListenNode` branch on the published connection state.

- [ ] **Step 5: Run the passive-open tests to verify GREEN**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_listen_syn_creates_syn_rcvd_without_immediate_accept tcp_syn_rcvd_final_ack_completes_accept_and_publishes_established_lookup -- --exact`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/input.rs crates/hammer-service/src/transport/tcp/state.rs crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/src/transport/tcp/accept.rs crates/hammer-service/src/service.rs crates/hammer-service/tests/tcp_input_nodes.rs crates/hammer-service/tests/tcp_state_nodes.rs
git commit -m "hammer-service(Feat): add passive-open syn-rcvd tcp path"
```

### Task 3: Active Open `SYN_SENT -> ESTABLISHED`

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-service/tests/app_tcp_connect_runtime.rs`
- Test: `crates/hammer-service/tests/tcp_state_nodes.rs`

- [ ] **Step 1: Write the failing active-open tests**

```rust
#[test]
fn tcp_syn_sent_syn_ack_promotes_connection_and_emits_final_ack() {}

#[test]
fn tcp_syn_sent_promotion_cancels_connect_timer_without_emitting_connect_cqe() {}
```

The second test must preserve the current phase-1 app behavior: no new "connected" CQE is introduced.

- [ ] **Step 2: Run the active-open tests to verify RED**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_syn_sent_syn_ack_promotes_connection_and_emits_final_ack tcp_syn_sent_promotion_cancels_connect_timer_without_emitting_connect_cqe -- --exact`
Expected: FAIL because `TcpSynSentNode` currently only records an observation and drops the packet.

- [ ] **Step 3: Implement real `SYN_SENT` receive-path validation**

```rust
fn process_syn_sent_syn_ack(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    state: &mut TcpRxConnectionState,
    shared: &SharedTcpControlPlane,
    replies: &dyn TcpReplyWriter,
) -> CoreResult<()> {
    let segment = TcpParsedSegment::from_index(runtime, index)?;
    validate_syn_sent_ack(state, &segment)?;
    state.tcp_state = TcpState::Established;
    state.snd_una = TcpSeq::new(segment.ack_number);
    state.rcv_nxt = TcpSeq::new(segment.sequence_number.wrapping_add(1));
    replies.send_ack(runtime, index, state)?;
    shared.apply(TcpControlPlaneAction::CancelTimer {
        connection_id: state.connection_id,
        kind: TcpTimerKind::Connect,
    })?;
}
```

- [ ] **Step 4: Publish the promoted established state back into service-owned lookup and snapshot state**

```rust
fn promote_pending_syn_sent_connection(
    &mut self,
    lookup_id: TcpLookupId,
    local: SocketAddr,
    remote: SocketAddr,
) -> HammerResult<()> {
    // keep the existing registration update path,
    // but populate the new receive-path fields too.
}
```

- [ ] **Step 5: Run the active-open tests to verify GREEN**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_syn_sent_syn_ack_promotes_connection_and_emits_final_ack tcp_syn_sent_promotion_cancels_connect_timer_without_emitting_connect_cqe -- --exact`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/syn_sent.rs crates/hammer-service/src/transport/tcp/state.rs crates/hammer-service/src/service.rs crates/hammer-service/tests/app_tcp_connect_runtime.rs crates/hammer-service/tests/tcp_state_nodes.rs
git commit -m "hammer-service(Feat): add syn-sent tcp receive transitions"
```

### Task 4: Established Receive, ACK Validation, and FIN Handling

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-service/tests/app_tcp_runtime.rs`
- Test: `crates/hammer-service/tests/tcp_state_nodes.rs`

- [ ] **Step 1: Write the failing established-path tests**

```rust
#[test]
fn tcp_established_in_order_ack_payload_delivers_to_app_and_advances_rcv_nxt() {}

#[test]
fn tcp_established_out_of_window_segment_emits_challenge_ack_without_app_delivery() {}

#[test]
fn tcp_established_fin_transitions_to_close_wait_and_emits_shutdown_observed() {}
```

- [ ] **Step 2: Run the established-path tests to verify RED**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_established_in_order_ack_payload_delivers_to_app_and_advances_rcv_nxt tcp_established_out_of_window_segment_emits_challenge_ack_without_app_delivery tcp_established_fin_transitions_to_close_wait_and_emits_shutdown_observed -- --exact`
Expected: FAIL because `TcpEstablishedNode` is currently only a forwarder to `TcpRcvProcessNode`.

- [ ] **Step 3: Implement VPP-sized receive-path helpers inside `established.rs`**

```rust
fn tcp_segment_validate_minimal(state: &TcpRxConnectionState, segment: &TcpParsedSegment) -> SegmentDisposition;

fn tcp_rcv_ack_no_cc(state: &mut TcpRxConnectionState, segment: &TcpParsedSegment) -> CoreResult<()>;

fn tcp_rcv_fin(state: &mut TcpRxConnectionState, segment: &TcpParsedSegment) -> CoreResult<FinDisposition>;
```

These helpers must cover:

- ACK inside `[snd_una, snd_nxt]`
- reject out-of-window data with challenge ACK
- reject spurious in-window `SYN`
- `RST` closes immediately
- `FIN` moves `ESTABLISHED -> CLOSE_WAIT`

- [ ] **Step 4: Deliver only validated in-order payload into `TcpRcvProcessNode`**

```rust
if segment.payload_len > 0 && segment.sequence_number == state.rcv_nxt.raw() {
    state.rcv_nxt = state.rcv_nxt.advance(segment.payload_len as u32);
    mark_pending_tcp_app_ingress(index, state.lookup_id)?;
    replies.send_ack(runtime, index, state)?;
    return Ok(Some(rcv_process_next));
}
```

Do **not** implement full out-of-order buffering in this plan. For non-in-order payload, send ACK and drop.

- [ ] **Step 5: Run the established-path tests to verify GREEN**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_established_in_order_ack_payload_delivers_to_app_and_advances_rcv_nxt tcp_established_out_of_window_segment_emits_challenge_ack_without_app_delivery tcp_established_fin_transitions_to_close_wait_and_emits_shutdown_observed -- --exact`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/established.rs crates/hammer-service/src/transport/tcp/rcv_process.rs crates/hammer-service/src/transport/tcp/state.rs crates/hammer-service/src/service.rs crates/hammer-service/tests/app_tcp_runtime.rs crates/hammer-service/tests/tcp_state_nodes.rs
git commit -m "hammer-service(Feat): add established tcp receive processing"
```

### Task 5: Reply Synthesis and Egress Routing

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/reply.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/reset.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Test: `crates/hammer-service/tests/tcp_reply_nodes.rs`
- Test: `crates/hammer-service/tests/interface_control.rs`

- [ ] **Step 1: Write the failing reply tests**

```rust
#[test]
fn tcp_reply_builder_synthesizes_syn_ack_with_reversed_tuple_and_ack_number() {}

#[test]
fn tcp_reply_writer_routes_ack_through_registered_interface_output() {}
```

- [ ] **Step 2: Run the reply tests to verify RED**

Run: `cargo test -p hammer-service --test tcp_reply_nodes tcp_reply_builder_synthesizes_syn_ack_with_reversed_tuple_and_ack_number tcp_reply_writer_routes_ack_through_registered_interface_output -- --exact`
Expected: compile failure because `transport::tcp::reply` does not exist yet.

- [ ] **Step 3: Implement the reply builder and writer trait**

```rust
pub trait TcpReplyWriter: Send + Sync {
    fn send_syn_ack(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        state: &TcpRxConnectionState,
    ) -> CoreResult<()>;

    fn send_ack(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        state: &TcpRxConnectionState,
    ) -> CoreResult<()>;

    fn send_reset(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        metadata: &RouteMetadata,
    ) -> CoreResult<()>;
}
```

- [ ] **Step 4: Route emitted replies through the existing interface-output graph**

```rust
struct ServiceTcpReplyWriter {
    interface_output: InterfaceOutputHandle,
    tx_node: NodeId,
}
```

Allocate a new buffer with reversed metadata, prepend the IPv4/TCP headers, stamp the packet cursor, set egress interface from the published TCP state, and schedule the frame to the registered output node.

- [ ] **Step 5: Run the reply tests to verify GREEN**

Run: `cargo test -p hammer-service --test tcp_reply_nodes -- --nocapture`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/reply.rs crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/src/transport/tcp/syn_sent.rs crates/hammer-service/src/transport/tcp/established.rs crates/hammer-service/src/transport/tcp/reset.rs crates/hammer-service/src/service.rs crates/hammer-service/tests/tcp_reply_nodes.rs crates/hammer-service/tests/interface_control.rs
git commit -m "hammer-service(Feat): add tcp reply synthesis and egress routing"
```

### Task 6: Timer Expiry, Terminal Cleanup, and Shared-Control Sync

**Files:**
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-runtime/src/protocol/tcp/mod.rs`
- Modify: `crates/hammer-runtime/tests/tcp_control_plane.rs`
- Modify: `crates/hammer-runtime/tests/tcp_timers.rs`
- Test: `crates/hammer-service/tests/tcp_state_nodes.rs`

- [ ] **Step 1: Write the failing timer cleanup tests**

```rust
#[test]
fn tcp_connect_timer_expiry_reclaims_worker_snapshot_and_lookup() {}

#[test]
fn tcp_terminal_close_does_not_allow_late_state_change_to_revive_connection() {}
```

- [ ] **Step 2: Run the timer cleanup tests to verify RED**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_connect_timer_expiry_reclaims_worker_snapshot_and_lookup tcp_terminal_close_does_not_allow_late_state_change_to_revive_connection -- --exact`
Expected: FAIL because `RuntimeService::handle_tcp_worker_event` currently ignores `TimerExpired`.

- [ ] **Step 3: Handle `TimerExpired` in runtime service**

```rust
TcpWorkerEvent::TimerExpired {
    connection_id,
    timer_id: _,
    kind,
} => {
    if let Some(reason) = kind.close_reason_on_expiry() {
        self.handle_tcp_worker_event(TcpWorkerEvent::Closed {
            connection_id,
            reason,
        })
    } else {
        Ok(())
    }
}
```

Keep delayed-ACK and persist as non-terminal no-op events for now.

- [ ] **Step 4: Keep shared control-plane state and worker-owned snapshot teardown in lockstep**

```rust
fn remove_tcp_connection_by_connection_id(
    &mut self,
    connection_id: TcpConnectionId,
) -> HammerResult<Option<TcpConnectionRegistration>> {
    // remove registration
    // republish lookup
    // republish app ingress
    // republish worker-owned tcp snapshot
}
```

- [ ] **Step 5: Run the timer cleanup tests to verify GREEN**

Run: `cargo test -p hammer-service --test tcp_state_nodes tcp_connect_timer_expiry_reclaims_worker_snapshot_and_lookup tcp_terminal_close_does_not_allow_late_state_change_to_revive_connection -- --exact`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/service.rs crates/hammer-runtime/src/protocol/tcp/mod.rs crates/hammer-runtime/tests/tcp_control_plane.rs crates/hammer-runtime/tests/tcp_timers.rs crates/hammer-service/tests/tcp_state_nodes.rs
git commit -m "hammer-service(Fix): sync tcp timer expiry with data-plane state"
```

### Task 7: Final Verification

**Files:**
- Verify only

- [ ] **Step 1: Format the workspace**

Run: `cargo fmt --all`
Expected: no diff after formatting.

- [ ] **Step 2: Run focused hammer-runtime TCP tests**

Run: `cargo test -p hammer-runtime --test tcp_control_plane --test tcp_timers`
Expected: PASS.

- [ ] **Step 3: Run focused hammer-service TCP graph tests**

Run: `cargo test -p hammer-service --test tcp_input_nodes --test tcp_state_nodes --test tcp_reply_nodes --test tcp_reset_observer --test tcp_syn_sent_adapter --test app_tcp_runtime --test app_tcp_connect_runtime`
Expected: PASS.

- [ ] **Step 4: Run relevant service library tests that exercise projected shared control state**

Run: `cargo test -p hammer-service --lib runtime_service_`
Expected: PASS.

- [ ] **Step 5: Run interface-output coverage because TCP replies depend on it**

Run: `cargo test -p hammer-service --test interface_control`
Expected: PASS.

- [ ] **Step 6: Commit the verification-only cleanup if formatting changed anything**

```bash
git add crates/hammer-service crates/hammer-runtime
git commit -m "hammer-service(Debug): verify vpp tcp node completion"
```

## Deferred Follow-Up After This Plan

Do not start these in the same implementation run unless the user explicitly expands scope:

- full out-of-order queueing and SACK bookkeeping
- timestamp / PAWS validation
- RTT estimation and RTO backoff logic
- congestion control and pacing
- retransmit queue and data send scheduler
- zero-window / persist probing
- `TIME_WAIT` correctness beyond simple close-and-reclaim semantics

Those all belong to the future Hammer TCP transmit path, not this receive-graph completion plan.

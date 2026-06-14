# TCP Session And Feature Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Hammer's TCP dataplane by wiring TCP transport nodes to worker-local session state, then filling the core TCP protocol features needed for a usable local TCP stack.

**Architecture:** Control plane stays listener-only. The session layer owns generic worker-local runtime concerns: ready queue, timer wheel, app op rings, and protocol callback entry points through concrete `SessionQueueRuntime<TcpSessionProtocol>`. TCP transport nodes parse/classify/emit packets and call typed TCP session methods; authoritative TCP state lives in `session/protocol/tcp`, not in control-plane snapshots or app runtime.

**Tech Stack:** Rust 2024, `hammer-service` packet graph/session nodes, `hammer-runtime::app` opaque io_uring-style operation rings, `hammer-infra::{vec,map,timer_wheel}`, VPP `src/vnet/session/session_node.c`, `src/vnet/session/session.c`, `src/vnet/tcp/tcp_input.c`, and `src/vnet/tcp/tcp_output.c`.

---

## Current State

- Control-plane TCP connection/session state was removed. `RuntimeService` now exposes listener bind/close only.
- `hammer-runtime::app` already uses opaque `AppOpId`, optional `AppUserData`, SQE/CQE descriptors, buffer leases, and ring wakers. It does not need TCP/session concepts.
- `crates/hammer-service/src/session/` has a generic worker-local queue, timer wheel, ready queue, and concrete `SessionQueueRuntime<P>`.
- `crates/hammer-service/src/session/protocol/tcp/state.rs` owns TCP state fields: `iss`, `irs`, `snd_una`, `snd_nxt`, `snd_wnd`, `rcv_nxt`, `rcv_wnd`, retransmit queue, RTO, and congestion state.
- `TcpSessionProtocol` is registered concretely, but `handle_timer_expiry` and `handle_ready` are still no-op.
- TCP packet nodes are still mostly scaffolding: `TcpAcceptNode`, `TcpEstablishedNode`, `TcpRcvProcessNode`, and `TcpSynSentNode` clear/drop frames.
- `TcpInputNode` currently routes mostly by listener lookup and flag pattern. It does not resolve an existing packet tuple to worker-local session state.

## VPP Reference Points

- VPP `session_queue_node_fn` is the worker-side event node. It drains session/app events and dispatches transport callbacks; it is registered as `session-queue` in `/private/tmp/vpp_session_node.c:2033-2174`.
- VPP attaches transport and session directly in `session_alloc_for_connection`: `s->connection_index = tc->c_index` and `tc->s_index = s->session_index` in `/private/tmp/vpp_session.c:488-503`.
- VPP listener setup publishes listener lookup; accepted children are transport/session state, not control-plane connection snapshots, in `/private/tmp/vpp_session.c:1463-1483`.
- VPP TCP input dispatches to `LISTEN`, `RCV_PROCESS`, `SYN_SENT`, `ESTABLISHED`, `RESET`, `PUNT`, and `DROP`; dispatch table setup is in `/private/tmp/vpp_tcp_input.c:3056-3285`.
- VPP listen path creates a child connection, initializes TCP vars, enters `SYN_RCVD`, attaches session, and sends SYN-ACK in `/private/tmp/vpp_tcp_input.c:2535-2687`.
- VPP receive path validates sequence/RST/SYN, validates ACK, enqueues in-order data, advances `rcv_nxt`, and programs ACK in `/private/tmp/vpp_tcp_input.c:207-331`, `/private/tmp/vpp_tcp_input.c:1031-1265`, and `/private/tmp/vpp_tcp_input.c:1436-1455`.
- VPP output emits SYN-ACK, ACK, RST, retransmit, and persist work from worker-local TCP context in `/private/tmp/vpp_tcp_output.c:805-828`, `/private/tmp/vpp_tcp_output.c:1011-1028`, and `/private/tmp/vpp_tcp_output.c:1325-1592`.

## Non-Negotiable Boundaries

- Do not reintroduce `AppBackend`, `AppIngressTarget`, `AppSessionBackend`, `AppTcpSessionBackend`, `SessionProtocolOps`, `TcpConnectionSnapshot`, `TcpConnectionRegistration`, `TcpSessionAccess`, `TcpOutputBackend`, `TcpAcceptBackend`, `TcpSynSentBackend`, or control-plane connection/session state.
- Do not put `SessionId`, TCP stream ids, TCP socket ids, listener ids, or transport state in `hammer-runtime::app`. App remains opaque `AppOpId` plus SQ/CQ.
- Do not add dyn registry, downcast, `Box<dyn SessionProtocolOps>`, or `PhantomData` for session protocol dispatch.
- Do not let control plane drive connection state, output, app completion, or timers. Control plane publishes listener lookup only.
- `SessionId` is allowed only under `crates/hammer-service/src/session/**`.
- TCP transport nodes may parse packets and emit packet buffers, but TCP state mutation goes through typed `TcpSessionProtocol` / `TcpSessionState` methods.

## Module Split For Parallel Agents

Use three agents max:

- **Agent A: Session Core** owns `crates/hammer-service/src/session/**` and `crates/hammer-runtime/src/app/**` tests only when proving app ring behavior. It must not edit `transport/tcp` packet nodes except test scaffolding.
- **Agent B: TCP Transport Nodes** owns `crates/hammer-service/src/transport/tcp/**` packet parsing, dispatch, listen/established/rcv-process/syn-sent/reset/output nodes. It can call `TcpSessionProtocol` typed APIs but should not design app ring ownership.
- **Agent C: TCP Feature Set + Integration** owns cross-cutting protocol feature tests, close/timer/retransmit behavior, `service.rs` graph wiring, and final cleanup. It starts after Agent A's session API and Agent B's transport dispatch contracts are stable.

Agents must not edit the same file concurrently. The coordination order is:

1. Agent A lands session API.
2. Agent B rebases and lands packet node wiring against that API.
3. Agent C rebases and lands feature completion plus integration wiring.

---

## Module 1: Session Core API And App Ring Ownership

**Owner:** Agent A

**Purpose:** Make `session/protocol/tcp` the authoritative owner of TCP sessions and app operation bindings. This module is independent from packet node logic; it should be testable with direct protocol/session runtime tests.

**Files:**
- Modify: `crates/hammer-service/src/session/protocol/tcp/state.rs`
- Modify: `crates/hammer-service/src/session/protocol/tcp/mod.rs`
- Modify: `crates/hammer-service/src/session/protocol/tcp/node.rs`
- Modify: `crates/hammer-service/src/session/worker.rs` only if a safe helper is needed to borrow `program` and `SessionProtocolContext` together.
- Modify tests: `crates/hammer-service/tests/tcp_connection_state.rs`
- Modify tests: `crates/hammer-service/tests/session_queue_node.rs`
- Create tests: `crates/hammer-service/tests/tcp_session_app_ring.rs`

**Deliverables:**

- [ ] Add `SessionId` and optional `AppOpId` to `TcpSessionState`.
- [ ] Extend `TcpSessionTable` indexes:
  - by `SessionId`
  - by `TcpLookupId`
  - by `TcpConnectionId`
  - by local/remote socket tuple
- [ ] Add remove and iteration helpers needed by timer/close code.
- [ ] Make `TcpSessionProtocol` own `TcpSessionTable`.
- [ ] Add typed session methods:
  - `alloc_session_id`
  - `insert_session`
  - `session_by_id`
  - `session_by_tuple`
  - `session_by_lookup_id`
  - `remove_session`
  - `mark_session_ready`
- [ ] Add app op/ring binding in `TcpSessionProtocol`:
  - session stores opaque `AppOpId`
  - protocol maps `AppOpId -> AppRingHandle`
  - protocol can complete recv CQEs and closed CQEs
  - protocol can synchronously drain send/close SQEs from the app ring
- [ ] Implement non-empty `handle_ready` and `handle_timer_expiry` entry points, even if feature-specific actions are initially delegated to placeholder typed methods.
- [ ] Keep all app-facing API opaque: `AppOpId`, optional `AppUserData`, descriptors, buffers. No app-side session ids.

**Required tests:**

- [ ] `tcp_session_table_resolves_by_session_id_lookup_id_connection_id_and_tuple`
- [ ] `tcp_session_protocol_owns_sessions_and_dispatches_ready_ids`
- [ ] `tcp_session_delivers_payload_to_pending_recv_cqe`
- [ ] `tcp_session_close_completes_closed_cqe`
- [ ] `tcp_session_drains_app_send_submission_without_async_backend`
- [ ] `tcp_session_timer_expiry_marks_session_ready`

**Implementation notes:**

- `TcpSessionState` should stay TCP-specific and remain under `session/protocol/tcp`.
- Use `hammer_infra::vec::Vec` and `hammer_infra::map::FlatHashTable` for session tables and dataplane-facing indexes.
- If app ring handles are not `FlatHashKey` values, keep a small `hammer_infra::vec::Vec<AppRingBinding>` and index by op id with `FlatHashTable<u64, usize>`.
- `AppUserData::new(0)` should not be used as a sentinel. Keep user data optional.
- Session runtime stays generic: `SessionQueueRuntime<P>`, not registry/dyn/downcast.

**Verification commands:**

```bash
cargo test -p hammer-service --test tcp_connection_state
cargo test -p hammer-service --test session_queue_node
cargo test -p hammer-service --test tcp_session_app_ring
```

**Commit:**

```bash
git add crates/hammer-service/src/session crates/hammer-service/tests/tcp_connection_state.rs crates/hammer-service/tests/session_queue_node.rs crates/hammer-service/tests/tcp_session_app_ring.rs
git commit -m "hammer-service(Refactor): make tcp protocol own session state"
```

---

## Module 2: TCP Transport Node Wiring

**Owner:** Agent B

**Purpose:** Make packet-side TCP nodes resolve packets to sessions and call typed TCP session operations. This module should not create app/backend abstractions and should not store authoritative TCP state in transport nodes.

**Files:**
- Create: `crates/hammer-service/src/session/protocol/tcp/packet.rs`
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/reply.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify tests: `crates/hammer-service/tests/tcp_input_nodes.rs`
- Create tests: `crates/hammer-service/tests/tcp_passive_open.rs`
- Create tests: `crates/hammer-service/tests/tcp_established_receive.rs`

**Deliverables:**

- [ ] Add reusable TCP packet parser helpers returning:
  - IP version
  - local/remote socket tuple
  - sequence number
  - optional ACK number
  - advertised window
  - flags
  - payload range/length
- [ ] `TcpInputNode` dispatch rules:
  - existing session tuple -> state-based next node
  - listener tuple -> listener owner handoff and `TcpListenNode`
  - bad listen ACK -> reset
  - no listener/session -> punt/reset/drop according to existing policy
- [ ] `TcpListenNode`:
  - pure SYN creates a TCP session through `TcpSessionProtocol`
  - initializes `irs`, `rcv_nxt`, `iss`, `snd_una`, `snd_nxt`, windows, owner worker
  - emits SYN-ACK
  - arms SYN-ACK retransmit timer through session context
- [ ] `TcpRcvProcessNode`:
  - handles `SYN_RCVD` final ACK
  - promotes session to `ESTABLISHED`
  - cancels SYN-ACK timer
  - handles close-state ACK/FIN/RST dispatch
- [ ] `TcpEstablishedNode`:
  - rejects invalid SEQ with ACK/challenge ACK
  - processes valid ACK into `snd_una`, retransmit queue release, congestion ACK sample
  - forwards in-order payload/FIN work to TCP session protocol
  - handles RST by closing/removing session and completing app close/reset signal
- [ ] `TcpSynSentNode`:
  - handles active-open `SYN|ACK`
  - validates ACK range
  - promotes session to `ESTABLISHED`
  - emits final ACK
  - handles RST/refused path
- [ ] `reply.rs`/`output.rs`:
  - provide packet emission helpers for SYN-ACK, ACK, RST, FIN-ACK, and data segments from `TcpSessionState`
  - track sequence-consuming output in session retransmit queue
  - update `snd_nxt` only through typed session methods

**Required tests:**

- [ ] `tcp_input_routes_existing_established_tuple_to_established_node`
- [ ] `tcp_input_handoffs_existing_session_to_owner_worker`
- [ ] `tcp_listen_syn_creates_syn_rcvd_session_and_emits_syn_ack`
- [ ] `tcp_syn_rcvd_final_ack_promotes_session_to_established`
- [ ] `tcp_established_in_order_payload_advances_rcv_nxt_and_completes_recv`
- [ ] `tcp_established_out_of_window_segment_emits_ack_without_advancing_rcv_nxt`
- [ ] `tcp_established_rst_closes_session`
- [ ] `tcp_syn_sent_valid_syn_ack_emits_final_ack_and_establishes`
- [ ] `tcp_syn_sent_rst_closes_half_open`

**Implementation notes:**

- Packet nodes can hold `SessionQueueHandle`/typed lookup handles, but must not own session tables.
- Do not resurrect `TcpAcceptNode` as a backend. If an accept node remains, it should be a thin packet graph step or be deleted.
- `TcpInputNext` should match the VPP shape already present: drop, punt, listen, rcv-process, syn-sent, established, reset.
- `TcpWorkerOwnedState` is listener lookup helper state. Do not grow it into connection/session state.
- If payload copy needs buffer APIs, keep helper functions in `session/protocol/tcp/packet.rs` or `transport/tcp/input.rs`, not app runtime.

**Verification commands:**

```bash
cargo test -p hammer-service --test tcp_input_nodes
cargo test -p hammer-service --test tcp_passive_open
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service --test tcp_reply_nodes
cargo test -p hammer-service --test tcp_output
```

**Commit:**

```bash
git add crates/hammer-service/src/session/protocol/tcp/packet.rs crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_input_nodes.rs crates/hammer-service/tests/tcp_passive_open.rs crates/hammer-service/tests/tcp_established_receive.rs
git commit -m "hammer-service(Feat): wire tcp transport nodes to sessions"
```

---

## Module 3: TCP Protocol Feature Completion

**Owner:** Agent C after Modules 1 and 2 land

**Purpose:** Fill TCP behavior beyond basic session attachment. This module should be implemented in feature groups, with focused tests before each group. It is okay to ship these as multiple commits.

**Files:**
- Modify: `crates/hammer-service/src/session/protocol/tcp/state.rs`
- Modify: `crates/hammer-service/src/session/protocol/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/options.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Create tests: `crates/hammer-service/tests/tcp_active_open.rs`
- Create tests: `crates/hammer-service/tests/tcp_close_states.rs`
- Create tests: `crates/hammer-service/tests/tcp_retransmit_timers.rs`
- Create tests: `crates/hammer-service/tests/tcp_options.rs`
- Create tests: `crates/hammer-service/tests/tcp_window_persist.rs`

### Feature Group 3.1: Active Open

- [ ] Implement active-open session creation API in `TcpSessionProtocol`.
- [ ] Emit SYN from session/output path.
- [ ] Arm connect/SYN retransmit timer.
- [ ] Handle `SYN_SENT -> ESTABLISHED` on valid `SYN|ACK`.
- [ ] Reject stray ACK and invalid SYN-ACK.
- [ ] Handle RST/refused and close app op.

Tests:

- [ ] `tcp_active_open_send_syn_tracks_half_open_session`
- [ ] `tcp_syn_sent_valid_syn_ack_establishes_and_sends_ack`
- [ ] `tcp_syn_sent_invalid_ack_sends_reset_or_drops`
- [ ] `tcp_syn_sent_rst_completes_closed`

### Feature Group 3.2: Close State Machine

- [ ] Implement `FIN_WAIT_1`, `FIN_WAIT_2`, `CLOSE_WAIT`, `LAST_ACK`, `CLOSING`, and `TIME_WAIT` transitions.
- [ ] App close SQE sends FIN and enters correct state.
- [ ] Peer FIN advances `rcv_nxt`, emits ACK, completes app recv with FIN flag or closed CQE.
- [ ] LAST_ACK final ACK removes session.
- [ ] TIME_WAIT timer removes session.

Tests:

- [ ] `tcp_app_close_sends_fin_and_enters_fin_wait_1`
- [ ] `tcp_peer_fin_in_established_enters_close_wait_and_completes_fin_recv`
- [ ] `tcp_last_ack_final_ack_removes_session`
- [ ] `tcp_time_wait_timer_removes_session`

### Feature Group 3.3: Retransmit, RTO, Persist, Delayed ACK

- [ ] RTO timer retransmits first unacked record and backs off RTO.
- [ ] ACK sample updates retransmit queue and congestion sample.
- [ ] Zero window starts persist timer.
- [ ] Persist timer emits probe without consuming new sequence.
- [ ] Delayed ACK timer emits ACK when no data is pending.

Tests:

- [ ] `tcp_retransmit_timer_reemits_first_unacked_segment_and_backs_off`
- [ ] `tcp_ack_releases_retransmit_record_and_updates_rto_sample`
- [ ] `tcp_zero_window_starts_persist_timer`
- [ ] `tcp_persist_timer_emits_window_probe`
- [ ] `tcp_delayed_ack_timer_emits_ack`

### Feature Group 3.4: Options And Receive Correctness

- [ ] Parse MSS/window scale/SACK permitted/timestamps from SYN and SYN-ACK.
- [ ] Emit negotiated MSS/window scale/SACK/timestamp options in SYN/SYN-ACK.
- [ ] Apply receive window scaling consistently.
- [ ] Add duplicate ACK handling.
- [ ] Add out-of-order policy. For now acceptable scope is "do not enqueue OOO, ACK current `rcv_nxt`"; full OOO queue can be a later module.
- [ ] Add challenge ACK for unacceptable ACK/SEQ where RFC/VPP does that.

Tests:

- [ ] `tcp_syn_options_negotiate_mss_window_scale_sack_timestamp`
- [ ] `tcp_output_syn_ack_includes_negotiated_options`
- [ ] `tcp_scaled_window_updates_snd_wnd`
- [ ] `tcp_out_of_order_payload_acknowledges_current_rcv_nxt`
- [ ] `tcp_unacceptable_ack_emits_challenge_ack`

**Implementation notes:**

- Use existing `TcpSessionOptionState` instead of inventing a new option store.
- Do not implement a full SACK scoreboard in this module unless tests and state shape are already stable. It belongs after basic out-of-order behavior.
- Congestion control state already exists; integrate ACK/loss signals only where output/retransmit behavior can actually use them.
- Keep timers worker-local through `SessionTimerWheel`.

**Verification commands:**

```bash
cargo test -p hammer-service --test tcp_active_open
cargo test -p hammer-service --test tcp_close_states
cargo test -p hammer-service --test tcp_retransmit_timers
cargo test -p hammer-service --test tcp_options
cargo test -p hammer-service --test tcp_window_persist
cargo test -p hammer-service --test tcp_congestion --test tcp_congestion_node
```

**Commits:**

```bash
git add crates/hammer-service/src/session/protocol/tcp crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_active_open.rs
git commit -m "hammer-service(Feat): complete tcp active open"

git add crates/hammer-service/src/session/protocol/tcp crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_close_states.rs
git commit -m "hammer-service(Feat): implement tcp close states"

git add crates/hammer-service/src/session/protocol/tcp crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_retransmit_timers.rs crates/hammer-service/tests/tcp_window_persist.rs
git commit -m "hammer-service(Feat): drive tcp retransmit and persist timers"

git add crates/hammer-service/src/session/protocol/tcp crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_options.rs
git commit -m "hammer-service(Feat): negotiate tcp options"
```

---

## Module 4: Service Graph Integration And Cleanup

**Owner:** Agent C after Modules 1 and 2 land

**Purpose:** Wire the real TCP graph into service/runtime, remove residual stubs/dead code, and prove the listener-only control plane boundary remains clean.

**Files:**
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/session/protocol/tcp/node.rs`
- Modify tests: `crates/hammer-service/tests/session_queue_node.rs`
- Modify tests: `crates/hammer-service/tests/tcp_input_nodes.rs`
- Modify tests: `crates/hammer-runtime/tests/tcp_control_plane.rs`

**Deliverables:**

- [ ] Install a real TCP graph on each data worker:
  - `TcpInputNode`
  - `TcpListenNode`
  - `TcpRcvProcessNode`
  - `TcpSynSentNode`
  - `TcpEstablishedNode`
  - `TcpResetNode`
  - `TcpSessionQueueNode` as driver node
- [ ] Pass `SessionQueueHandle`/typed session lookup handle into TCP packet nodes.
- [ ] Keep `RuntimeTcpListenerControlState` listener-only:
  - next lookup id
  - listener registrations
  - listener lookup snapshot
  - `TcpInputControlPlane`
  - no TCP session table
  - no sequence/window fields
  - no app ring/app op map
- [ ] Remove or implement all unconditional TCP stub drops.
- [ ] Delete unused shims after real receive logic exists:
  - `TcpReceiveProgress`
  - `apply_receive_progress`
  - helper functions used only by that shim
- [ ] Confirm `hammer-runtime::app` has no TCP/session/socket/listener concepts.
- [ ] Update old tests that expected immediate accept/drop behavior.

**Required tests:**

- [ ] `runtime_service_bind_tcp_listener_updates_listener_lookup`
- [ ] `runtime_service_close_tcp_listener_removes_listener_lookup`
- [ ] `runtime_service_control_plane_is_listener_only`
- [ ] `runtime_service_installs_tcp_graph_with_session_queue`
- [ ] Existing runtime app ring tests still pass.

**Cleanup scans:**

```bash
rg -n "AppBackend|AppIngressTarget|AppSessionBackend|AppTcpSessionBackend|SessionProtocolOps|TcpConnectionSnapshot|TcpConnectionRegistration|TcpSessionAccess|with_worker_tcp_session_protocol|downcast|Box<dyn|PhantomData|AppObjectRef::Session|AppObjectRef::Socket|AppSocketId|AppStreamId" crates/hammer-service/src crates/hammer-runtime/src/app crates/hammer-service/tests crates/hammer-runtime/tests
```

Expected: no output.

```bash
rg -n "frame\\.clear\\(\\)|let _ = session_id|let _ = expiry|TcpReceiveProgress|apply_receive_progress|unused_node_id\\(\\)" crates/hammer-service/src/session crates/hammer-service/src/transport/tcp crates/hammer-service/src/service.rs
```

Expected: no TCP node stubs or unused receive-progress shims. `frame.clear()` is acceptable only in true drop/test-only code.

**Verification commands:**

```bash
cargo test -p hammer-runtime --test app_ring --test tcp_control_plane
cargo test -p hammer-service --test session_queue_node --test tcp_input_nodes
cargo test -p hammer-service
cargo fmt --all
git diff --check
```

**Commit:**

```bash
git add crates/hammer-service/src crates/hammer-service/tests crates/hammer-runtime/tests/tcp_control_plane.rs
git commit -m "hammer-service(Refactor): wire tcp graph and remove residual stubs"
```

---

## Final Execution Order

- [ ] Run Module 1 first. It defines the typed session API.
- [ ] Run Module 2 in parallel only after Agent B has the Module 1 API contract, or use a temporary local trait-free shim that is removed before commit.
- [ ] Run Module 3 feature groups after basic session/transport packet flow passes.
- [ ] Run Module 4 after Modules 1 and 2, then again after Module 3 to catch new residual code.

## Final Verification

```bash
cargo fmt --all
cargo test -p hammer-runtime --test app_ring --test tcp_control_plane
cargo test -p hammer-service
cargo test --workspace
git diff --check
rg -n "AppBackend|AppIngressTarget|AppSessionBackend|AppTcpSessionBackend|SessionProtocolOps|TcpConnectionSnapshot|TcpConnectionRegistration|TcpSessionAccess|with_worker_tcp_session_protocol|downcast|Box<dyn|PhantomData|AppObjectRef::Session|AppObjectRef::Socket|AppSocketId|AppStreamId" crates/hammer-service/src crates/hammer-runtime/src/app crates/hammer-service/tests crates/hammer-runtime/tests
```

Expected:

- All tests pass.
- Forbidden scan has no output.
- No generated iOS artifacts are committed.
- `target/` remains untracked/cleaned before final push if the user asks for cleanup.

## Self-Review

- Spec coverage: This plan covers session core, TCP transport node attachment, TCP protocol feature completion, service graph wiring, and cleanup boundaries.
- Parallelism: Modules are split by ownership so multiple agents can work without editing the same files at the same time.
- Boundary check: App stays opaque io_uring rings; control plane stays listener-only; TCP state stays in `session/protocol/tcp`; transport nodes stay packet-facing.

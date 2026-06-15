# TCP Session And Feature Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Hammer's TCP dataplane by wiring TCP transport nodes to worker-local session state, then filling the core TCP protocol features needed for a usable local TCP stack.

**Architecture:** Control plane stays listener-only. TCP protocol parsing and shared TCP protocol semantics live in `hammer-core::protocol::tcp`; `hammer-service` packet nodes consume those core parse results instead of owning parser logic. Congestion control is shared transport behavior under `hammer-service/src/transport/congestion/`, with TCP and future QUIC adapting their own packet/ACK/loss events into the same controller API. The session layer owns generic worker-local runtime concerns: ready queue, timer wheel, app op rings, and protocol callback entry points through concrete `SessionQueueRuntime<TcpSessionProtocol>`. TCP transport nodes classify/emit packets and call typed TCP session methods; authoritative per-session TCP runtime state lives in `session/protocol/tcp`, not in control-plane snapshots or app runtime.

**Tech Stack:** Rust 2024, `hammer-core::protocol::tcp` parsing/option primitives, `hammer-service` packet graph/session nodes, `hammer-service::transport::congestion` shared BBR controller, existing Hammer IP parsing and packet cursors, `etherparse` wrapped inside `hammer-core` for TCP header/slice parsing, `hammer-runtime::app` opaque io_uring-style operation rings, `hammer-infra::{vec,map,timer_wheel}`, VPP `src/vnet/session/session_node.c`, `src/vnet/session/session.c`, `src/vnet/tcp/tcp_input.c`, and `src/vnet/tcp/tcp_output.c`.

---

## Current State

- Control-plane TCP connection/session state was removed. `RuntimeService` now exposes listener bind/close only.
- `hammer-runtime::app` already uses opaque `AppOpId`, optional `AppUserData`, SQE/CQE descriptors, buffer leases, and ring wakers. It does not need TCP/session concepts.
- `crates/hammer-service/src/session/` has a generic worker-local queue, timer wheel, ready queue, and concrete `SessionQueueRuntime<P>`.
- `crates/hammer-service/src/session/protocol/tcp/state.rs` owns TCP state fields: `iss`, `irs`, `snd_una`, `snd_nxt`, `snd_wnd`, `rcv_nxt`, `rcv_wnd`, retransmit queue, RTO, and congestion state.
- `TcpSessionProtocol` is registered concretely, but `handle_timer_expiry` and `handle_ready` are still no-op.
- TCP packet nodes are still mostly scaffolding: `TcpAcceptNode`, `TcpEstablishedNode`, `TcpRcvProcessNode`, and `TcpSynSentNode` clear/drop frames.
- `TcpInputNode` currently routes mostly by listener lookup and flag pattern. It does not resolve an existing packet tuple to worker-local session state.
- The workspace already carries `etherparse`; do not build a new TCP parser. `hammer-core::protocol::tcp` should wrap the parser library and expose Hammer TCP segment/options/flags views. TCP nodes should reuse Hammer's IP parser/cursor plus core TCP parsing, with only packet-buffer cursor glue in service if needed.
- `crates/hammer-service/src/transport/tcp/congestion.rs` has TCP-local paced congestion code today. Treat it as source material to migrate, not as the final design, because congestion control must be shared under `transport/congestion`.

## VPP Reference Points

- VPP `session_queue_node_fn` is the worker-side event node. It drains session/app events and dispatches transport callbacks; it is registered as `session-queue` in `/private/tmp/vpp_session_node.c:2033-2174`.
- VPP attaches transport and session directly in `session_alloc_for_connection`: `s->connection_index = tc->c_index` and `tc->s_index = s->session_index` in `/private/tmp/vpp_session.c:488-503`.
- VPP listener setup publishes listener lookup; accepted children are transport/session state, not control-plane connection snapshots, in `/private/tmp/vpp_session.c:1463-1483`. Hammer maps this as: listener lookup is replicated control-plane data, while a SYN hitting that listener creates a child session on the current data worker.
- VPP TCP input dispatches to `LISTEN`, `RCV_PROCESS`, `SYN_SENT`, `ESTABLISHED`, `RESET`, `PUNT`, and `DROP`; dispatch table setup is in `/private/tmp/vpp_tcp_input.c:3056-3285`.
- VPP listen path creates a child connection, initializes TCP vars, enters `SYN_RCVD`, attaches session, and sends SYN-ACK in `/private/tmp/vpp_tcp_input.c:2535-2687`.
- VPP receive path validates sequence/RST/SYN, validates ACK, enqueues in-order data, advances `rcv_nxt`, and programs ACK in `/private/tmp/vpp_tcp_input.c:207-331`, `/private/tmp/vpp_tcp_input.c:1031-1265`, and `/private/tmp/vpp_tcp_input.c:1436-1455`.
- VPP output emits SYN-ACK, ACK, RST, retransmit, and persist work from worker-local TCP context in `/private/tmp/vpp_tcp_output.c:805-828`, `/private/tmp/vpp_tcp_output.c:1011-1028`, and `/private/tmp/vpp_tcp_output.c:1325-1592`.

## Non-Negotiable Boundaries

- Do not reintroduce `AppBackend`, `AppIngressTarget`, `AppSessionBackend`, `AppTcpSessionBackend`, `SessionProtocolOps`, `TcpConnectionSnapshot`, `TcpConnectionRegistration`, `TcpSessionAccess`, `TcpOutputBackend`, `TcpAcceptBackend`, `TcpSynSentBackend`, or control-plane connection/session state.
- Do not put `SessionId`, TCP stream ids, TCP socket ids, listener ids, or transport state in `hammer-runtime::app`. App remains opaque `AppOpId` plus SQ/CQ.
- Do not add dyn registry, downcast, `Box<dyn SessionProtocolOps>`, or `PhantomData` for session protocol dispatch.
- Do not let control plane drive connection state, output, app completion, or timers. Control plane publishes listener lookup only.
- Do not hand off a listener SYN to a "listener owner" worker. Listener lookup is control-plane-published data; the child TCP session is created by `TcpListenNode` on the current data worker.
- Do not add TCP protocol parsing in `hammer-service`. Existing IP parsing stays in `parse_ip_packet_with_chain_len`/`BufferPacketCursor`; TCP header/payload/options parsing belongs in `hammer-core::protocol::tcp`, backed by the existing parser library (`etherparse`) and core option state.
- Do not keep congestion control under `transport/tcp`. Congestion control belongs in a protocol-agnostic `transport/congestion` folder so TCP and future QUIC can share BBR/controller logic.
- `SessionId` is allowed only under `crates/hammer-service/src/session/**`.
- TCP transport nodes may invoke core TCP parsing, classify packets, and emit packet buffers, but TCP state mutation goes through typed `TcpSessionProtocol` / `TcpSessionState` methods.

## Module Split For Parallel Agents

Use three agents max:

- **Agent A: Session Core** owns `crates/hammer-service/src/session/**` and `crates/hammer-runtime/src/app/**` tests only when proving app ring behavior. It must not edit `transport/tcp` packet nodes except test scaffolding.
- **Agent B: TCP Core Parse + Transport Nodes** owns `crates/hammer-core/src/protocol/tcp/**` parsing helpers and `crates/hammer-service/src/transport/tcp/**` dispatch/listen/established/rcv-process/syn-sent/reset/output nodes. It can call `TcpSessionProtocol` typed APIs but should not design app ring ownership.
- **Agent C: Shared Congestion + TCP Feature Set + Integration** owns `crates/hammer-service/src/transport/congestion/**`, TCP close/timer/retransmit integration, `service.rs` graph wiring, and final cleanup. It starts after Agent A's session API and Agent B's transport dispatch contracts are stable.

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
- Modify: `crates/hammer-service/src/session/runtime.rs` only if a safe helper is needed to borrow `program` and `SessionProtocolContext` together.
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

## Module 2: TCP Core Parse And Transport Node Wiring

**Owner:** Agent B

**Purpose:** Move TCP segment/options parsing into `hammer-core`, then make packet-side TCP nodes resolve packets to sessions and call typed TCP session operations. This module should not create app/backend abstractions and should not store authoritative TCP state in transport nodes.

**Files:**
- Modify: `crates/hammer-core/Cargo.toml`
- Modify: `crates/hammer-core/src/protocol/tcp/mod.rs`
- Create: `crates/hammer-core/src/protocol/tcp/segment.rs`
- Create: `crates/hammer-core/src/protocol/tcp/options.rs`
- Create tests: `crates/hammer-core/tests/protocol_tcp_segment.rs`
- Modify or create only for packet-buffer glue: `crates/hammer-service/src/transport/tcp/segment.rs`
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

- [x] Move TCP option parsing out of `crates/hammer-service/src/transport/tcp/options.rs` into `hammer-core::protocol::tcp::options`:
  - expose parsed MSS/window scale/SACK/SACK blocks/timestamps/ECN
  - keep `TcpCapabilities` and `TcpNegotiatedOptions` in core
  - make service import core option parsing instead of owning `tcp_options_from_bytes`
  - delete or reduce service `options.rs` to output-option construction only if output code still needs a service-local builder
- [x] Add `hammer-core::protocol::tcp::segment` backed by `etherparse`:
  - add `etherparse = { workspace = true }` to `crates/hammer-core/Cargo.toml`
  - expose `ParsedTcpSegment<'a>` or `TcpSegmentView<'a>` from core
  - expose `TcpFlags` from core or move existing `TcpInputFlags` into core if it is protocol-level
  - parse TCP header length, ports, sequence, optional ACK, flags, advertised window, options slice, and payload slice
  - return typed parse errors for short header, bad data offset, and invalid TCP slice
  - do not parse IP addresses inside the TCP segment parser
- [x] Reuse the existing packet stack in service; do not write a new IP/TCP parser:
  - use `crate::net::ip::parse_ip_packet_with_chain_len`
  - use `hammer_adapter::BufferPacketCursor`
  - reuse cursor values set by IP local/input nodes
  - pass the TCP header/payload slice to `hammer_core::protocol::tcp::segment`
  - reuse existing TCP sequence/handshake/key types from `hammer_core::protocol::tcp`
- [x] Remove duplicated manual TCP header offset reads from service packet nodes. It is acceptable to keep tiny test packet builders, but production parsing should go through `hammer-core` TCP parsing.
- [x] If service duplicated TCP view logic needs cleanup, add only a thin packet-buffer glue wrapper in `transport/tcp/segment.rs`; it must borrow packet bytes/cursor and combine parsed IP metadata plus `hammer-core` TCP parse results, not reimplement IP/TCP parsing.
- [x] Core `TcpSegmentView` plus optional service packet glue expose:
  - IP version
  - local/remote socket tuple
  - sequence number
  - optional ACK number
  - advertised window
  - flags
  - payload range/length
  - TCP option bytes slice
- [x] `TcpInputNode` dispatch rules:
  - existing session tuple -> state-based next node
  - listener tuple -> `TcpListenNode` on the current worker
  - bad listen ACK -> reset
  - no listener/session -> punt/reset/drop according to existing policy
- [x] `TcpListenNode`:
  - pure SYN creates a TCP session through `TcpSessionProtocol`
  - does not handoff just because the listener was created by the control plane
  - chooses child session owner as the current data worker
  - initializes `irs`, `rcv_nxt`, `iss`, `snd_una`, `snd_nxt`, windows, owner worker
  - applies peer SYN options using the core TCP options parser
  - emits SYN-ACK
  - arms SYN-ACK retransmit timer through session context
- [x] `TcpRcvProcessNode`:
  - handles `SYN_RCVD` final ACK
  - promotes session to `ESTABLISHED`
  - cancels SYN-ACK timer
  - handles close-state ACK/FIN/RST dispatch
- [x] `TcpEstablishedNode`:
  - rejects invalid SEQ with ACK/challenge ACK
  - processes valid ACK into `snd_una`, retransmit queue release, congestion ACK sample
  - forwards in-order payload/FIN work to TCP session protocol
  - handles RST by closing/removing session and completing app close/reset signal
- [x] `TcpSynSentNode`:
  - handles active-open `SYN|ACK`
  - validates ACK range
  - promotes session to `ESTABLISHED`
  - emits final ACK
  - handles RST/refused path
- [x] `reply.rs`/`output.rs`:
  - provide packet emission helpers for SYN-ACK, ACK, RST, FIN-ACK, and data segments from `TcpSessionState`
  - track sequence-consuming output in session retransmit queue
  - update `snd_nxt` only through typed session methods

**Required tests:**

- [x] `core_tcp_segment_parses_header_ports_sequence_ack_flags_window_options_and_payload`
- [x] `core_tcp_segment_rejects_short_header_and_bad_data_offset`
- [x] `core_tcp_options_parse_mss_window_scale_sack_timestamp_ecn`
- [x] `tcp_input_routes_existing_established_tuple_to_established_node`
- [x] `tcp_input_handoffs_existing_session_to_owner_worker`
- [x] `tcp_input_routes_listener_syn_to_local_listen_node_without_handoff`
- [x] `tcp_listen_syn_creates_syn_rcvd_session_and_emits_syn_ack`
- [x] `tcp_syn_rcvd_final_ack_promotes_session_to_established`
- [x] `tcp_established_in_order_payload_advances_rcv_nxt_and_completes_recv`
- [x] `tcp_established_out_of_window_segment_emits_ack_without_advancing_rcv_nxt`
- [x] `tcp_established_rst_closes_session`
- [x] `tcp_syn_sent_valid_syn_ack_emits_final_ack_and_establishes`
- [x] `tcp_syn_sent_rst_closes_half_open`

**Implementation notes:**

- TCP parse/option semantics belong in `hammer-core`, not `hammer-service`. Service can own graph decisions, buffer cursors, and output buffer mutation, but not the reusable protocol parser.
- Packet nodes can hold `SessionQueueHandle`/typed lookup handles, but must not own session tables.
- Do not resurrect `TcpAcceptNode` as a backend. If an accept node remains, it should be a thin packet graph step or be deleted.
- `TcpInputNext` should match the VPP shape already present: drop, punt, listen, rcv-process, syn-sent, established, reset.
- `TcpWorkerOwnedState` must not become connection/session state. Prefer renaming/removing the "owner" wording if it only holds listener lookup publication.
- Listener lookup is control-plane-published data available on workers. It is not a reason to handoff a SYN.
- Handoff is only for an existing session whose tuple maps to a different `owner_worker`.
- If payload copy needs buffer APIs, keep helper functions in `transport/tcp/segment.rs` or `transport/tcp/input.rs`, not app runtime.

**Verification commands:**

```bash
cargo test -p hammer-core --test protocol_tcp_segment
cargo test -p hammer-service --test tcp_input_nodes
cargo test -p hammer-service --test tcp_passive_open
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service --test tcp_reply_nodes
cargo test -p hammer-service --test tcp_output
```

**Commit:**

```bash
git add crates/hammer-core/Cargo.toml crates/hammer-core/src/protocol/tcp crates/hammer-core/tests/protocol_tcp_segment.rs crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_input_nodes.rs crates/hammer-service/tests/tcp_passive_open.rs crates/hammer-service/tests/tcp_established_receive.rs
git commit -m "hammer-service(Feat): wire tcp transport nodes to sessions"
```

---

## Module 3: TCP Protocol Feature Completion

**Owner:** Agent C after Modules 1 and 2 land

**Purpose:** Fill TCP behavior beyond basic session attachment. This module should be implemented in feature groups, with focused tests before each group. It is okay to ship these as multiple commits.

**Files:**
- Modify: `crates/hammer-service/src/transport/mod.rs`
- Create: `crates/hammer-service/src/transport/congestion/mod.rs`
- Create: `crates/hammer-service/src/transport/congestion/bbr.rs`
- Create: `crates/hammer-service/src/transport/congestion/bandwidth.rs`
- Create: `crates/hammer-service/src/transport/congestion/min_max.rs`
- Create: `crates/hammer-service/src/transport/congestion/types.rs`
- Modify: `crates/hammer-service/src/session/protocol/tcp/state.rs`
- Modify: `crates/hammer-service/src/session/protocol/tcp/mod.rs`
- Modify or delete after moving parser to core: `crates/hammer-service/src/transport/tcp/options.rs`
- Delete or replace with compatibility re-export: `crates/hammer-service/src/transport/tcp/congestion.rs`
- Modify: `crates/hammer-service/src/transport/tcp/congestion_control.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Create tests: `crates/hammer-service/tests/transport_congestion_bbr.rs`
- Create tests: `crates/hammer-service/tests/tcp_active_open.rs`
- Create tests: `crates/hammer-service/tests/tcp_close_states.rs`
- Create tests: `crates/hammer-service/tests/tcp_retransmit_timers.rs`
- Create tests: `crates/hammer-service/tests/tcp_congestion_integration.rs`
- Create tests: `crates/hammer-service/tests/tcp_options.rs`
- Create tests: `crates/hammer-service/tests/tcp_window_persist.rs`

### Feature Group 3.1: Active Open

**Purpose:** Finish active open using the worker-local `TcpSessionQueue` that already exists. Current code can build a test-only `SynSent` connection, emit and retransmit SYN from `TcpSessionProtocol::handle_ready_session`, route `TcpState::SynSent` packets from `TcpInputNode`, and establish on a basic `SYN|ACK` in `TcpSynSentNode`. This feature group turns those pieces into a real service/runtime connect path with VPP-aligned half-open behavior: app connect completion, strict SYN-SENT ACK/RST handling, timer cleanup, and no TCP/session identifiers exposed through `hammer-runtime::app`.

**VPP mapping researched:**

- VPP `session_open_vc` calls `transport_connect`, allocates a half-open session with `session_alloc_for_half_open`, stores it in the app-worker half-open table, and publishes half-open lookup before establishment in `/private/tmp/vpp_session.c:1320-1356`.
- VPP `session_stream_connect_notify` removes half-open lookup, reports connect errors to the app, and on success allocates the established session, adds normal lookup, and sends `app_worker_connect_notify(..., SESSION_E_NONE, opaque)` in `/private/tmp/vpp_session.c:747-800`.
- VPP `tcp_connect` sets `TCP_STATE_SYN_SENT`, initializes send variables, and sends the initial SYN in `/private/tmp/vpp_tcp.c:847-850`.
- VPP `tcp46_syn_sent_inline` handles every `SYN_SENT` packet shape, rejects unacceptable ACKs with RST unless the packet already has RST, reports acceptable RST as `SESSION_E_REFUSED`, parses peer options, moves the half-open connection to the worker pool, transitions `SYN|ACK` to `ESTABLISHED`, notifies the app, estimates initial RTT, and sends the mandatory final ACK in `/private/tmp/vpp_tcp_input.c:1736-1980`.
- VPP `tcp_timer_retransmit_syn_handler` ignores stale SYN timers after the connection is no longer `SYN_SENT`, reports active-open timeout to the app, retransmits SYN, and backs off RTO in `/private/tmp/vpp_tcp_output.c:1480-1535`.

**Hammer mapping and deviations:**

- Hammer does not need VPP's separate half-open pool today. The active-open connection remains a worker-local `TcpConnectionState` stored directly in `TcpSessionQueue` with `TcpState::SynSent`; tuple lookup in `TcpSessionConnectionIndex` plays the role of VPP half-open lookup.
- Keep app API opaque. `hammer-runtime::app` may learn a generic completion kind such as `Connected`, but it must not learn `SessionId`, `TcpConnectionId`, TCP tuple keys, or transport state.
- Keep state mutation in `crates/hammer-service/src/transport/tcp/session.rs`, `connection.rs`, and `syn_sent.rs`. `TcpInputNode` only dispatches by tuple/state; it must not own active-open state.
- Do not implement simultaneous open in this feature group. VPP supports `SYN_SENT + SYN without ACK -> SYN_RCVD`, but this plan keeps it as an explicit follow-up because current Hammer tests and service API only require normal active open.

**Files:**
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-runtime/src/app/context.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify tests: `crates/hammer-runtime/tests/app_ring.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/reset.rs`
- Modify tests: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify tests: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify tests: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Create tests: `crates/hammer-service/tests/tcp_active_open.rs`

#### Task 3.1.1: Generic App Connect Completion

**Why:** VPP reports connect success with `app_worker_connect_notify(..., SESSION_E_NONE, opaque)`. Hammer's app ring currently has `Recv` and `Closed`; using `Closed` for successful connect would make active open ambiguous. Add a generic `Connected` CQE that carries only the opaque app op.

- [ ] **Step 1: Add the failing runtime ring test**

Add this test to `crates/hammer-runtime/tests/app_ring.rs`:

```rust
#[test]
fn app_ring_connected_completion_round_trips_descriptor() {
    let ring = AppRingHandle::new(4, 4);
    let op = AppOpId::new(9_001);

    ring.push_test_completion(AppCqe::connected(Some(AppUserData::new(44)), op))
        .expect("push connected completion");

    let completion = ring.pop_completion().expect("connected completion");
    assert_eq!(completion.user_data(), Some(AppUserData::new(44)));
    assert_eq!(completion.opcode(), AppOpcode::Nop);
    match completion.kind() {
        AppCqeKind::Connected { op: completed_op } => assert_eq!(*completed_op, op),
        other => panic!("expected connected completion, got {other:?}"),
    }

    let descriptor = AppCqe::connected(None, op)
        .descriptor()
        .expect("connected descriptor")
        .expect("connected descriptor present");
    assert_eq!(descriptor.result(), 0);
    assert_eq!(descriptor.flags(), AppCqeFlags::NONE);
    assert_eq!(descriptor.object(), AppObjectRef::Operation(op));
    assert_eq!(descriptor.payload(), AppCqeData::Connected);
}
```

- [ ] **Step 2: Run the failing runtime ring test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_ring_connected_completion_round_trips_descriptor
```

Expected: FAIL with unresolved `AppCqe::connected`, `AppCqeKind::Connected`, and `AppCqeData::Connected`.

- [ ] **Step 3: Implement `Connected` in `hammer-runtime::app`**

In `crates/hammer-runtime/src/app/ring.rs`, update the app completion data and kind:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppCqeData {
    None,
    Recv { data: AppDataAddr },
    Connected,
    Closed,
}
```

```rust
#[derive(Debug)]
pub enum AppCqeKind {
    Recv {
        op: AppOpId,
        recv: AppRecv,
        fin: bool,
    },
    Connected {
        op: AppOpId,
    },
    Closed {
        op: Option<AppOpId>,
    },
}
```

Update `AppCqeKind::opcode` so connected completions use `AppOpcode::Nop`:

```rust
impl AppCqeKind {
    #[inline]
    pub const fn opcode(&self) -> AppOpcode {
        match self {
            Self::Recv { .. } => AppOpcode::Recv,
            Self::Connected { .. } => AppOpcode::Nop,
            Self::Closed { .. } => AppOpcode::Close,
        }
    }
}
```

Add the constructor next to `AppCqe::recv` and `AppCqe::closed`:

```rust
impl AppCqe {
    #[inline]
    pub const fn connected(user_data: Option<AppUserData>, op: AppOpId) -> Self {
        Self::new(user_data, AppCqeKind::Connected { op })
    }
}
```

Update `cqe_into_descriptor`:

```rust
fn cqe_into_descriptor(cqe: AppCqe) -> AppCqeDescriptor {
    let user_data = cqe.user_data();
    match cqe.inner.kind {
        AppCqeKind::Recv { op, recv, fin } => {
            let data = recv.into_data_addr();
            AppCqeDescriptor::new(
                user_data,
                data.len() as i32,
                if fin {
                    AppCqeFlags::BUFFER.union(AppCqeFlags::FIN)
                } else {
                    AppCqeFlags::BUFFER
                },
                AppObjectRef::Operation(op),
                AppCqeData::Recv { data },
            )
        }
        AppCqeKind::Connected { op } => AppCqeDescriptor::new(
            user_data,
            0,
            AppCqeFlags::NONE,
            AppObjectRef::Operation(op),
            AppCqeData::Connected,
        ),
        AppCqeKind::Closed { op } => AppCqeDescriptor::new(
            user_data,
            0,
            AppCqeFlags::NONE,
            op.map_or(AppObjectRef::None, AppObjectRef::Operation),
            AppCqeData::Closed,
        ),
    }
}
```

Update `cqe_from_descriptor`:

```rust
fn cqe_from_descriptor(descriptor: AppCqeDescriptor, ring: &AppRingHandle) -> AppCqe {
    match descriptor.payload() {
        AppCqeData::None => AppCqe::new(descriptor.user_data(), AppCqeKind::Closed { op: None }),
        AppCqeData::Recv { data } => AppCqe::recv(
            descriptor.user_data(),
            op_from_completion_descriptor(descriptor),
            AppRecv::new(ring.clone(), data),
            descriptor.flags().contains(AppCqeFlags::FIN),
        ),
        AppCqeData::Connected => AppCqe::connected(
            descriptor.user_data(),
            op_from_completion_descriptor(descriptor),
        ),
        AppCqeData::Closed => AppCqe::new(
            descriptor.user_data(),
            AppCqeKind::Closed {
                op: match descriptor.object() {
                    AppObjectRef::Operation(op) => Some(op),
                    AppObjectRef::None => None,
                },
            },
        ),
    }
}
```

In `crates/hammer-runtime/src/app/context.rs`, add a generic completion helper next to `try_complete_closed_op`:

```rust
pub fn try_complete_connected_op(&self, op: AppOpId) -> HammerResult<bool> {
    let Some(owner_worker) = self.registered_op_owner(op)? else {
        return Ok(false);
    };
    if self.current_worker_index().ok() == Some(owner_worker) {
        let ring = self.local_ring_for_op(op)?;
        ring.try_push_completion(AppCqe::connected(None, op))?;
    } else {
        let app_context_id = self.id;
        let ring_capacity = self.ring_capacity;
        self.data_context
            .call_blocking_on_worker(owner_worker, move || {
                worker_app_ring(app_context_id, ring_capacity)
                    .try_push_completion(AppCqe::connected(None, op))
            })?;
    }
    Ok(true)
}
```

- [ ] **Step 4: Run the runtime ring test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_ring_connected_completion_round_trips_descriptor
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-runtime/src/app/ring.rs crates/hammer-runtime/src/app/context.rs crates/hammer-runtime/src/app/mod.rs crates/hammer-runtime/tests/app_ring.rs
git commit -m "hammer-runtime(Feat): add app connected completion"
```

#### Task 3.1.2: Public Active-Open Session API

**Why:** VPP opens a VC by allocating and indexing half-open state before the SYN-ACK arrives. Hammer should expose the same operation at the service/session boundary by creating a worker-local `TcpState::SynSent` session, indexing its tuple, binding the opaque app op, arming SYN retransmit, and marking it ready for the existing session queue driver.

- [ ] **Step 1: Add the failing integration test**

Create `crates/hammer-service/tests/tcp_active_open.rs` with this first test and support code:

```rust
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DataWorkerId, InternalNode, Node, NodeId, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpSegmentFlags, TcpSegmentView, tcp_options_from_bytes,
};
use hammer_service::session::{SessionQueueNext, SessionQueueNode};
use hammer_service::transport::tcp::{TcpActiveOpenRequest, TcpSessionProtocol};

const CLIENT_ISN: u32 = 81_000;

#[derive(Default)]
struct CaptureState {
    packets: std::vec::Vec<std::vec::Vec<u8>>,
}

struct CaptureNode {
    runtime_data: NodeRuntimeData,
}

impl CaptureNode {
    fn new(state: Arc<Mutex<CaptureState>>) -> Self {
        let mut states = capture_states().lock().expect("capture registry");
        let slot = states.len();
        states.push(state);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
        }
    }
}

impl Node for CaptureNode {
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal("capture node must use descriptor process"))
    }

    fn node_process(&self) -> NodeProcessFn {
        capture_process
    }

    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for CaptureNode {}

fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
    static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
}

fn capture_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = {
        let states = capture_states()
            .lock()
            .map_err(|_| CoreError::internal("capture registry poisoned"))?;
        Arc::clone(
            states
                .get(data.usize_word(0)?)
                .ok_or_else(|| CoreError::internal("capture state missing"))?,
        )
    };
    for index in frame.drain_pending() {
        let packet = runtime.copy_current_chain(index)?;
        state
            .lock()
            .map_err(|_| CoreError::internal("capture poisoned"))?
            .packets
            .push(packet.into_iter().collect());
        runtime.free_index(index);
    }
    Ok(NodeResult::drop())
}

#[test]
fn tcp_active_open_start_emits_syn_from_session_queue() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
        .expect("session queue");
    let local: SocketAddr = "192.0.2.10:50001".parse().expect("local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
    let capabilities = TcpCapabilities {
        max_segment_size: Some(1_200),
        window_scale: Some(7),
        sack: true,
        timestamps: false,
        ecn: false,
    };

    let capture = Arc::new(Mutex::new(CaptureState::default()));
    let output = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&capture)));
    let session_node = SessionQueueNode::new().expect("session queue node");
    session_node
        .attach_queue(
            handle,
            SessionQueueNext::from_node(output),
            TcpSessionProtocol::session_queue_dispatch_fn(),
        )
        .expect("attach tcp queue");
    let session_queue = runtime.nodes().register_driver(session_node);

    let _started = TcpSessionProtocol::start_active_open(
        handle,
        TcpActiveOpenRequest {
            app_op: None,
            app_ring: None,
            local,
            remote,
            iss: CLIENT_ISN,
            capabilities,
        },
    )
    .expect("start active open");

    runtime
        .schedule_empty_frame(session_queue)
        .expect("schedule session queue");
    assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

    let packets = &capture.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
    let segment = TcpSegmentView::parse(&packets[0]).expect("tcp segment");
    assert_eq!(segment.source_port(), local.port());
    assert_eq!(segment.destination_port(), remote.port());
    assert_eq!(segment.sequence_number(), CLIENT_ISN);
    assert!(segment.flags().contains(TcpSegmentFlags::SYN));
    assert_eq!(tcp_options_from_bytes(segment.options()).capabilities, capabilities);
}
```

Add this internal state test to the existing `#[cfg(test)] mod tests` in `crates/hammer-service/src/transport/tcp/session.rs`:

```rust
#[test]
fn tcp_active_open_start_initializes_indexes_and_arms_retransmit_timer() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue_for_test(TcpSessionQueue::new(
        worker,
        runtime.packet_buffers().clone(),
    ))
    .expect("register queue");
    let local: SocketAddr = "192.0.2.10:50001".parse().expect("local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");

    let started = TcpSessionProtocol::with_queue(handle, |queue| {
        queue.start_active_open(TcpActiveOpenRequest {
            app_op: None,
            app_ring: None,
            local,
            remote,
            iss: ACTIVE_OPEN_ISS,
            capabilities: TcpCapabilities::default(),
        })
    })
    .expect("start active open");

    TcpSessionProtocol::with_queue(handle, |queue| {
        let session = queue
            .session_state(started.session_id)
            .expect("active-open session");
        assert_eq!(session.state(), TcpState::SynSent);
        assert_eq!(session.connection_id(), Some(started.connection_id));
        assert_eq!(session.snd_una(), ACTIVE_OPEN_ISS);
        assert_eq!(session.snd_nxt(), ACTIVE_OPEN_ISS + 1);
        assert_eq!(session.rcv_nxt(), 0);
        assert!(session.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
        assert_eq!(queue.session_id_by_tuple(local, remote), Some(started.session_id));
        Ok(())
    })
    .expect("inspect active open");
}
```

- [ ] **Step 2: Run the failing integration test**

Run:

```bash
cargo test -p hammer-service --test tcp_active_open tcp_active_open_start_emits_syn_from_session_queue
cargo test -p hammer-service transport::tcp::session::tests::tcp_active_open_start_initializes_indexes_and_arms_retransmit_timer
```

Expected: FAIL with unresolved `TcpActiveOpenRequest`, `TcpActiveOpenStarted`, and `start_active_open`.

- [ ] **Step 3: Add active-open request types and initializer**

In `crates/hammer-service/src/transport/tcp/connection.rs`, add `initialize_active_open` next to `initialize_passive_open`:

```rust
impl TcpConnectionState {
    #[inline]
    pub fn initialize_active_open(&mut self, iss: u32) {
        self.iss = iss;
        self.irs = 0;
        self.snd_una = iss;
        self.snd_nxt = TcpSeq::new(iss).advance(1).raw();
        self.snd_wnd = self.effective_send_window(self.snd_wnd);
        self.rcv_nxt = 0;
        self.state = TcpState::SynSent;
    }
}
```

In `crates/hammer-service/src/transport/tcp/session.rs`, import app types for non-test builds:

```rust
use hammer_runtime::app::{AppOpId, AppRingHandle};
```

Add these request/result types near `TcpSessionQueue`:

```rust
pub(crate) struct TcpActiveOpenRequest {
    pub app_op: Option<AppOpId>,
    pub app_ring: Option<AppRingHandle>,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub iss: u32,
    pub capabilities: TcpCapabilities,
}

pub(crate) struct TcpActiveOpenStarted {
    pub session_id: SessionId,
    pub connection_id: TcpConnectionId,
}
```

Add `TcpSessionQueue::start_active_open`:

```rust
impl TcpSessionQueue {
    pub(crate) fn start_active_open(
        &mut self,
        request: TcpActiveOpenRequest,
    ) -> CoreResult<TcpActiveOpenStarted> {
        match (request.app_op, request.app_ring.as_ref()) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(CoreError::internal(
                    "active-open app op/ring must be both set or both absent",
                ));
            }
        }

        let mut connection = TcpConnectionState::new(
            None,
            self.worker(),
            TcpState::SynSent,
            request.local.port(),
            Some(request.local),
            request.remote,
        );
        connection.set_local_capabilities(request.capabilities);
        connection.initialize_active_open(request.iss);

        let session_id = self.insert_session(connection);
        let connection_id = TcpConnectionId::new(session_id.get());
        let connection = self
            .session_state_mut(session_id)
            .ok_or_else(|| CoreError::internal("active-open session missing after insert"))?;
        connection.set_connection_id(connection_id);
        let indexed = connection.clone();
        self.index_session(session_id, &indexed);

        if let (Some(op), Some(ring)) = (request.app_op, request.app_ring) {
            if !self.bind_session_app_ring(session_id, op, ring) {
                let _ = self.close_session(session_id)?;
                return Err(CoreError::internal("failed to bind active-open app ring"));
            }
        }

        self.arm_retransmit_timer(session_id, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
        self.mark_session_ready(session_id);
        Ok(TcpActiveOpenStarted {
            session_id,
            connection_id,
        })
    }
}
```

Add a public wrapper on `TcpSessionProtocol` so integration tests and later `RuntimeService` code do not need access to the private `TcpSessionQueue` type:

```rust
impl TcpSessionProtocol {
    pub fn start_active_open(
        handle: SessionQueueHandle,
        request: TcpActiveOpenRequest,
    ) -> CoreResult<TcpActiveOpenStarted> {
        Self::with_queue(handle, |queue| queue.start_active_open(request))
    }
}
```

Export the new API from `crates/hammer-service/src/transport/tcp/mod.rs`:

```rust
pub use session::{TcpActiveOpenRequest, TcpActiveOpenStarted, TcpSessionProtocol};
```

- [ ] **Step 4: Run the active-open API test**

Run:

```bash
cargo test -p hammer-service --test tcp_active_open tcp_active_open_start_creates_indexed_syn_sent_session_and_emits_syn
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/tests/tcp_active_open.rs
git commit -m "hammer-service(Feat): add tcp active open session api"
```

#### Task 3.1.3: SYN-SENT Success Path

**Why:** VPP accepts only an ACK that covers our SYN, converts half-open state to established state, notifies the app, resets the SYN retransmit timer, and emits the final ACK. Hammer already establishes and emits ACK; this task adds app success completion and explicit timer cleanup.

- [ ] **Step 1: Add the failing success-path test**

Extend `crates/hammer-service/tests/tcp_active_open.rs` with:

```rust
#[test]
fn tcp_syn_sent_valid_syn_ack_establishes_cancels_timer_sends_ack_and_completes_connect() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
        .expect("session queue");
    let local: SocketAddr = "192.0.2.10:50002".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let app_ring = hammer_runtime::app::AppRingHandle::new(4, 4);
    let app_op = hammer_runtime::app::AppOpId::new(9_101);
    let server_isn = 17_000;

    let _started = TcpSessionProtocol::start_active_open(
        handle,
        TcpActiveOpenRequest {
            app_op: Some(app_op),
            app_ring: Some(app_ring.clone()),
            local,
            remote,
            iss: CLIENT_ISN,
            capabilities: TcpCapabilities::default(),
        },
    )
    .expect("start active open");

    let output = Arc::new(Mutex::new(CaptureState::default()));
    let output_node = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&output)));
    let drop_node = runtime.nodes().register_internal(hammer_service::data_plane::DropNode::new());
    let syn_sent = runtime.nodes().register_internal(
        hammer_service::transport::tcp::TcpSynSentNode::new(
            hammer_service::transport::tcp::TcpSynSentNext::nodes(output_node, drop_node),
        )
        .with_session_queue(handle),
    );

    send_tcp_packet(
        &runtime,
        syn_sent,
        remote,
        local,
        server_isn,
        CLIENT_ISN + 1,
        TcpSegmentFlags::SYN | TcpSegmentFlags::ACK,
        TcpCapabilities::default(),
    );
    assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 2);

    let packets = &output.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
    let ack = TcpSegmentView::parse(&packets[0]).expect("final ack");
    assert_eq!(ack.source_port(), local.port());
    assert_eq!(ack.destination_port(), remote.port());
    assert_eq!(ack.sequence_number(), CLIENT_ISN + 1);
    assert_eq!(ack.acknowledgment_number(), Some(server_isn + 1));
    assert!(ack.flags().contains(TcpSegmentFlags::ACK));

    let completion = app_ring.pop_completion().expect("connect completion");
    match completion.kind() {
        hammer_runtime::app::AppCqeKind::Connected { op } => assert_eq!(*op, app_op),
        other => panic!("expected connected completion, got {other:?}"),
    }
}
```

Add this state-focused unit test to the existing `#[cfg(test)] mod tests` in `crates/hammer-service/src/transport/tcp/syn_sent.rs`:

```rust
#[test]
fn valid_syn_ack_establishes_and_cancels_retransmit_timer() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let handle = TcpSessionProtocol::register_queue(
        DataWorkerId::new(0),
        runtime.packet_buffers().clone(),
    )
    .expect("session queue");
    let session_id = insert_syn_sent_session(handle);
    TcpSessionProtocol::with_queue(handle, |queue| {
        queue.arm_retransmit_timer(session_id, 2)?;
        Ok(())
    })
    .expect("arm retransmit");

    let output_state = Arc::new(Mutex::new(CaptureState::default()));
    let output = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&output_state)));
    let drop = runtime.nodes().register_internal(DropNode::new());
    let syn_sent = runtime.nodes().register_internal(
        TcpSynSentNode::new(TcpSynSentNext::nodes(output, drop)).with_session_queue(handle),
    );

    send_packet(
        &runtime,
        syn_sent,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            SERVER_ISN,
            CLIENT_ISN + 1,
            SYN | ACK,
        ),
    );
    assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 2);

    TcpSessionProtocol::with_queue(handle, |queue| {
        let session = queue
            .session_state(session_id)
            .expect("established session");
        assert_eq!(session.state(), TcpState::Established);
        assert_eq!(session.snd_una(), CLIENT_ISN + 1);
        assert_eq!(session.irs(), SERVER_ISN);
        assert_eq!(session.rcv_nxt(), SERVER_ISN + 1);
        assert!(!session.tcp_timer_is_live(TcpConnectionTimerKind::Retransmit));
        Ok(())
    })
    .expect("inspect established session");
}
```

Add helper functions in the same test file:

```rust
fn send_tcp_packet(
    runtime: &DataPlaneRuntime,
    node: NodeId,
    remote: SocketAddr,
    local: SocketAddr,
    sequence: u32,
    acknowledgment: u32,
    flags: TcpSegmentFlags,
    capabilities: TcpCapabilities,
) {
    let frame = runtime.alloc_frame_index().expect("frame");
    let packet = tcp_packet(remote, local, sequence, acknowledgment, flags, capabilities);
    let buffer = runtime
        .alloc_index_with_bytes(tcp_metadata(remote, local), &packet)
        .expect("packet");
    stamp_tcp_cursor(runtime, buffer, &packet);
    runtime
        .get_frame_mut(frame)
        .expect("frame mut")
        .push_index(buffer)
        .expect("push packet");
    assert!(runtime.schedule_frame(node, frame).expect("schedule"));
}

fn tcp_metadata(remote: SocketAddr, local: SocketAddr) -> hammer_adapter::RouteMetadata {
    hammer_adapter::RouteMetadata {
        network: hammer_adapter::Network::Tcp,
        source: Some(hammer_adapter::SocksAddr::ip(remote.ip(), remote.port())),
        destination: Some(hammer_adapter::SocksAddr::ip(local.ip(), local.port())),
        ..hammer_adapter::RouteMetadata::default()
    }
}

fn stamp_tcp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    let header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
    let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    let tcp_header_len = ((packet[header_len + 12] >> 4) as usize) * 4;
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            hammer_adapter::BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, header_len)
                .with_transport_header(header_len, tcp_header_len)
                .with_transport_payload_offset(header_len + tcp_header_len),
        );
}

fn tcp_packet(
    remote: SocketAddr,
    local: SocketAddr,
    sequence: u32,
    acknowledgment: u32,
    flags: TcpSegmentFlags,
    capabilities: TcpCapabilities,
) -> std::vec::Vec<u8> {
    let remote_ip = match remote.ip() {
        IpAddr::V4(ip) => ip,
        IpAddr::V6(_) => panic!("test helper expects IPv4"),
    };
    let local_ip = match local.ip() {
        IpAddr::V4(ip) => ip,
        IpAddr::V6(_) => panic!("test helper expects IPv4"),
    };
    let mut tcp = vec![0u8; 60];
    let options_len = hammer_core::protocol::tcp::write_tcp_segment_header(
        &mut tcp,
        hammer_core::protocol::tcp::TcpSegmentHeader {
            source_port: remote.port(),
            destination_port: local.port(),
            sequence_number: sequence,
            acknowledgment_number: acknowledgment,
            flags,
            advertised_window: u16::MAX,
            capabilities,
        },
    )
    .expect("tcp header");
    tcp.truncate(options_len);

    let total_len = 20 + tcp.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&remote_ip.octets());
    packet[16..20].copy_from_slice(&local_ip.octets());
    packet[20..].copy_from_slice(&tcp);
    packet
}
```

- [ ] **Step 2: Run the failing success-path test**

Run:

```bash
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_valid_syn_ack_establishes_cancels_timer_sends_ack_and_completes_connect
```

Expected: FAIL because the existing `TcpSynSentNode` does not complete `Connected` and does not cancel the retransmit timer on success.

- [ ] **Step 3: Implement success completion and timer cleanup**

In `crates/hammer-service/src/session/app.rs`, add a connected completion helper:

```rust
pub fn complete_connected(&self, op: AppOpId) -> CoreResult<()> {
    let Some(ring) = self.ring.as_ref() else {
        return Ok(());
    };
    ring.try_push_completion(AppCqe::connected(None, op))
}
```

In `crates/hammer-service/src/session/runtime.rs`, add:

```rust
pub(crate) fn complete_connected(&self, id: SessionId) -> CoreResult<bool> {
    let Some(op) = self.session(id).and_then(SessionEntry::app_op) else {
        return Ok(false);
    };
    self.app.complete_connected(op)?;
    Ok(true)
}
```

In `crates/hammer-service/src/transport/tcp/session.rs`, add a queue wrapper:

```rust
pub(crate) fn complete_connected(&self, session_id: SessionId) -> CoreResult<bool> {
    self.driver.complete_connected(session_id)
}
```

In `crates/hammer-service/src/transport/tcp/syn_sent.rs`, after `connection.set_state(TcpState::Established)` and before enqueueing the final ACK, complete the active-open app op and cancel retransmit:

```rust
connection.set_state(TcpState::Established);
connection.tcp_timer_reset(TcpConnectionTimerKind::Retransmit);
let (allocated, _, _) = alloc_tcp_segment_for_connection(
    runtime.packet_buffers(),
    connection,
    packet.local,
    TcpSegmentFlags::ACK,
    0,
)?;
output_index = Some(allocated);
let indexed = connection.clone();
queue.index_session(session_id, &indexed);
queue.cancel_retransmit_timer(session_id);
queue.complete_connected(session_id)?;
```

Also tighten ACK acceptance for Hammer's no-TFO active open:

```rust
if !packet.flags.contains(TcpSegmentFlags::ACK)
    || acknowledgment != connection.snd_nxt()
{
    return Err(CoreError::internal("syn-sent ACK is unacceptable"));
}
```

- [ ] **Step 4: Run the success-path test**

Run:

```bash
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_valid_syn_ack_establishes_cancels_timer_sends_ack_and_completes_connect
```

Expected: PASS.

- [ ] **Step 5: Add and run the options test**

Add this test to `crates/hammer-service/tests/tcp_active_open.rs`:

```rust
#[test]
fn tcp_syn_sent_syn_ack_options_update_output_payload_len_and_window_scale() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
        .expect("session queue");
    let local: SocketAddr = "192.0.2.10:50003".parse().expect("local");
    let remote: SocketAddr = "198.51.100.30:443".parse().expect("remote");
    let server_isn = 18_000;

    let _started = TcpSessionProtocol::start_active_open(
        handle,
        TcpActiveOpenRequest {
            app_op: None,
            app_ring: None,
            local,
            remote,
            iss: CLIENT_ISN,
            capabilities: TcpCapabilities {
                max_segment_size: Some(1_200),
                window_scale: Some(7),
                sack: true,
                timestamps: false,
                ecn: false,
            },
        },
    )
    .expect("start active open");

    let output = Arc::new(Mutex::new(CaptureState::default()));
    let output_node = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&output)));
    let drop_node = runtime.nodes().register_internal(hammer_service::data_plane::DropNode::new());
    let syn_sent = runtime.nodes().register_internal(
        hammer_service::transport::tcp::TcpSynSentNode::new(
            hammer_service::transport::tcp::TcpSynSentNext::nodes(output_node, drop_node),
        )
        .with_session_queue(handle),
    );

    send_tcp_packet(
        &runtime,
        syn_sent,
        remote,
        local,
        server_isn,
        CLIENT_ISN + 1,
        TcpSegmentFlags::SYN | TcpSegmentFlags::ACK,
        TcpCapabilities {
            max_segment_size: Some(536),
            window_scale: Some(3),
            sack: true,
            timestamps: false,
            ecn: false,
        },
    );
    assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 2);

    let packets = &output.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
}
```

Add this options state unit test to `crates/hammer-service/src/transport/tcp/syn_sent.rs`:

```rust
#[test]
fn valid_syn_ack_options_update_output_payload_len_and_window_scale() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let handle = TcpSessionProtocol::register_queue(
        DataWorkerId::new(0),
        runtime.packet_buffers().clone(),
    )
    .expect("session queue");
    let session_id = TcpSessionProtocol::with_queue(handle, |queue| {
        let mut connection = TcpConnectionState::new(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            TcpState::SynSent,
            LOCAL_PORT,
            Some(local_addr()),
            remote_addr(),
        );
        connection.set_local_capabilities(TcpCapabilities {
            max_segment_size: Some(1_200),
            window_scale: Some(7),
            sack: true,
            timestamps: false,
            ecn: false,
        });
        connection.set_sequence_state(
            CLIENT_ISN,
            0,
            CLIENT_ISN,
            CLIENT_ISN + 1,
            u16::MAX as u32,
            0,
            u16::MAX as u32,
        );
        let session_id = queue.insert_session(connection);
        let indexed = queue.session_state(session_id).expect("session").clone();
        queue.index_session(session_id, &indexed);
        Ok(session_id)
    })
    .expect("insert session");

    let output_state = Arc::new(Mutex::new(CaptureState::default()));
    let output = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&output_state)));
    let drop = runtime.nodes().register_internal(DropNode::new());
    let syn_sent = runtime.nodes().register_internal(
        TcpSynSentNode::new(TcpSynSentNext::nodes(output, drop)).with_session_queue(handle),
    );

    send_packet(
        &runtime,
        syn_sent,
        tcp_packet_with_options(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            SERVER_ISN,
            CLIENT_ISN + 1,
            SYN | ACK,
            TcpCapabilities {
                max_segment_size: Some(536),
                window_scale: Some(3),
                sack: true,
                timestamps: false,
                ecn: false,
            },
        ),
    );
    assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 2);

    TcpSessionProtocol::with_queue(handle, |queue| {
        let session = queue.session_state(session_id).expect("session");
        assert_eq!(session.output_payload_len(), 536);
        assert_eq!(session.effective_send_window_scale(), 3);
        assert_eq!(session.effective_receive_window_scale(), 7);
        assert!(session.negotiated_options().sack);
        Ok(())
    })
    .expect("inspect session options");
}
```

Run:

```bash
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_syn_ack_options_update_output_payload_len_and_window_scale
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/session/app.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/transport/tcp/syn_sent.rs crates/hammer-service/tests/tcp_active_open.rs
git commit -m "hammer-service(Feat): complete tcp syn-sent success path"
```

#### Task 3.1.4: Invalid ACK And RST Behavior

**Why:** VPP follows RFC 793 SYN-SENT handling: unacceptable ACKs trigger RST unless the packet is already RST, acceptable RST reports refused, and malformed/non-SYN packets do not destroy the half-open state. Hammer currently returns an error to the drop path for invalid ACKs; it needs an explicit reset emission path for the unacceptable ACK case.

- [ ] **Step 1: Add reset synthesis helper for reuse**

Make `TcpResetNode`'s existing reset synthesis reusable without exposing node internals. In `crates/hammer-service/src/transport/tcp/reset.rs`, change:

```rust
fn tcp_synthesized_reset(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    metadata: &RouteMetadata,
) -> Option<TcpSynthesizedReset> {
```

to:

```rust
pub(crate) fn tcp_synthesized_reset(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    metadata: &RouteMetadata,
) -> Option<TcpSynthesizedReset> {
```

Add this allocator helper below it:

```rust
pub(crate) fn alloc_synthesized_tcp_reset(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<Option<BufferIndex>> {
    let metadata = runtime.metadata(index)?;
    let Some(reset) = tcp_synthesized_reset(runtime, index, &metadata) else {
        return Ok(None);
    };
    let reset_index = runtime
        .packet_buffers()
        .alloc_index_with_bytes(reset.metadata, &reset.packet)?;
    Ok(Some(reset_index))
}
```

- [ ] **Step 2: Add failing invalid ACK/RST tests**

Add these tests to `crates/hammer-service/tests/tcp_active_open.rs`:

```rust
#[test]
fn tcp_syn_sent_invalid_ack_emits_reset_and_keeps_half_open_when_not_rst() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
        .expect("session queue");
    let local: SocketAddr = "192.0.2.10:50004".parse().expect("local");
    let remote: SocketAddr = "198.51.100.40:443".parse().expect("remote");
    let started = start_test_active_open(handle, local, remote, None);
    let output = Arc::new(Mutex::new(CaptureState::default()));
    let syn_sent = install_syn_sent_node(&runtime, handle, Arc::clone(&output));

    send_tcp_packet(
        &runtime,
        syn_sent,
        remote,
        local,
        19_000,
        CLIENT_ISN,
        TcpSegmentFlags::ACK,
        TcpCapabilities::default(),
    );
    assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 2);

    let packets = &output.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
    let reset = TcpSegmentView::parse(&packets[0]).expect("reset");
    assert!(reset.flags().contains(TcpSegmentFlags::RST));
    assert_eq!(reset.sequence_number(), CLIENT_ISN);
    TcpSessionProtocol::with_queue(handle, |queue| {
        let session = queue.session_state(started.session_id).expect("half-open");
        assert_eq!(session.state(), TcpState::SynSent);
        assert!(session.tcp_timer_is_live(TcpConnectionTimerKind::Retransmit));
        Ok(())
    })
    .expect("inspect half-open");
}

#[test]
fn tcp_syn_sent_rst_with_acceptable_ack_removes_session_and_completes_refused() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
        .expect("session queue");
    let local: SocketAddr = "192.0.2.10:50005".parse().expect("local");
    let remote: SocketAddr = "198.51.100.50:443".parse().expect("remote");
    let app_ring = hammer_runtime::app::AppRingHandle::new(4, 4);
    let app_op = hammer_runtime::app::AppOpId::new(9_102);
    let started = start_test_active_open(handle, local, remote, Some((app_op, app_ring.clone())));
    let output = Arc::new(Mutex::new(CaptureState::default()));
    let syn_sent = install_syn_sent_node(&runtime, handle, Arc::clone(&output));

    send_tcp_packet(
        &runtime,
        syn_sent,
        remote,
        local,
        20_000,
        CLIENT_ISN + 1,
        TcpSegmentFlags::RST | TcpSegmentFlags::ACK,
        TcpCapabilities::default(),
    );
    assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 1);

    assert!(output.lock().unwrap().packets.is_empty());
    match app_ring.pop_completion().expect("refused completion").kind() {
        hammer_runtime::app::AppCqeKind::Closed { op } => assert_eq!(*op, Some(app_op)),
        other => panic!("expected closed completion, got {other:?}"),
    }
    TcpSessionProtocol::with_queue(handle, |queue| {
        assert!(queue.session_state(started.session_id).is_none());
        assert_eq!(queue.session_id_by_tuple(local, remote), None);
        Ok(())
    })
    .expect("inspect removed session");
}

#[test]
fn tcp_syn_sent_rst_with_unacceptable_ack_drops_without_removing_session() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
        .expect("session queue");
    let local: SocketAddr = "192.0.2.10:50006".parse().expect("local");
    let remote: SocketAddr = "198.51.100.60:443".parse().expect("remote");
    let started = start_test_active_open(handle, local, remote, None);
    let output = Arc::new(Mutex::new(CaptureState::default()));
    let syn_sent = install_syn_sent_node(&runtime, handle, Arc::clone(&output));

    send_tcp_packet(
        &runtime,
        syn_sent,
        remote,
        local,
        21_000,
        CLIENT_ISN,
        TcpSegmentFlags::RST | TcpSegmentFlags::ACK,
        TcpCapabilities::default(),
    );
    assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 1);

    assert!(output.lock().unwrap().packets.is_empty());
    TcpSessionProtocol::with_queue(handle, |queue| {
        let session = queue.session_state(started.session_id).expect("half-open");
        assert_eq!(session.state(), TcpState::SynSent);
        assert!(session.tcp_timer_is_live(TcpConnectionTimerKind::Retransmit));
        Ok(())
    })
    .expect("inspect half-open");
}
```

Add these helpers:

```rust
fn start_test_active_open(
    handle: hammer_service::session::SessionQueueHandle,
    local: SocketAddr,
    remote: SocketAddr,
    app: Option<(hammer_runtime::app::AppOpId, hammer_runtime::app::AppRingHandle)>,
) -> hammer_service::transport::tcp::TcpActiveOpenStarted {
    TcpSessionProtocol::with_queue(handle, |queue| {
        queue.start_active_open(TcpActiveOpenRequest {
            app_op: app.as_ref().map(|(op, _)| *op),
            app_ring: app.map(|(_, ring)| ring),
            local,
            remote,
            iss: CLIENT_ISN,
            capabilities: TcpCapabilities::default(),
        })
    })
    .expect("start active open")
}

fn install_syn_sent_node(
    runtime: &DataPlaneRuntime,
    handle: hammer_service::session::SessionQueueHandle,
    output: Arc<Mutex<CaptureState>>,
) -> NodeId {
    let output_node = runtime
        .nodes()
        .register_internal(CaptureNode::new(output));
    let drop_node = runtime.nodes().register_internal(hammer_service::data_plane::DropNode::new());
    runtime.nodes().register_internal(
        hammer_service::transport::tcp::TcpSynSentNode::new(
            hammer_service::transport::tcp::TcpSynSentNext::nodes(output_node, drop_node),
        )
        .with_session_queue(handle),
    )
}
```

- [ ] **Step 3: Run the failing invalid ACK/RST tests**

Run:

```bash
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_invalid_ack_emits_reset_and_keeps_half_open_when_not_rst
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_rst_with_acceptable_ack_removes_session_and_completes_refused
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_rst_with_unacceptable_ack_drops_without_removing_session
```

Expected: the invalid ACK reset test FAILS until `TcpSynSentNode` emits synthesized resets; the acceptable RST test FAILS until RST cleanup cancels retransmit cleanly; the unacceptable RST test may already pass if current drop behavior preserves state.

- [ ] **Step 4: Implement VPP/RFC SYN-SENT ACK and RST handling**

In `crates/hammer-service/src/transport/tcp/syn_sent.rs`, import the reset helper:

```rust
use super::reset::alloc_synthesized_tcp_reset;
```

Refactor `tcp_syn_sent_index` so it distinguishes four outcomes:

```rust
enum SynSentAction {
    Drop,
    Output(BufferIndex),
    Reset(BufferIndex),
    Remove(SessionId),
}
```

Use these rules inside the queue mutation:

```rust
let ack_present = packet.flags.contains(TcpSegmentFlags::ACK);
let ack_acceptable = ack_present && acknowledgment == connection.snd_nxt();

if ack_present && !ack_acceptable {
    if packet.flags.contains(TcpSegmentFlags::RST) {
        action = SynSentAction::Drop;
    } else {
        action = match alloc_synthesized_tcp_reset(runtime, index)? {
            Some(reset) => SynSentAction::Reset(reset),
            None => SynSentAction::Drop,
        };
    }
    return Ok(());
}

if packet.flags.contains(TcpSegmentFlags::RST) {
    queue.cancel_retransmit_timer(session_id);
    remove_session = Some(session_id);
    action = SynSentAction::Remove(session_id);
    return Ok(());
}

if !packet.flags.contains(TcpSegmentFlags::SYN) || !ack_present {
    action = SynSentAction::Drop;
    return Ok(());
}
```

When removing on acceptable RST, use `queue.cancel_retransmit_timer(session_id)` before `queue.close_session(session_id)?` so pending and active timer bits are cleared before the session is removed. Continue to complete the app op through the existing `close_session` path; this maps VPP `SESSION_E_REFUSED` to Hammer's current `Closed` CQE.

When `SynSentAction::Reset(reset_index)` is selected, enqueue `reset_index` to the output next and free the original input packet. When `SynSentAction::Drop` is selected, free the original input packet without removing the session or touching the timer.

- [ ] **Step 5: Run the invalid ACK/RST tests**

Run:

```bash
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_invalid_ack_emits_reset_and_keeps_half_open_when_not_rst
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_rst_with_acceptable_ack_removes_session_and_completes_refused
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_rst_with_unacceptable_ack_drops_without_removing_session
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/reset.rs crates/hammer-service/src/transport/tcp/syn_sent.rs crates/hammer-service/tests/tcp_active_open.rs
git commit -m "hammer-service(Fix): align tcp syn-sent reset handling"
```

#### Task 3.1.4: Timer And Retransmission Polishing

**Why:** VPP's active-open SYN timer does nothing after the connection leaves `SYN_SENT`, and timeout reports connect failure to the app. Hammer already suppresses output for non-`SynSent` states; this task locks that behavior down and ensures cancelled/stale expiries do not revive SYN emission.

- [ ] **Step 1: Add the stale timer test**

Add this test to `crates/hammer-service/tests/tcp_active_open.rs`:

```rust
#[test]
fn tcp_syn_sent_stale_retransmit_timer_after_established_does_not_reemit_syn() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
        .expect("session queue");
    let local: SocketAddr = "192.0.2.10:50007".parse().expect("local");
    let remote: SocketAddr = "198.51.100.70:443".parse().expect("remote");
    let started = start_test_active_open(handle, local, remote, None);

    TcpSessionProtocol::with_queue(handle, |queue| {
        queue.expire_timers_for_test(2).expect("force stale expiry");
        Ok(())
    })
    .expect("force timer expiry");

    let output = Arc::new(Mutex::new(CaptureState::default()));
    let syn_sent = install_syn_sent_node(&runtime, handle, Arc::clone(&output));
    send_tcp_packet(
        &runtime,
        syn_sent,
        remote,
        local,
        22_000,
        CLIENT_ISN + 1,
        TcpSegmentFlags::SYN | TcpSegmentFlags::ACK,
        TcpCapabilities::default(),
    );
    assert_eq!(runtime.run_ready_nodes().expect("establish"), 2);
    output.lock().unwrap().packets.clear();

    let session_node = SessionQueueNode::new().expect("session queue node");
    let output_node = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&output)));
    session_node
        .attach_queue(
            handle,
            SessionQueueNext::from_node(output_node),
            TcpSessionProtocol::session_queue_dispatch_fn(),
        )
        .expect("attach tcp queue");
    let session_queue = runtime.nodes().register_driver(session_node);
    runtime
        .schedule_empty_frame(session_queue)
        .expect("schedule session queue");
    assert_eq!(runtime.run_ready_nodes().expect("run stale timer"), 1);

    assert!(output.lock().unwrap().packets.is_empty());
    TcpSessionProtocol::with_queue(handle, |queue| {
        let session = queue.session_state(started.session_id).expect("session");
        assert_eq!(session.state(), TcpState::Established);
        assert!(!session.tcp_timer_is_live(TcpConnectionTimerKind::Retransmit));
        Ok(())
    })
    .expect("inspect session");
}
```

- [ ] **Step 2: Run the stale timer test**

Run:

```bash
cargo test -p hammer-service --test tcp_active_open tcp_syn_sent_stale_retransmit_timer_after_established_does_not_reemit_syn
```

Expected: PASS after Task 3.1.3 success-path timer cancellation. If it fails because `expire_timers_for_test` is test-only private, expose only a `#[cfg(test)]` helper on `TcpSessionQueue` already matching the existing `expire_timers_for_test` shape.

- [ ] **Step 3: Keep existing SYN retransmit unit tests**

Run the existing unit tests rather than moving them unless public API access is required:

```bash
cargo test -p hammer-service transport::tcp::session::tests::tcp_active_open_creates_syn_sent_session_and_emits_syn
cargo test -p hammer-service transport::tcp::session::tests::tcp_active_open_retransmit_timer_reemits_syn
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/tests/tcp_active_open.rs
git commit -m "hammer-service(Fix): suppress stale tcp syn retransmits"
```

#### Task 3.1.5: Input Dispatch For Active-Open Responses

**Why:** VPP dispatches half-open response packets to `tcp4-syn-sent`/`tcp6-syn-sent` through half-open lookup, not listener lookup. Hammer already has `session_input_entry` and maps `TcpState::SynSent` to `TcpInputNext::SynSent`; this task adds public integration coverage so regressions do not send active-open responses to listen/reset/drop.

- [ ] **Step 1: Add or update the dispatch test**

Add this test to `crates/hammer-service/tests/tcp_input_nodes.rs` if it does not already exist:

```rust
#[test]
fn tcp_input_routes_syn_sent_tuple_to_syn_sent_node() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
        .expect("session queue");
    let local: SocketAddr = "192.0.2.10:50008".parse().expect("local");
    let remote: SocketAddr = "198.51.100.80:443".parse().expect("remote");
    TcpSessionProtocol::with_queue(handle, |queue| {
        queue.start_active_open(TcpActiveOpenRequest {
            app_op: None,
            app_ring: None,
            local,
            remote,
            iss: 81_000,
            capabilities: TcpCapabilities::default(),
        })?;
        Ok(())
    })
    .expect("start active open");

    let drop = runtime.nodes().register_internal(DropNode::new());
    let listen = NodeId::new(101);
    let rcv_process = NodeId::new(102);
    let syn_sent = NodeId::new(103);
    let established = NodeId::new(104);
    let reset = NodeId::new(105);
    let punt = NodeId::new(106);
    let control = TcpInputControlPlane::new(TcpInputNext::nodes(
        drop,
        listen,
        rcv_process,
        syn_sent,
        established,
        reset,
        punt,
    ));
    let mut node = control.node().with_session_queue(handle);
    let packet = tcp_packet(
        remote,
        local,
        23_000,
        81_001,
        TcpSegmentFlags::SYN | TcpSegmentFlags::ACK,
        TcpCapabilities::default(),
    );
    let index = runtime
        .alloc_index_with_bytes(tcp_metadata(remote, local), &packet)
        .expect("packet");
    stamp_tcp_cursor(&runtime, index, &packet);
    let frame = runtime.alloc_frame_index().expect("frame");
    runtime
        .get_frame_mut(frame)
        .expect("frame mut")
        .push_index(index)
        .expect("push");

    assert_eq!(
        super::next_node_for_index(
            &runtime,
            index,
            &TcpInputSnapshot::new(),
            &TcpInputNext::nodes(drop, listen, rcv_process, syn_sent, established, reset, punt),
            None,
            Some(handle),
        )
        .expect("next"),
        Some(syn_sent)
    );
}
```

If `next_node_for_index` remains private to the module's unit tests, put this assertion in the existing `#[cfg(test)] mod tests` in `crates/hammer-service/src/transport/tcp/input.rs` instead, using the same setup. The behavior under test is the important part: existing `SynSent` tuple resolves to `TcpInputNext::SynSent` before listener lookup.

- [ ] **Step 2: Run the dispatch test**

Run:

```bash
cargo test -p hammer-service --test tcp_input_nodes tcp_input_routes_syn_sent_tuple_to_syn_sent_node
```

Expected: PASS if the test lives in integration tests. If it lives in the module unit tests instead, run:

```bash
cargo test -p hammer-service transport::tcp::input::tests::tcp_input_routes_syn_sent_tuple_to_syn_sent_node
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/input.rs crates/hammer-service/tests/tcp_input_nodes.rs
git commit -m "hammer-service(Test): cover tcp syn-sent input dispatch"
```

#### Task 3.1.6: Verification And Commit

- [ ] **Step 1: Run active-open focused tests**

```bash
cargo test -p hammer-service --test tcp_active_open
```

Expected: PASS.

- [ ] **Step 2: Run related TCP dispatch and receive tests**

```bash
cargo test -p hammer-service --test tcp_input_nodes
cargo test -p hammer-service --test tcp_established_receive
```

Expected: PASS.

- [ ] **Step 3: Run app ring completion coverage**

```bash
cargo test -p hammer-runtime --test app_ring app_ring_connected_completion_round_trips_descriptor
```

Expected: PASS.

- [ ] **Step 4: Run existing active-open unit coverage**

```bash
cargo test -p hammer-service transport::tcp::session::tests::tcp_active_open_creates_syn_sent_session_and_emits_syn
cargo test -p hammer-service transport::tcp::session::tests::tcp_active_open_retransmit_timer_reemits_syn
```

Expected: PASS.

- [ ] **Step 5: Format changed Rust code**

```bash
cargo fmt --all
```

Expected: exits 0 with no formatting errors.

- [ ] **Step 6: Commit final cleanup if formatting changed files**

```bash
git add crates/hammer-runtime/src/app crates/hammer-runtime/tests/app_ring.rs crates/hammer-service/src/session crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_active_open.rs crates/hammer-service/tests/tcp_input_nodes.rs
git commit -m "hammer-service(Feat): complete tcp active open"
```

### Feature Group 3.2: Close State Machine

- [ ] App close/write shutdown:
  - no unsent data: emit FIN, track FIN in retransmit queue, enter `FinWait1`
  - unsent data: mark FIN pending, send queued data first, then FIN
  - ACK of our FIN in `FinWait1` -> `FinWait2`
- [ ] Peer FIN handling:
  - only accept FIN at `SEG.SEQ + data_len == rcv_nxt`
  - advance `rcv_nxt` by 1
  - emit ACK
  - complete pending recv with `FIN` flag when appropriate
  - `Established + FIN -> CloseWait`
  - `FinWait1 + FIN before our FIN ACKed -> Closing`
  - `FinWait2 + FIN -> TimeWait`
- [ ] App close after `CloseWait`:
  - emit FIN
  - enter `LastAck`
  - final ACK of our FIN removes session
- [ ] `Closing`:
  - ACK of our FIN -> `TimeWait`
  - arm time-wait timer
- [ ] `TimeWait`:
  - duplicate FIN emits ACK and rearms timer
  - timer expiry removes session
- [ ] RST handling:
  - acceptable RST closes/removes session
  - completes app close/reset CQE
  - clears timers

Tests:

- [ ] `tcp_app_close_sends_fin_and_enters_fin_wait_1`
- [ ] `tcp_fin_wait_1_ack_of_fin_enters_fin_wait_2`
- [ ] `tcp_peer_fin_in_established_enters_close_wait_and_completes_fin_recv`
- [ ] `tcp_close_wait_app_close_sends_fin_and_enters_last_ack`
- [ ] `tcp_last_ack_final_ack_removes_session`
- [ ] `tcp_simultaneous_fin_enters_closing_then_time_wait`
- [ ] `tcp_time_wait_duplicate_fin_reacks_and_rearms_timer`
- [ ] `tcp_time_wait_timer_removes_session`

### Feature Group 3.3: Retransmit, RTO, Persist, Delayed ACK

- [ ] Retransmit queue:
  - track SYN, FIN, and data records with sequence range and sent timestamp
  - ACK releases fully covered records
  - partial data ACK trims or splits the first record only if data segmentation supports it; otherwise keep segment-sized records
- [ ] RTO:
  - first valid non-retransmitted ACK updates SRTT/RTTVAR/RTO
  - retransmitted records suppress the next ambiguous RTT sample
  - RTO timer retransmits first unacked record
  - exponential backoff clamps at max RTO
  - retry exhaustion closes session with retransmit timeout
- [ ] Persist / zero window:
  - peer advertised zero window stops normal data send
  - arm persist timer while send queue has data
  - persist probe uses `snd_una - 1` or one byte probe according to available queued data
  - probe does not advance `snd_nxt` as new data
  - non-zero window ACK cancels persist and marks session ready
- [ ] Delayed ACK:
  - in-order payload may arm delayed ACK instead of immediate ACK
  - FIN/RST/state transitions ACK immediately
  - delayed ACK timer emits ACK and clears pending flag

Tests:

- [ ] `tcp_retransmit_timer_reemits_first_unacked_segment_and_backs_off`
- [ ] `tcp_ack_releases_retransmit_record_and_updates_rto_sample`
- [ ] `tcp_retransmitted_segment_suppresses_ambiguous_rtt_sample`
- [ ] `tcp_retransmit_retry_exhaustion_closes_session`
- [ ] `tcp_zero_window_starts_persist_timer`
- [ ] `tcp_persist_timer_emits_window_probe`
- [ ] `tcp_nonzero_window_ack_cancels_persist_timer`
- [ ] `tcp_delayed_ack_timer_emits_ack`

### Feature Group 3.4: Congestion Control

- [ ] Create shared transport congestion module:
  - `transport/congestion/types.rs` defines protocol-agnostic `CongestionAlgorithm`, `CongestionController`, `CongestionConfig`, `CongestionMetrics`, `AckedPacket`, `LostPacket`, `RttSample`, and `PacketNumber`
  - `transport/congestion/bbr.rs` implements `BbrController`
  - `transport/congestion/bandwidth.rs` implements delivery-rate sampling and max bandwidth filter
  - `transport/congestion/min_max.rs` implements the max-filter used for ACK aggregation
  - `transport/congestion/mod.rs` re-exports the public transport congestion API
  - `transport/mod.rs` publishes `pub mod congestion`
- [ ] Remove TCP-specific congestion ownership:
  - move existing `transport/tcp/congestion.rs` logic into `transport/congestion`
  - replace `TcpCongestionState` with `BbrController` or a protocol-agnostic `CongestionState`
  - keep `transport/tcp/congestion_control.rs` only as TCP adapter glue that looks up sessions and feeds shared controller events
  - do not keep algorithm types named `TcpCongestion*` unless they are strictly TCP adapter observations
- [ ] BBR API shape:
  - `BbrController::new(config: BbrConfig, now: Instant, max_datagram_size: u32)`
  - `on_packet_sent(now, packet_number, bytes, bytes_in_flight)`
  - `on_ack(now, AckedPacket { packet_number, bytes, sent_at, app_limited }, RttSample)`
  - `on_end_acks(now, bytes_in_flight, app_limited, largest_acked_packet)`
  - `on_loss(now, LostPacket { packet_number, bytes, sent_at }, persistent_congestion)`
  - `on_mtu_update(max_datagram_size)`
  - `send_window() -> u64`
  - `pacing_rate() -> Option<u64>`
  - `next_send_delay(pending_bytes) -> Option<Duration>`
  - `metrics() -> CongestionMetrics`
- [ ] BBR state to implement, based on the existing Quinn BBR shape in `third_party/quinn/quinn-proto/src/congestion/bbr/mod.rs`:
  - modes: `Startup`, `Drain`, `ProbeBw`, `ProbeRtt`
  - gains: high gain `2.885`, drain gain `1 / high_gain`, probe bandwidth gain cycle `[1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]`
  - initial window: `min(10 * max_datagram_size, max(2 * max_datagram_size, 14720))`, clamped by implementation maximum if needed
  - minimum window: `4 * max_datagram_size`
  - bandwidth max filter, min RTT filter, round counter, full-bandwidth detection after 3 rounds below 1.25 growth
  - ACK aggregation tracking
  - recovery state: `NotInRecovery`, `Conservation`, `Growth`
  - recovery window and loss accounting
  - ProbeRTT entry after min RTT expiry and exit after 200ms plus a round at low in-flight
- [ ] TCP adapter mapping:
  - TCP retransmit records carry a monotonically increasing congestion packet number in addition to TCP sequence range
  - TCP output calls `on_packet_sent` when SYN/FIN/data are emitted
  - cumulative ACK release from retransmit queue emits one or more `AckedPacket` samples
  - RTT sample comes from non-retransmitted records or timestamp echo; ambiguous retransmitted samples do not feed BBR
  - loss/RTO emits `LostPacket` or persistent-congestion signal to the controller
  - duplicate ACK fast-retransmit is TCP adapter logic, then feeds shared loss/recovery events
  - TCP send budget is `min(snd_wnd, congestion.send_window()) - bytes_in_flight`
  - pacing delay can defer ready output by setting TCP session `next_output_at`
- [ ] Future QUIC compatibility constraints:
  - no TCP sequence types in `transport/congestion`
  - no `TcpSessionState`, `TcpSeq`, `TcpLookupId`, or retransmit-queue references in shared controller code
  - packet number is a protocol-provided monotonic `u64`
  - QUIC/Hysteria can later feed Quinn-style ACK/loss packet metadata into the same API without adopting TCP state
- [ ] Keep VPP mapping explicit:
  - VPP ACK path computes `bytes_acked`, updates RTT/RTO, and invokes congestion events in `/private/tmp/vpp_tcp_input.c:345-447` and `/private/tmp/vpp_tcp_input.c:878-981`
  - VPP RTO path resets congestion/recovery and backs off timers in `/private/tmp/vpp_tcp_output.c:1272-1291` and `/private/tmp/vpp_tcp_output.c:1350-1413`
  - Hammer differs by using a shared transport BBR controller; TCP still places events on ACK/send/loss/RTO paths

Tests:

- [ ] `bbr_starts_in_startup_with_initial_window`
- [ ] `bbr_ack_samples_update_bandwidth_min_rtt_cwnd_and_pacing`
- [ ] `bbr_exits_startup_to_drain_after_three_rounds_without_full_bandwidth_growth`
- [ ] `bbr_drain_exits_to_probe_bw_when_inflight_reaches_bdp`
- [ ] `bbr_probe_bw_cycles_pacing_gain`
- [ ] `bbr_probe_rtt_caps_window_and_exits_after_timer_and_round`
- [ ] `bbr_loss_enters_recovery_and_limits_recovery_window`
- [ ] `bbr_app_limited_samples_do_not_inflate_bandwidth`
- [ ] `transport_congestion_has_no_tcp_types`
- [ ] `tcp_ack_processing_feeds_congestion_after_retransmit_release`
- [ ] `tcp_output_budget_uses_min_snd_wnd_and_cwnd_minus_in_flight`
- [ ] `tcp_output_pacing_sets_next_output_at`
- [ ] `tcp_duplicate_acks_mark_fast_retransmit_recovery`

### Feature Group 3.5: Options And Receive Correctness

- [ ] Options:
  - parse MSS/window scale/SACK permitted/timestamps/ECN with `hammer-core::protocol::tcp::options`
  - apply peer MSS to `output_payload_len`
  - negotiate send/receive window scale through `TcpSessionOptionState`
  - emit SYN/SYN-ACK options from local capabilities
  - timestamp receive stores latest TSval/TSecr if timestamps are negotiated
- [ ] Window/sequence acceptability:
  - implement RFC segment receive test for zero/non-zero receive window
  - valid in-order payload advances `rcv_nxt`
  - duplicate payload below `rcv_nxt` is not delivered and ACKs current `rcv_nxt`
  - out-of-order payload above `rcv_nxt` is not delivered initially and ACKs current `rcv_nxt`
  - FIN accepted only when all preceding data is accepted
- [ ] ACK correctness:
  - acceptable ACK range is `snd_una <= SEG.ACK <= snd_nxt`
  - ACK below `snd_una` is duplicate ACK
  - ACK above `snd_nxt` emits challenge ACK/drop and does not advance send state
  - duplicate ACK count is tracked for future fast retransmit but does not fake congestion behavior before retransmit support is ready
- [ ] Challenge ACK / reset policy:
  - unacceptable sequence in synchronized states emits ACK with current `snd_nxt/rcv_nxt`
  - unacceptable ACK in `SYN_RCVD` emits RST like VPP/RFC path
  - bad SYN on established session emits challenge ACK or reset according to current state
  - RST is accepted only if sequence is acceptable

Tests:

- [ ] `tcp_syn_options_negotiate_mss_window_scale_sack_timestamp`
- [ ] `tcp_output_syn_ack_includes_negotiated_options`
- [ ] `tcp_scaled_window_updates_snd_wnd`
- [ ] `tcp_zero_receive_window_accepts_only_exact_sequence_zero_len_segment`
- [ ] `tcp_duplicate_payload_is_not_delivered_and_reacks_rcv_nxt`
- [ ] `tcp_out_of_order_payload_acknowledges_current_rcv_nxt`
- [ ] `tcp_unacceptable_ack_emits_challenge_ack`
- [ ] `tcp_syn_rcvd_unacceptable_ack_emits_reset`
- [ ] `tcp_established_rst_with_unacceptable_sequence_is_ignored_or_challenge_acked`

**Implementation notes:**

- Use existing `TcpSessionOptionState` instead of inventing a new option store.
- `TcpSessionOptionState` can remain service session runtime state, but option parsing/option value structs belong in core.
- Congestion control algorithms and samples are shared transport behavior, so `transport/congestion` owns reusable BBR state transitions; TCP owns only the adapter that maps TCP retransmit/ACK/loss state into generic congestion events.
- Do not implement a full SACK scoreboard in this module unless tests and state shape are already stable. It belongs after basic out-of-order behavior.
- Keep timers worker-local through `SessionTimerWheel`.

**Verification commands:**

```bash
cargo test -p hammer-service --test transport_congestion_bbr
cargo test -p hammer-service --test tcp_active_open
cargo test -p hammer-service --test tcp_close_states
cargo test -p hammer-service --test tcp_retransmit_timers
cargo test -p hammer-service --test tcp_congestion_integration
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

git add crates/hammer-service/src/transport/congestion crates/hammer-service/src/transport/mod.rs crates/hammer-service/src/session/protocol/tcp crates/hammer-service/src/transport/tcp crates/hammer-service/tests/transport_congestion_bbr.rs crates/hammer-service/tests/tcp_congestion_integration.rs
git commit -m "hammer-service(Feat): add shared transport bbr congestion"

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

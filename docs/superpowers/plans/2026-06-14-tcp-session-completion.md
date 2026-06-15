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
- [x] `tcp_syn_sent_rst_closes_pending_syn_sent_session`

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

**Architecture boundary:** Active open is a TCP state-machine operation behind a stable Hammer
`SessionId`. Fuchsia netstack3's TCP code has a TCP-internal `State<I, R, S, ActiveOpen>`
enum whose variants wrap concrete TCP-owned state structs (`Closed<Error>`, `Listen`,
`SynSent<I, ActiveOpen>`, `Established<I, R, S>`), and transitions happen inside TCP methods.
That state enum is not a socket/session/app dispatch surface. Hammer should follow that
boundary in 3.1: `SessionDriverRuntime<S>` stores generic `S` and app binding only; it must
not grow workflow-specific session variants, a second session identity, a TCP
pre-establishment pool, or separate ready dispatch for pre-establishment sessions. Reworking
Hammer's internal `TcpState` enum into Fuchsia-style TCP-owned typed states is a later
TCP-state-machine refactor, not part of this session-state cleanup.

**VPP mapping:** VPP keeps SYN-SENT lookup separate from established connection
lookup, sends SYN from active-open TCP state, removes pending lookup on success/refusal, adds
established lookup only after a valid `SYN|ACK`, and notifies the app after the state-machine
result. Hammer maps this to a TCP-owned `TcpPendingIndex` whose value is the same stable
`SessionId`; it does not copy VPP's pre-establishment session pool into Hammer's generic
session layer.

- [x] Implement generic connected CQE support:
  - add `AppCqeData::Connected`, `AppCqeKind::Connected`, and `AppCqe::connected`
  - convert CQE descriptors through standard `From`/`TryFrom` style trait impls, not ad hoc
    conversion helper functions
  - add `SessionAppRuntime::complete_connected(op)` so only session/app runtime completes app ops
- [x] Keep session runtime generic:
  - one `Pool<SessionEntry<S>>`
  - one stable `SessionId`
  - `SessionEntry<S> { app_op: Option<AppOpId>, state: S }`
  - one generic `handle_ready_session(...)`
  - no second session identity, no app binding target enum, no ready queue item enum, no
    pending/established ready dispatch
  - no session-visible TCP state enum or phase discriminator
- [x] Implement `TcpSessionProtocol::connect(handle, local, remote) -> CoreResult<SessionId>`:
  - choose `iss` inside TCP
  - create `TcpConnectionState` with `TcpState::SynSent`
  - set `snd_una = iss`, `snd_nxt = iss + 1`, `rcv_nxt = 0`
  - insert the state as a normal session entry under the returned `SessionId`
  - add TCP pending tuple lookup for that same `SessionId`
  - arm the normal session retransmit timer and mark the same session ready
  - do not accept `iss`, capabilities, app ring, or TCP policy from the caller
- [x] Maintain TCP indexes incrementally:
  - delete the full-table index rebuild helper
  - remove old tuple/connection-id keys during `upsert`
  - remove exact keys during `remove_session`
  - add `TcpPendingIndex: tuple -> SessionId` for SYN-SENT demux only
- [x] Route SYN-SENT input through TCP pending lookup:
  - `tcp_input` checks established tuple lookup first and pending tuple lookup second
  - `tcp_syn_sent` uses `pending_id_by_tuple`, not `session_id_by_tuple`
  - valid `SYN|ACK` mutates the same session's TCP state to `Established`, removes pending
    lookup, inserts established lookup, sends the final ACK, and completes connected through
    session/app runtime
  - acceptable `RST|ACK` removes pending lookup, closes the same session, and completes closed
- [x] Replace remaining TCP one-off helpers:
  - replace all connection-specific segment allocation call sites with generic
    `alloc_tcp_segment(buffers, metadata, TcpSegmentHeader { ... })`
  - replace open-specific state initializers with generic setters at the transition site
  - do not add active-open request structs or reset-specific allocation helpers

Tests:

- [x] `app_ring_connected_completion_round_trips_descriptor`
- [x] `tcp_active_open_creates_syn_sent_session_and_emits_syn`
- [x] `tcp_active_open_retransmit_timer_reemits_syn`
- [x] `tcp_input_routes_pending_syn_sent_tuple_to_syn_sent_node`
- [x] `valid_syn_ack_emits_final_ack_and_establishes`
- [x] `rst_closes_pending_syn_sent_session`
- [x] `transport::tcp::session_index::tests`
- [x] structural check: session layer remains generic over `S`
- [x] cleanup check: no full index rebuild, no active-open request/reset special APIs, and no
  remaining TCP connection-specific segment allocation helper

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

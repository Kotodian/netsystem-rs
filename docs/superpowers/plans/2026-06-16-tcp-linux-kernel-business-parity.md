# TCP Linux Kernel Business Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring Hammer TCP business logic closer to modern Linux TCP where it matters for a user-space packet graph: app send, transmit scheduling, ACK/recovery, receive ordering, timers, close, and congestion-control integration.

**Architecture:** Keep the current Hammer typestate architecture: `TcpConnection<S, C>` is the typed TCP connection carrier, packet nodes use left-hand concrete connection types, and `TcpConnectionState<C>` is only erased session storage. Linux is used as the business reference, not as an API shape to copy; kernel socket/request/skb details are mapped into Hammer's session queue, packet graph, app ring, packet buffers, and `transport/congestion` boundaries.

**Tech Stack:** Rust 2024, `hammer-service` TCP packet nodes/session queue, `hammer-core::protocol::tcp`, `hammer_adapter` packet buffers, `hammer_infra::{vec,map,timer_wheel}`, current `transport/congestion::{CongestionController,BbrController}`, and Linux TCP mainline source at commit `6b5a2b7d9bc156e505f09e698d85d6a1547c1206`.

---

## Kernel Reference Scope

The Linux comparison is pinned to `torvalds/linux` commit `6b5a2b7d9bc156e505f09e698d85d6a1547c1206` from 2026-06-16. Do not use older cached summaries or downloaded source trees while executing this plan. Read required snippets from the upstream repository by URL or through a streaming command such as `curl -k -L <url> | rg ...`; do not save Linux source files into the workspace or `/private/tmp`.

Reference files and function families:

- `net/ipv4/tcp.c`: `tcp_sendmsg_locked`, `tcp_recvmsg_locked`, `tcp_cleanup_rbuf`, `tcp_shutdown`, `tcp_close`, zerocopy receive/send entry points.
- `net/ipv4/tcp_output.c`: `tcp_write_xmit`, `tcp_transmit_skb`, `tcp_cwnd_test`, `tcp_snd_wnd_test`, `tcp_nagle_test`, `tcp_tso_should_defer`, `tcp_mss_split_point`, `tcp_mtu_probe`, `tcp_schedule_loss_probe`, `tcp_send_loss_probe`, `tcp_send_fin`, `tcp_send_ack`, `tcp_send_delayed_ack`.
- `net/ipv4/tcp_input.c`: `tcp_ack`, `tcp_clean_rtx_queue`, `tcp_sacktag_write_queue`, `tcp_fastretrans_alert`, `tcp_validate_incoming`, `tcp_rcv_established`, `tcp_data_queue`, `tcp_data_queue_ofo`, `tcp_ofo_queue`, `tcp_dsack_set`, `tcp_ack_snd_check`, `tcp_fin`.
- `net/ipv4/tcp_recovery.c`: RFC 8985 RACK loss marking, reordering window, and reordering timeout.
- `net/ipv4/tcp_timer.c`: write timer dispatch for RTO/RACK/TLP/probe0, delayed ACK timer, keepalive timer, FIN-WAIT-2/TIME-WAIT behavior.
- `net/ipv4/tcp_minisocks.c`, `net/ipv4/tcp_ipv4.c`, `net/ipv4/syncookies.c`: request socket, SYN-RECV child creation, SYN backlog, syncookies, TIME-WAIT packet handling.
- `net/ipv4/tcp_cong.c`, `net/ipv4/tcp_bbr.c`, `include/net/tcp.h`: congestion-control ops, Reno fallback, BBR model, `rate_sample`, pacing, cwnd, app-limited samples, delivery counters.

## Current Hammer Anchors

- `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - Already has `TcpConnection<S, C>`, state marker types, `next_node()`, read-only protocol getters, timer flags, options, RTO estimator, congestion controller, and recovery state.
  - Already has typed protocol transitions such as `connect_state`, `accept_syn`, `accept_syn_ack`, `accept_payload`, `accept_fin`, and close-state helpers.
  - Missing real send/write queue state, real receive out-of-order state, SACK/DSACK state, and production connection between recovery records and output/ACK processing.

- `crates/hammer-service/src/transport/tcp/session.rs`
  - Already has `TcpServiceController = BbrController`, `TcpSessionQueue`, typed `take_connection<S>()`, app ring drain hooks, session timer token mapping, and typed event methods implemented on `TcpConnection<State, TcpServiceController>`.
  - Missing production consumption of `take_drained_sends()` and `take_drained_closes()`.
  - Timer dispatch currently mainly re-emits active-open control packets; it does not yet drive established send, delayed ACK, persist, pacing, RACK/TLP, keepalive, or TIME-WAIT behavior.

- `crates/hammer-service/src/transport/tcp/output.rs`
  - Currently validates TCP metadata and routes to lookup.
  - Has send-window helper math, but no `tcp_write_xmit` equivalent that drains a per-connection write queue.

- `crates/hammer-service/src/transport/tcp/recovery.rs`
  - Has an initial `TcpRecoveryState` using `hammer_infra::vec::Vec`, sent segment records, ACK/SACK hooks, RACK timeout, and TLP probe selection.
  - Production send path does not yet record sent payload/control segments into it.

- `crates/hammer-service/src/transport/congestion/**`
  - Already has transport-agnostic `CongestionController`, `BbrController`, BBR node sibling of `CongestionControlNode`, and packet/ACK/loss sample types.
  - TCP must not ask which congestion algorithm the connection carries. Left-hand connection types and sibling congestion nodes decide the concrete controller type.

## Linux TCP Capability Map

| Linux capability | Kernel shape | Why it matters | Hammer priority | Hammer landing |
| --- | --- | --- | --- | --- |
| App write ingestion | `tcp_sendmsg_locked` appends to write queue and pushes pending frames | Makes TCP usable from app ring sends | P0 | Drain `SessionAppSendSubmission` into TCP send queue and schedule ready output |
| Output scheduler | `tcp_write_xmit` gates by cwnd, peer window, Nagle/SWS, pacing, MTU, and small queues | Prevents blind packet emission and connects congestion to output | P0 | `TcpConnection<Established, C>` output event plus session queue ready dispatch |
| Packet transmit accounting | `tcp_transmit_skb`, `tcp_event_new_data_sent`, sent timestamps/delivery counters | Needed for RTT, RACK, TLP, BBR samples | P0 | Record every emitted data/FIN/SYN/SYN-ACK segment in TCP recovery/sent queue |
| ACK cleanup | `tcp_ack`, `tcp_clean_rtx_queue` | Frees retransmit records, advances `snd_una`, updates windows, generates samples | P0 | Established and close-state ACK handling consumes sent queue before updating congestion |
| SACK scoreboard | `tcp_sacktag_write_queue` | Modern loss recovery depends on SACK data, not only cumulative ACK | P0 | Parse SACK blocks from core packet view, feed recovery, mark RACK candidates |
| RACK | `tcp_rack_mark_lost`, `tcp_rack_reo_timeout` | Current best Linux loss detection baseline | P0 | Production RACK timer and sent-time-based loss marking |
| TLP | `tcp_schedule_loss_probe`, `tcp_send_loss_probe` | Improves tail-loss recovery before full RTO | P0 | TLP timer sends latest eligible segment/probe through same output path |
| Receive in-order path | `tcp_rcv_established`, `tcp_data_queue` | Current Hammer has only basic in-order receive | P0 | Preserve existing payload delivery, add ACK scheduling and receive-window accounting |
| Out-of-order receive | `tcp_data_queue_ofo`, `tcp_ofo_queue` | Necessary for real networks with reordering | P0 | TCP receive queue stores OOO packet buffers and drains when gaps close |
| SACK/DSACK emission | `tcp_dsack_set`, SACK block update in OOO path | Lets peer recover efficiently and detect spurious retransmits | P0/P1 | Emit SACK blocks in ACK headers; DSACK after duplicate/overlap detection |
| Delayed ACK | `tcp_ack_snd_check`, `tcp_send_delayed_ack`, delayed ACK timer | Reduces pure ACK load while maintaining progress | P0/P1 | Immediate ACK for gaps/FIN/dup/reordered; delayed ACK timer otherwise |
| Persist/probe0 | `tcp_probe_timer`, `ICSK_TIME_PROBE0` | Handles zero-window peers without hanging forever | P1 | Persist timer emits one-byte/zero-window probes from connection send queue |
| Pacing/TSQ concept | BBR pacing plus `tcp_small_queue_check`/internal pacing timer | Prevents bursts from user-space stack | P1 | Session timer-backed pacing event; congestion controller provides next send delay |
| Close path | `tcp_shutdown`, `tcp_send_fin`, `tcp_close_state`, close-state receive | App close must produce FIN and complete close-state transitions | P0 | Drain app closes into FIN output and typed close transitions |
| TIME-WAIT | `tcp_time_wait`, `tcp_timewait_state_process` | Avoids old duplicate segments corrupting new connections | P1 | Session timer-backed `TimeWait` retention and tuple handling |
| Listener request handling | request sockets, SYN backlog, SYN-ACK retransmit | Passive open needs flood-tolerant child state | P1 | Keep current session model, add bounded pending-open backlog and SYN-ACK retry policy |
| Syncookies | `syncookies.c` | Excellent under listener SYN pressure, but only after backlog exists | P2 | Stateless SYN-ACK option encoded in TCP service protocol, not in input node |
| PMTU/PLPMTUD | `tcp_mtu_probe`, ICMP frag-needed path | Avoids black holes and bad fixed MSS | P1/P2 | Start with MSS clamp and MTU update; add active probe later |
| ECN/AccECN | TCP ECN flags/options in input/output | Useful for modern congestion behavior | P2 | Negotiate and surface CE/ECE samples to congestion controller |
| Zerocopy send/receive | `MSG_ZEROCOPY`, `TCP_ZEROCOPY_RECEIVE`, dmabuf | Hammer app ring already has data leases; full ZC is a later optimization | P2 | Keep app ring opaque; avoid importing Linux socket semantics |
| Kernel-specific compatibility | repair, BPF hooks, MD5/AO, MPTCP, TLS offload, proc iterators, urgent data | Not the TCP business core for Hammer's current user-space stack | Excluded | Do not implement in this plan |

## Non-Negotiable Architecture Rules

- Keep `TcpConnection<S, C>` as the only typed TCP connection carrier.
- Keep packet nodes using left-hand concrete types:

```rust
let connection: TcpConnection<Established, TcpServiceController> =
    queue.take_connection(session_id)?;
connection.receive_established_packet(runtime, index, queue, session_id, &packet)?;
```

- Packet nodes must not match `TcpConnectionState`, branch on `TcpState`, ask which congestion algorithm is inside the connection, or decide whether a session is closed/pending/live.
- Connection typed event methods drive queue/index/timer/app side effects because they live in `session.rs` and can access `TcpSessionQueue` internals.
- `state_machine.rs` owns TCP protocol fields and real TCP protocol structures. It must not import app ring types or generic session runtime internals.
- `connection.rs` remains erased storage and timer-dispatch boundary. It must not grow general protocol getters/delegates on `TcpConnectionState<C>`.
- `transport/congestion` stays transport-agnostic. No TCP/session/app/QUIC-specific types enter the congestion controller trait or BBR implementation.
- Congestion algorithms are sibling implementations. Do not add algorithm enums, algorithm-name matches, default generic parameters such as `TcpConnection<S, C = BbrController>`, or node code that probes algorithm identity.
- Real protocol data structures are allowed when they represent TCP state: send queue, sent/retransmit queue, out-of-order receive queue, SACK/DSACK scoreboard, timer flags. Convenience wrapper outputs/dispositions are not allowed.
- Do not reintroduce `TcpOutputSendView`, `TcpStateMachine`, `TcpStateTransition`, `TcpStateMachineOutput`, `TcpAcceptedPayload`, `phase`, `with_state`, `enter_*`, `expect_*`, `as_*`, `map_*`, `process_*_packet`, or storage-policy queue APIs.
- Do not store connection-private wakeup `Instant`s as a replacement for session timers. Estimators may store measured times; runnable wakeups must be registered through the session timer wheel.

## File Responsibility Map

- `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - Add fields for real TCP protocol queues:
    - TCP send/write queue for app bytes waiting to be transmitted.
    - TCP sent/retransmit queue metadata for emitted sequence ranges.
    - TCP receive out-of-order queue and SACK/DSACK state.
  - Keep these fields private and mutated only by typed connection methods.
  - Add pure TCP helpers for send-window/cwnd eligibility, ACK acceptance, SACK/DSACK update, receive gap filling, FIN sequence accounting, and timer state updates.
  - Do not add packet-buffer allocation, app ring handling, session driver calls, or generic runtime side effects here.

- `crates/hammer-service/src/transport/tcp/send_queue.rs`
  - Create this file if the queue code makes `state_machine.rs` too large.
  - Responsibility: TCP write/sent queue mechanics equivalent to Linux write queue plus retransmit queue, using `hammer_infra::vec::Vec`.
  - Entries are real TCP sequence ranges and payload-buffer references. They are not state-machine transition wrappers.
  - Expose constructor methods and narrow mutation methods; no public fields.

- `crates/hammer-service/src/transport/tcp/receive_queue.rs`
  - Create this file for out-of-order receive, overlap trimming, SACK block generation, and DSACK bookkeeping.
  - Use `hammer_infra::vec::Vec` sorted by sequence range. Start with bounded linear insertion because Hammer currently lacks a TCP-scale rb-tree; keep the API independent so a tree can replace it later.
  - Store packet buffer indexes and payload ranges; session event code owns freeing or delivery.

- `crates/hammer-service/src/transport/tcp/recovery.rs`
  - Convert the current test-oriented RACK/TLP state into production integration.
  - Keep using `hammer_infra::vec::Vec`.
  - Remove any workaround that reconstructs vectors to hide missing operations; `hammer_infra::vec::Vec::remove`, `drain`, `truncate`, and `pop` exist and should be used directly.
  - Add sent-time, retransmitted, probe, SACKed, lost, and delivered accounting needed by Linux-style ACK cleanup and RACK.

- `crates/hammer-service/src/transport/tcp/segment.rs`
  - Add payload segment allocation using existing `DataPlaneBuffers` APIs.
  - Keep `alloc_tcp_segment` for pure control packets.
  - Add a builder that writes TCP header and appends/copies payload bytes into packet buffers without inventing a node-facing output carrier.

- `crates/hammer-service/src/transport/tcp/session.rs`
  - Drain app send and close submissions during ready-session handling.
  - For each drained submission, take the concrete typed connection by left-hand type, call the typed connection event method, and let that method update queue storage/index/timers/app state.
  - Register session timers for RTO, RACK, TLP, pacing, delayed ACK, persist, keepalive, and TIME-WAIT through existing token mapping.
  - Do not match on `TcpConnectionState<C>` from nodes or protocol code except inside the erased timer dispatch boundary in `connection.rs`.

- `crates/hammer-service/src/transport/tcp/{established,close_wait,fin_wait1,fin_wait2,closing,last_ack,time_wait}.rs`
  - Keep node code fixed to its state-specific left-hand connection type.
  - Node code may parse packet, resolve route/index, allocate returned control headers, enqueue emitted packet buffers, and free consumed input buffers.
  - Node code must not decide protocol next state, close/remove session, or choose congestion algorithm.

- `crates/hammer-service/src/transport/tcp/output.rs`
  - Remain a graph output validation/routing node.
  - Do not move per-connection send scheduling into this node. Scheduling belongs to typed connection/session events so it can see sequence, window, recovery, and congestion state.

- `crates/hammer-service/src/transport/congestion/**`
  - Add only transport-agnostic controller inputs if BBR/Reno/CUBIC need them.
  - Keep TCP-specific adaptation in `transport/tcp`.
  - Sibling congestion nodes may share common `CongestionControlNode` behavior; do not duplicate per-algorithm TCP node code.

## Implementation Modules

### Module 1: Linux-Style TCP Write Queue And App Send Intake

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Create: `crates/hammer-service/src/transport/tcp/send_queue.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Create tests: `crates/hammer-service/tests/tcp_send_queue.rs`
- Create tests: `crates/hammer-service/tests/tcp_app_send.rs`

**Kernel reference:** `tcp_sendmsg_locked`, `tcp_rate_check_app_limited`, Linux write queue append/collapse behavior.

- [ ] Step 1: Add failing send queue unit tests.

Test names:

```rust
#[test]
fn tcp_send_queue_pushes_app_bytes_in_sequence_order() {}

#[test]
fn tcp_send_queue_splits_by_effective_mss_without_advancing_snd_nxt() {}

#[test]
fn tcp_send_queue_keeps_unsent_bytes_after_partial_transmit() {}

#[test]
fn tcp_send_queue_releases_payload_buffer_after_full_ack() {}
```

Expected first run:

```bash
cargo test -p hammer-service --test tcp_send_queue
```

Expected result: fails because `transport::tcp::send_queue` does not exist.

- [ ] Step 2: Implement `send_queue.rs` as TCP state, not node glue.

Implementation shape:

```rust
#[derive(Debug, Clone)]
pub(crate) struct TcpSendQueue {
    writes: hammer_infra::vec::Vec<TcpQueuedWrite>,
    sent: hammer_infra::vec::Vec<TcpSentWrite>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpQueuedWrite {
    sequence: u32,
    end_sequence: u32,
    payload: hammer_adapter::BufferIndex,
    payload_offset: usize,
    payload_len: usize,
    transmitted: usize,
    fin: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpSentWrite {
    sequence: u32,
    end_sequence: u32,
    payload: hammer_adapter::BufferIndex,
    payload_offset: usize,
    payload_len: usize,
    packet_number: crate::transport::congestion::PacketNumber,
    sent_at: std::time::Instant,
    retransmitted: bool,
    sack_acked: bool,
    lost: bool,
    probe: bool,
}
```

Rules:

- All fields stay private or `pub(crate)` only where tests need construction through functions.
- Add constructors such as `TcpSendQueue::new()` and queue methods; do not manually construct queue entries in production code.
- Use `hammer_infra::vec::Vec`; do not use `std::vec::Vec` in TCP hot-path state.
- Do not store `AppSend`, `AppDataAddr`, or app ring handles in this queue. `session.rs` converts app sends into packet-buffer payload ownership before queueing.

- [ ] Step 3: Add `TcpConnection<Established, C>` app-send intake.

Allowed event shape in `session.rs`:

```rust
let connection: TcpConnection<Established, TcpServiceController> =
    queue.take_connection(session_id)?;
connection.send_app_data(runtime, queue, session_id, output, output_next, send, now)?;
```

The event method:

- reads/copies the app send payload through the existing app ring handle APIs available from `SessionAppSendSubmission`;
- stores payload in TCP-owned packet buffers or TCP-owned queued payload;
- updates `snd_nxt` only when bytes are actually emitted, not when app bytes are accepted;
- marks the session ready again when unsent bytes remain;
- releases app send ownership after TCP has accepted the data into its own queue.

- [ ] Step 4: Run focused tests.

```bash
cargo test -p hammer-service --test tcp_send_queue
cargo test -p hammer-service --test tcp_app_send
```

Expected result: pass.

### Module 2: Output Scheduler Equivalent To `tcp_write_xmit`

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/send_queue.rs`
- Modify: `crates/hammer-service/src/transport/tcp/segment.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify tests: `crates/hammer-service/tests/tcp_output.rs`
- Create tests: `crates/hammer-service/tests/tcp_send_output.rs`

**Kernel reference:** `tcp_write_xmit`, `tcp_cwnd_test`, `tcp_snd_wnd_test`, `tcp_nagle_test`, `tcp_mss_split_point`, `tcp_tso_should_defer`, `tcp_small_queue_check`.

- [ ] Step 1: Add failing output tests.

Test names:

```rust
#[test]
fn tcp_established_send_respects_peer_window_and_cwnd() {}

#[test]
fn tcp_established_send_splits_payload_by_mss() {}

#[test]
fn tcp_established_send_records_recovery_segment_and_advances_snd_nxt() {}

#[test]
fn tcp_established_send_defers_when_pacing_delay_is_active() {}

#[test]
fn tcp_established_send_keeps_unsent_tail_ready() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_send_output
```

Expected result: fails because app send does not emit payload TCP segments.

- [ ] Step 2: Add payload allocation in `segment.rs`.

Required behavior:

- Write TCP header using existing `write_tcp_segment_header`.
- Append payload bytes after the header.
- Preserve `tcp_segment_metadata(local, remote)`.
- Return `BufferIndex` directly, not a custom output wrapper.
- On any error, free the allocated buffer.

- [ ] Step 3: Implement send scheduler inside typed connection/session event.

The scheduler chooses bytes to emit using:

- peer send window from `snd_una`, `snd_nxt`, `snd_wnd`;
- congestion window from `C::congestion_window()`;
- negotiated effective MSS from TCP options;
- optional pacing delay from `C::next_send_delay(pending_bytes)`;
- basic Nagle/SWS policy as TCP connection state, not node policy.

Scheduling event shape:

```rust
connection.flush_send_queue(runtime, queue, session_id, output, output_next, now)?;
```

This method may allocate and enqueue packet buffers because it lives in `session.rs`. It calls pure state methods in `state_machine.rs` to select sequence ranges and mark sent ranges.

- [ ] Step 4: Register output-related timers through session.

When pacing delay exists, arm `TcpConnectionTimerKind::PACING`.

When unacked data exists, arm or refresh:

- `RETRANSMIT` for RTO;
- `TLP` for tail loss probe;
- `RACK` when RACK has pending reordering timeout.

Do not add connection-private output deadlines.

- [ ] Step 5: Run tests.

```bash
cargo test -p hammer-service --test tcp_output
cargo test -p hammer-service --test tcp_send_output
```

Expected result: pass.

### Module 3: ACK Cleanup, Rate Samples, And Congestion Integration

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/send_queue.rs`
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/congestion/types.rs` only if a transport-agnostic field is missing
- Create tests: `crates/hammer-service/tests/tcp_ack_recovery.rs`
- Modify tests: `crates/hammer-service/tests/tcp_rack_tlp.rs`
- Modify tests: `crates/hammer-service/tests/transport_congestion_bbr.rs`

**Kernel reference:** `tcp_ack`, `tcp_clean_rtx_queue`, `tcp_sacktag_write_queue`, `struct rate_sample`, BBR `bbr_main`.

- [ ] Step 1: Add failing ACK cleanup tests.

Test names:

```rust
#[test]
fn tcp_ack_advances_snd_una_and_removes_acked_sent_records() {}

#[test]
fn tcp_ack_observes_rtt_sample_for_original_transmission() {}

#[test]
fn tcp_ack_skips_rtt_sample_for_retransmitted_segment() {}

#[test]
fn tcp_sack_marks_sent_records_without_advancing_snd_una() {}

#[test]
fn tcp_ack_generates_transport_agnostic_bbr_sample() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_ack_recovery
```

Expected result: fails because production ACK handling only applies `snd_una`/window and does not clean sent records.

- [ ] Step 2: Make ACK processing consume sent queue first.

ACK processing order:

1. Validate ACK acceptability against `snd_una..=snd_nxt`.
2. Update send window from advertised window.
3. Process SACK blocks, if present, against sent queue.
4. Remove cumulative ACKed sent records.
5. Generate RTT and delivery samples from ACKed original transmissions.
6. Notify recovery/RACK about SACKed/lost state.
7. Notify congestion controller through `CongestionController`.
8. Re-arm or cancel RTO/RACK/TLP timers.

- [ ] Step 3: Keep controller interface transport-agnostic.

Allowed inputs are existing or generic transport terms:

```rust
AckedPacket
LostPacket
RttSample
PacketNumber
CongestionMetrics
```

If a new field is required, it must describe transport delivery generically, such as `delivered_bytes`, `ecn_ce`, or `app_limited`. Do not add TCP sequence numbers, SACK blocks, session ids, or app-ring types to `transport/congestion`.

- [ ] Step 4: Run tests.

```bash
cargo test -p hammer-service --test tcp_ack_recovery
cargo test -p hammer-service --test tcp_rack_tlp
cargo test -p hammer-service --test transport_congestion_bbr
```

Expected result: pass.

### Module 4: Production RACK, TLP, And RTO Timer Flow

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify tests: `crates/hammer-service/tests/tcp_rack_tlp.rs`
- Create tests: `crates/hammer-service/tests/tcp_timer_recovery.rs`

**Kernel reference:** `tcp_rack_mark_lost`, `tcp_rack_reo_timeout`, `tcp_schedule_loss_probe`, `tcp_send_loss_probe`, `tcp_retransmit_timer`, `tcp_write_timer_handler`.

- [ ] Step 1: Add failing timer tests.

Test names:

```rust
#[test]
fn tcp_rack_timer_marks_reordered_older_segment_lost() {}

#[test]
fn tcp_tlp_timer_reemits_latest_tail_segment_once() {}

#[test]
fn tcp_rto_retransmits_oldest_unacked_segment_and_backs_off() {}

#[test]
fn tcp_recovery_timers_cancel_when_sent_queue_is_empty() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_timer_recovery
```

Expected result: fails because established timers do not retransmit or mark loss in production.

- [ ] Step 2: Implement timer dispatch as connection event, not node policy.

Allowed erased boundary:

```rust
TcpConnectionState<C>::on_tcp_timer_expiry(...)
```

This boundary may dispatch to the typed connection internally because storage is erased there. Packet nodes do not do this dispatch.

Timer behavior:

- `RETRANSMIT`: retransmit oldest unacked sequence range, apply RTO backoff, skip invalid RTT sample for retransmitted range.
- `RACK`: mark elapsed candidates lost, notify congestion, schedule retransmit output.
- `TLP`: emit one tail probe if unacked data remains, then leave RTO armed.
- `PACING`: resume `flush_send_queue`.
- `DELAYED_ACK`: emit pending ACK.
- `PERSIST`: emit zero-window probe.
- `KEEP_ALIVE`: emit keepalive probe only when configured later in Module 11.
- `TIME_WAIT`: close the retained session.

- [ ] Step 3: Keep timer registration in session timer wheel.

`TcpConnectionTimerKind` remains a bitflag kind. Tokens can continue to be derived from one-hot bit position. Do not add a hand-written `if kind == ...` token chain.

- [ ] Step 4: Run timer tests.

```bash
cargo test -p hammer-service --test tcp_timer_recovery
cargo test -p hammer-service transport::tcp::session::tests
```

Expected result: pass.

### Module 5: Receive Out-Of-Order Queue, SACK, And DSACK

**Files:**
- Modify: `crates/hammer-core/src/protocol/tcp/options.rs`
- Modify: `crates/hammer-core/src/protocol/tcp/segment.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Create: `crates/hammer-service/src/transport/tcp/receive_queue.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Create tests: `crates/hammer-service/tests/tcp_receive_ofo_sack.rs`
- Modify tests: `crates/hammer-service/tests/tcp_established_receive.rs`

**Kernel reference:** `tcp_data_queue`, `tcp_data_queue_ofo`, `tcp_ofo_queue`, `tcp_dsack_set`, `tcp_send_dupack`.

- [ ] Step 1: Add failing OOO receive tests.

Test names:

```rust
#[test]
fn tcp_out_of_order_payload_is_queued_and_sack_is_advertised() {}

#[test]
fn tcp_gap_fill_drains_ofo_queue_to_app_in_sequence_order() {}

#[test]
fn tcp_duplicate_payload_sets_dsack_and_does_not_deliver_twice() {}

#[test]
fn tcp_overlapping_payload_trims_duplicate_prefix_and_delivers_new_tail() {}

#[test]
fn tcp_fin_queued_out_of_order_is_applied_when_gap_closes() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_receive_ofo_sack
```

Expected result: fails because out-of-order receive is not stored.

- [ ] Step 2: Extend core TCP options for SACK blocks.

`hammer-core::protocol::tcp` already exposes `TcpSackBlock` in tests. Ensure packet parsing exposes inbound SACK blocks and output options can include outbound SACK/DSACK blocks.

- [ ] Step 3: Implement receive queue.

Receive queue behavior:

- In-order payload advances `rcv_nxt` and enqueues data to app.
- Future payload is retained in `TcpReceiveQueue` and schedules an ACK with SACK blocks.
- Duplicate or overlapping payload creates DSACK state.
- When the gap closes, queued buffers are delivered in sequence and `rcv_nxt` advances through contiguous ranges.
- FIN consumes one sequence number and can be held in OOO queue until preceding data arrives.

- [ ] Step 4: Connect ACK generation.

ACK generation should include:

- immediate ACK for gap, duplicate, overlap, FIN, and gap-fill;
- delayed ACK for normal in-order data when policy permits;
- SACK blocks for OOO ranges;
- DSACK block before normal SACK blocks when duplicate data was observed.

- [ ] Step 5: Run receive tests.

```bash
cargo test -p hammer-service --test tcp_receive_ofo_sack
cargo test -p hammer-service --test tcp_established_receive
```

Expected result: pass.

### Module 6: Delayed ACK, Persist, And Pacing Timers

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Create tests: `crates/hammer-service/tests/tcp_ack_timers.rs`
- Create tests: `crates/hammer-service/tests/tcp_persist_pacing.rs`

**Kernel reference:** `tcp_ack_snd_check`, `tcp_send_delayed_ack`, `tcp_probe_timer`, `tcp_pacing_delay`.

- [ ] Step 1: Add failing ACK/persist/pacing tests.

Test names:

```rust
#[test]
fn tcp_in_order_payload_schedules_delayed_ack_instead_of_immediate_ack() {}

#[test]
fn tcp_second_in_order_payload_before_timer_emits_ack() {}

#[test]
fn tcp_delayed_ack_timer_emits_pending_ack_once() {}

#[test]
fn tcp_zero_window_ack_arms_persist_timer() {}

#[test]
fn tcp_persist_timer_emits_probe_without_advancing_write_queue() {}

#[test]
fn tcp_pacing_timer_resumes_deferred_send_queue() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_ack_timers
cargo test -p hammer-service --test tcp_persist_pacing
```

Expected result: fails because these timers do not drive established TCP behavior.

- [ ] Step 2: Add pending ACK state to TCP connection.

This is real TCP state:

- ACK pending bit;
- immediate ACK reason bit;
- delayed ACK counter or simple two-segment policy;
- latest SACK/DSACK blocks from receive queue;
- advertised receive window.

Do not create a node-facing ACK disposition enum.

- [ ] Step 3: Implement delayed ACK policy.

Initial Hammer policy:

- immediate ACK for SYN/SYN-ACK/final ACK processing, FIN, RST response, invalid seq, out-of-order segment, duplicate segment, DSACK, and gap fill;
- delayed ACK for first clean in-order data segment;
- immediate ACK when a second clean in-order data segment arrives before delayed ACK timer expires.

- [ ] Step 4: Implement persist and pacing.

Persist:

- when peer advertised send window becomes zero and there is pending unsent data, arm `PERSIST`;
- on expiry, emit one probe segment through the same segment allocation path;
- keep RTO separate from persist timer.

Pacing:

- ask `C::next_send_delay(pending_bytes)` before emitting data;
- if delay is non-zero, arm `PACING` through session timer and leave session ready for timer expiry;
- on `PACING`, call the same send flush path.

- [ ] Step 5: Run tests.

```bash
cargo test -p hammer-service --test tcp_ack_timers
cargo test -p hammer-service --test tcp_persist_pacing
```

Expected result: pass.

### Module 7: App Close, FIN Output, And Close-State Completion

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify close nodes under `crates/hammer-service/src/transport/tcp/`
- Create tests: `crates/hammer-service/tests/tcp_close_path.rs`
- Modify tests: `crates/hammer-service/tests/tcp_connection_state.rs`

**Kernel reference:** `tcp_shutdown`, `tcp_send_fin`, `tcp_close_state`, `tcp_fin`, `tcp_time_wait`.

- [ ] Step 1: Add failing close tests.

Test names:

```rust
#[test]
fn tcp_app_close_from_established_queues_fin_and_enters_fin_wait1() {}

#[test]
fn tcp_app_close_from_close_wait_queues_fin_and_enters_last_ack() {}

#[test]
fn tcp_fin_is_retransmitted_until_acknowledged() {}

#[test]
fn tcp_fin_wait1_ack_enters_fin_wait2() {}

#[test]
fn tcp_fin_wait2_remote_fin_enters_time_wait_and_acks() {}

#[test]
fn tcp_last_ack_ack_closes_session_and_completes_app_close() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_close_path
```

Expected result: fails because drained app closes are not consumed into FIN transitions.

- [ ] Step 2: Consume drained app closes during ready-session handling.

Allowed shape:

```rust
let connection: TcpConnection<Established, TcpServiceController> =
    queue.take_connection(session_id)?;
connection.close_from_app(runtime, queue, session_id, output, output_next, now)?;
```

There should be one typed method per state where local close is legal. Do not add a generic `close_erased` queue API.

- [ ] Step 3: Treat FIN as sent sequence state.

FIN consumes one sequence number and must enter sent/retransmit tracking just like Linux tracks FIN in the write queue.

- [ ] Step 4: Complete app close exactly at TCP close completion.

Close completion should happen when the TCP state reaches `Closed` from the local close path, not when the app close submission is drained.

- [ ] Step 5: Run close tests.

```bash
cargo test -p hammer-service --test tcp_close_path
cargo test -p hammer-service --test tcp_connection_state
```

Expected result: pass.

### Module 8: Passive Open Backlog, SYN-ACK Retry, And Syncookie Design Hook

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_rcvd.rs`
- Create tests: `crates/hammer-service/tests/tcp_listen_backlog.rs`
- Modify tests: `crates/hammer-service/tests/tcp_passive_open.rs`

**Kernel reference:** `request_sock`, `tcp_create_openreq_child`, `tcp_check_req`, `tcp_v4_conn_request`, `syncookies.c`.

- [ ] Step 1: Add failing listen backlog tests.

Test names:

```rust
#[test]
fn tcp_listener_limits_syn_rcvd_children_by_backlog() {}

#[test]
fn tcp_syn_ack_timer_retransmits_syn_ack_for_pending_child() {}

#[test]
fn tcp_final_ack_removes_pending_child_from_backlog() {}

#[test]
fn tcp_backlog_overflow_uses_syncookie_hook_without_allocating_child() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_listen_backlog
```

Expected result: fails because listener child backlog is not bounded.

- [ ] Step 2: Add bounded passive-open accounting.

Hammer mapping:

- Linux `request_sock` maps to a `SynRcvd` child session plus pending index entry.
- Listener policy stays in TCP session protocol, not in input node.
- Backlog limit is per listener key.
- SYN-ACK retransmit uses session timer.

- [ ] Step 3: Add a syncookie hook, not full syncookie implementation.

Current module adds the decision point and tests that overflow does not allocate unbounded child sessions. Full cookie encoding/validation is Module 11 because it needs option encoding policy and secret rotation.

- [ ] Step 4: Run passive open tests.

```bash
cargo test -p hammer-service --test tcp_listen_backlog
cargo test -p hammer-service --test tcp_passive_open
```

Expected result: pass.

### Module 9: TIME-WAIT, Tuple Retention, And Old Segment Handling

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/time_wait.rs`
- Create tests: `crates/hammer-service/tests/tcp_time_wait.rs`

**Kernel reference:** `tcp_time_wait`, `tcp_timewait_state_process`.

- [ ] Step 1: Add failing TIME-WAIT tests.

Test names:

```rust
#[test]
fn tcp_time_wait_retains_tuple_until_timer_expires() {}

#[test]
fn tcp_time_wait_reacks_duplicate_fin() {}

#[test]
fn tcp_time_wait_rst_closes_session() {}

#[test]
fn tcp_time_wait_timer_removes_session_and_index() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_time_wait
```

Expected result: fails because TIME-WAIT does not retain behavior through timer expiry.

- [ ] Step 2: Implement session timer-backed TIME-WAIT.

No private deadline fields. Entering `TimeWait` arms `TcpConnectionTimerKind::TIME_WAIT`; timer expiry closes the session and removes indexes.

- [ ] Step 3: Run tests.

```bash
cargo test -p hammer-service --test tcp_time_wait
```

Expected result: pass.

### Module 10: Receive Window, Memory Pressure, And Buffer Ownership

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/receive_queue.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/session/app.rs`
- Create tests: `crates/hammer-service/tests/tcp_receive_window.rs`

**Kernel reference:** `tcp_cleanup_rbuf`, `tcp_receive_window`, `tcp_prune_ofo_queue`, receive buffer accounting.

- [ ] Step 1: Add failing receive-window tests.

Test names:

```rust
#[test]
fn tcp_receive_window_shrinks_when_app_recv_queue_is_full() {}

#[test]
fn tcp_receive_window_grows_after_app_consumes_payload() {}

#[test]
fn tcp_ofo_queue_is_bounded_by_receive_buffer_limit() {}

#[test]
fn tcp_receive_buffer_pressure_drops_new_ofo_before_in_order_payload() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_receive_window
```

Expected result: fails because advertised receive window is static.

- [ ] Step 2: Account receive-buffer ownership.

Hammer receive memory includes:

- packet buffers queued to app receive path;
- packet buffers held in TCP out-of-order queue;
- packet buffers held while waiting for app recv descriptors.

Advertised receive window should derive from configured receive capacity minus held bytes.

- [ ] Step 3: Run tests.

```bash
cargo test -p hammer-service --test tcp_receive_window
```

Expected result: pass.

### Module 11: Modern Feature Hooks After Business Closure

**Files:**
- Modify only after Modules 1-10 pass.
- Expected files: `state_machine.rs`, `segment.rs`, `session.rs`, `transport/congestion/**`, and new focused tests.

**Kernel reference:** ECN/AccECN, syncookies, PMTU probing, keepalive, zerocopy.

- [ ] Step 1: Add ECN/CE sample propagation.

Tests:

```rust
#[test]
fn tcp_ecn_ce_mark_reaches_congestion_sample() {}

#[test]
fn tcp_ece_ack_enters_cwr_or_controller_recovery_event() {}
```

- [ ] Step 2: Add full syncookie implementation for backlog overflow.

Tests:

```rust
#[test]
fn tcp_syncookie_ack_reconstructs_syn_rcvd_options_without_child_allocation() {}

#[test]
fn tcp_syncookie_rejects_expired_or_wrong_tuple_cookie() {}
```

- [ ] Step 3: Add PMTU/MSS update path.

Tests:

```rust
#[test]
fn tcp_mtu_update_reduces_effective_mss_for_future_segments() {}

#[test]
fn tcp_mtu_probe_success_raises_effective_payload_size() {}
```

- [ ] Step 4: Add keepalive when a user-visible keepalive setting exists.

Tests:

```rust
#[test]
fn tcp_keepalive_timer_emits_probe_on_idle_established_connection() {}

#[test]
fn tcp_keepalive_probe_limit_closes_unresponsive_connection() {}
```

- [ ] Step 5: Add zerocopy optimization only after TCP queue ownership is correct.

Rules:

- Use Hammer app ring/data-buffer ownership, not Linux socket flags.
- Keep fallback copy path.
- Do not expose TCP internals to `hammer-runtime::app`.

## Explicitly Excluded From This Plan

These Linux features are intentionally not implemented in this plan because they are kernel compatibility surfaces, security-specific extensions, or broad subsystems outside Hammer's current TCP business closure:

- TCP repair mode.
- TCP MD5 and TCP-AO.
- MPTCP.
- Kernel BPF sockops and struct-ops integration.
- TLS/device offload.
- SMC.
- Urgent data/OOB behavior.
- `/proc` socket iterators and diagnostic dump parity.
- Full Linux `setsockopt`/`ioctl` compatibility.
- Kernel orphan socket policy and per-net sysctl matrix.

If any of these become product requirements, write a separate plan with its own architecture review.

## Structural Scans

Run these after each module and before final review:

```bash
rg -n "TcpOutputSendView|TcpStateMachine|TcpStateTransition|TcpStateMachineOutput|TcpAcceptedPayload|TcpStateSegment|TcpActiveOpen|TcpConnectionView|with_state|enter_|expect_|as_|map_phase|phase" crates/hammer-service/src/transport/tcp
rg -n "match .*TcpConnectionState|TcpConnectionState::" crates/hammer-service/src/transport/tcp/{listen.rs,syn_sent.rs,syn_rcvd.rs,established.rs,close_wait.rs,fin_wait1.rs,fin_wait2.rs,closing.rs,last_ack.rs,time_wait.rs}
rg -n "BbrController|Cubic|Reno|congestion.*algorithm|algorithm.*congestion" crates/hammer-service/src/transport/tcp
rg -n "std::vec::Vec" crates/hammer-service/src/transport/tcp crates/hammer-service/src/transport/congestion
rg -n "Instant" crates/hammer-service/src/transport/tcp/state_machine.rs crates/hammer-service/src/transport/tcp/connection.rs
```

Expected:

- First scan returns no production matches except plan/docs/tests that intentionally mention forbidden names.
- Packet-node scan returns no node-side state-storage matches.
- Congestion algorithm scan returns no TCP production code that selects behavior by algorithm identity.
- `std::vec::Vec` is absent from production TCP/congestion hot-path state.
- `Instant` appears only for estimators, sent timestamps, recovery samples, and congestion samples, not connection-private wakeup deadlines.

## Verification Commands

Focused module tests:

```bash
cargo test -p hammer-service --test tcp_send_queue
cargo test -p hammer-service --test tcp_app_send
cargo test -p hammer-service --test tcp_send_output
cargo test -p hammer-service --test tcp_ack_recovery
cargo test -p hammer-service --test tcp_timer_recovery
cargo test -p hammer-service --test tcp_receive_ofo_sack
cargo test -p hammer-service --test tcp_ack_timers
cargo test -p hammer-service --test tcp_persist_pacing
cargo test -p hammer-service --test tcp_close_path
cargo test -p hammer-service --test tcp_listen_backlog
cargo test -p hammer-service --test tcp_time_wait
cargo test -p hammer-service --test tcp_receive_window
```

Existing focused regression tests:

```bash
cargo test -p hammer-service --test tcp_connection_state
cargo test -p hammer-service --test tcp_passive_open
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service --test tcp_rack_tlp
cargo test -p hammer-service --test tcp_output
cargo test -p hammer-service --test transport_congestion_bbr
cargo test -p hammer-service transport::tcp::session::tests
cargo test -p hammer-service transport::tcp::syn_sent::tests
```

Final workspace checks:

```bash
cargo fmt --all
cargo test -p hammer-service
```

## Execution Order

1. Implement Module 1 and Module 2 together only if one engineer owns both; otherwise Module 1 first, then Module 2.
2. Implement Module 3 before Module 4 because timer recovery needs real ACK cleanup and sent records.
3. Implement Module 5 before Module 6 because delayed ACK policy depends on receive gap/SACK state.
4. Implement Module 7 before Module 9 because TIME-WAIT is reached from the close path.
5. Implement Module 8 independently after Module 4 because SYN-ACK retry uses the same timer discipline.
6. Implement Module 10 after Module 5 because receive memory accounting needs OOO buffer ownership.
7. Implement Module 11 only after the P0/P1 business path is green.

Commit after each module with scoped messages, for example:

```bash
git add crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_send_queue.rs
git commit -m "hammer-service(Feat): add tcp write queue"
```

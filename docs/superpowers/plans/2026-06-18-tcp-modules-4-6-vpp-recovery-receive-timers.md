# TCP Modules 4-6 VPP Session TX Recovery Receive Timers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Any item in **Approval Gate** must be shown to the user and approved before Rust implementation.

**Goal:** Complete Module 4, Module 5, and Module 6 using the current Hammer code as the source of truth: session-owned TX payload buffers, TCP connection-owned protocol state, exact session timer dispatch, recovery/CC integration, SACK/DSACK receive behavior, delayed ACK, persist, and pacing.

**Architecture:** Follow VPP semantics, not invented helper surfaces. Session owns app boundary copying and TX payload retention; TCP owns sequence/window/recovery/ACK/timer decisions; TCP output prepends headers from `TcpSegment`; runtime schedules nodes and timers. After the app/session copy, payload bytes must move by buffer ownership/refcount/chaining only.

**Tech Stack:** Rust 2024, `hammer-service` session/TCP nodes, `hammer-runtime::app`, `hammer-adapter::buffer`, `hammer-infra::{fifo,map,pool,vec,timer_wheel}`, `hammer-core::protocol::tcp`, local VPP source under `third_party/vpp`.

---

## 当前代码事实

- `crates/hammer-service/src/session/app.rs`
  - `SessionAppRuntime` already uses `Pool<FifoQueue<SessionAppTxProgress>> + FlatHashTable<u64, PoolIndex>`.
  - This lookup shape stays. No linear scan.
  - `SessionAppTxProgress` currently stores `AppSendData` and `sent_len`; this is not the final ownership model.

- `crates/hammer-service/src/session/runtime.rs`
  - `flush_one_session_tx` currently calls `copy_pending_send_bytes`, gets a `Vec<u8>`, appends that into a new dataplane buffer, and immediately commits app progress.
  - That is the main Module 4 bug: it creates a temporary payload vector and treats “output queued” as “session bytes can be released”.

- `crates/hammer-adapter/src/buffer.rs`
  - Buffer already has VPP-like header fields: `current_data`, `current_len`, `flags`, `next_buffer`, and `total_len_not_including_first`.
  - Buffer allocation already reserves default headroom in `alloc_slot`.
  - Buffer access must stay as direct borrowed `Buffer` / `Buffer` mutable access; do not introduce a dedicated wrapper type.
  - `prepend`, `advance`, and VPP-style chain metadata already exist on buffer ownership layers.

- `crates/hammer-service/src/transport/tcp/session.rs`
  - `TcpSessionProtocol` stores `segments: Pool<TcpSegment>` and `segment_index: FlatHashTable<u128, PoolIndex>`.
  - `insert_segment` is the current mapping from `BufferIndex` to TCP output intent; keep the concept, but keep naming about TCP segments only.
  - `handle_timer_expiry` already receives an exact timer token. It must not scan all timer kinds.

- `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - `TcpConnection<Established, C>` already has `tx_payload_len`, `tx_segment`, `commit_payload_tx`, `receive_ack`, and `accept_payload`.
  - `commit_payload_tx` currently constructs public `TcpSentSegment`; recovery records must become recovery-private.
  - Receive accepts only in-order payload today.

- `crates/hammer-service/src/transport/tcp/recovery.rs`
  - `TcpRecoveryState` already uses `hammer_infra::vec::Vec`.
  - `TcpSentSegment` is public and externally constructed. This must be removed.

- `crates/hammer-core/src/protocol/tcp/options.rs`
  - `TcpSackBlock` and inbound SACK parsing already exist.
  - Outbound SACK option writing is not wired into `TcpSegmentHeader`.

## VPP Facts Used

- `third_party/vpp/src/vlib/buffer.h`
  - `vlib_buffer_t` is the chain node. Chaining is `NEXT_PRESENT + next_buffer + total_length_not_including_first_buffer`.

- `third_party/vpp/src/vlib/buffer_funcs.h`
  - `vlib_buffer_attach_clone` attaches a tail chain to a head and increments refcounts through the tail.
  - `vlib_buffer_free_inline` decrements refcount and returns memory only when it reaches zero.

- `third_party/vpp/src/vnet/session/session_node.c`
  - `session_tx_fill_buffer` prepares buffers and leaves transport header prepend to transport.
  - `session_tx_fifo_read_and_snd_i` asks transport for send parameters, packetizes, then calls `push_header`.

- `third_party/vpp/src/vnet/tcp/tcp_output.c`
  - `tcp_session_push_header` is the transport header push point.

- `third_party/vpp/src/vnet/tcp/tcp_input.c`
  - `tcp_session_enqueue_data` and `tcp_session_enqueue_ooo` keep receive ordering and SACK facts in TCP/session state, not in packet nodes.

- `third_party/vpp/src/svm/svm_fifo.c`
  - FIFO supports `peek` and `dequeue_drop`; Hammer must reproduce the semantic result with current Hammer buffer ownership, not by inventing TCP-local byte containers.

## 不可变规则

- App/session is the only payload-copy boundary.
- Session owns TX payload buffer retention until ACK cleanup.
- TCP never owns app ring descriptors and never creates private payload copies for recovery.
- TCP output prepends headers. Session/runtime must not know TCP header fields.
- Runtime schedules nodes and timers. Congestion control does not schedule nodes.
- Congestion control remains transport-agnostic and is owned through `TcpConnection<S, C>`.
- Timer expiry dispatch uses exactly the token supplied by runtime.
- `TcpSegment` is the only TCP output intent. It must be built through its constructor or an approved replacement constructor.
- Recovery sent-packet accounting is private to `tcp/recovery.rs`.
- Do not add new session TX payload container types, receive-side queue modules, buffer owner wrappers, extra TCP output carriers, TCP-specific runtime helpers, TCP-specific buffer helpers, or builder-style node dependency setters.

## Layer Contract

- `hammer-runtime::app`
  - Owns app data chunks and io_uring-like submission/completion descriptors.
  - May copy app bytes into session-provided storage at the app/session boundary.

- `hammer-adapter::buffer`
  - Owns generic packet buffer allocation, chaining, prepend, tail writing, refcount, and Drop/release behavior.
  - Generic buffer capabilities must not mention TCP, session, recovery, or app.

- `hammer-service::session`
  - Owns pending app sends, session-owned TX payload buffers, ACK-driven cleanup hooks, RX app completion, and runtime scheduling.
  - May ask protocol for send parameters and then prepare buffer chains.

- `hammer-service::transport::tcp::state_machine`
  - Owns TCP sequence numbers, windows, recovery decisions, SACK/DSACK facts, delayed ACK state, persist state, and pacing decisions.
  - Must not import app ring types or allocate dataplane buffers.

- `hammer-service::transport::tcp::session`
  - Adapts typed TCP decisions to session runtime effects: timers, ready marking, output enqueue, connection index updates, and app close/recv completion.

- `hammer-service::transport::tcp::output`
  - Takes `TcpSegment`, prepends the TCP header, sets route metadata, and forwards.

- `hammer-service::transport::congestion`
  - Receives transport-generic packet sent/ACK/loss events only.

## 最终结果设计

### Module 4 Result

Session TX is buffer-owned:

1. App send submission is drained by `SessionAppRuntime`.
2. Session copies app bytes once into dataplane buffers it owns.
3. `SessionAppTxProgress` stops storing `AppSendData`; it stores only session-owned buffer state and byte counters needed for scheduling and ACK cleanup.
4. Ready-session dispatch asks TCP for send parameters.
5. Session builds an output packet by allocating a header-capable head buffer and attaching/refcounting the selected session-owned payload chain.
6. TCP creates a `TcpSegment` from connection state.
7. TCP output prepends the TCP header.
8. `commit_tx` records the packet in recovery, advances `snd_nxt`, calls `CongestionController::on_packet_sent`, and arms exact timers.
9. ACK processing drops confirmed bytes from session-owned TX payload buffers and clears recovery records.

No temporary payload vector is created in session/TCP TX.

### Module 5 Result

Receive ordering is TCP connection state:

1. `parse_tcp_packet` exposes inbound SACK blocks already parsed by `hammer-core`.
2. `receive_ack` feeds cumulative ACK and inbound SACK blocks into `TcpRecoveryState`.
3. In-order payload keeps the current path: advance input buffer to payload, truncate to payload length, and enqueue to app.
4. Future-sequence payload is retained by the TCP connection/session state as `BufferIndex` ownership plus sequence facts inside `TcpConnection`; no separate receive-queue module/type is introduced in this plan.
5. Duplicate or overlapping payload records DSACK facts and releases the duplicate input buffer.
6. When a gap closes, retained payload buffers are delivered in order through the existing app receive path.
7. ACK generation includes DSACK first, then SACK blocks, using the existing `TcpSegment` output path.

If implementation proves that the receive state cannot stay readable inside `TcpConnection`, stop and request approval for a concrete internal type before adding it.

### Module 6 Result

Timers are connection decisions plus session runtime dispatch:

1. Delayed ACK is a TCP connection flag/counter plus `DELAYED_ACK` session timer.
2. Persist is armed when peer advertised window is zero and unsent session-owned TX bytes remain.
3. Pacing is driven from `CongestionController::next_send_delay`; session runtime arms `PACING` and later marks the same session ready.
4. RTO/RACK/TLP operate on recovery-private records and schedule retransmission through the same session TX output path.
5. Timer expiry always dispatches the exact kind from runtime token; no timer-kind scan.

## Approval Gate

Before implementation, show the user the exact signature and reason for each item below.

### A. App Data Copy Into Existing Buffer Storage

Files:

- Modify: `crates/hammer-runtime/src/app/data.rs`
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify call sites in `crates/hammer-service/src/session/app.rs`

Required capability:

```rust
impl AppDataArea {
    pub fn copy_into(
        &self,
        addr: AppDataAddr,
        offset: usize,
        dst: &mut [u8],
    ) -> HammerResult<usize>;
}

impl AppSendData {
    pub fn copy_into(
        &self,
        offset: usize,
        dst: &mut [u8],
    ) -> HammerResult<usize>;
}
```

Reason:

- Existing app-send vector-copy behavior returns a new vector.
- Session needs the single allowed app/session copy to write directly into dataplane buffer tail storage.
- This API belongs to app data, not TCP, session runtime, or buffer.

### B. Generic Buffer Refcount Attach

Files:

- Modify: `crates/hammer-adapter/src/buffer.rs`

Required capability:

```rust
impl BufferPool {
    pub fn attach_clone(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()>;
}

impl DataPlaneBuffers {
    pub fn attach_clone(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()>;
}
```

Reason:

- This mirrors VPP `vlib_buffer_attach_clone`.
- Output packet head can carry the TCP/IP headers while payload remains session-owned.
- Refcounted release lets output and session share the same payload buffers without copying payload.

### C. Refcount Release Through Existing Buffer Owners

Files:

- Modify: `crates/hammer-adapter/src/buffer.rs`

Required result:

- Add `ref_count` to `Buffer`.
- `attach_clone` increments tail-chain refcounts.
- `free_index` and chain release decrement refcounts and only return slots to thread cache/pool at zero.
- Existing owner-like types such as `PooledBufferFrame` get Drop only if tests prove ownership leaks/double-free risk.

Reason:

- VPP release semantics are refcount based.
- Business modules must not decide cache vs pool return path.
- `BufferIndex` remains a Copy index, not an owning handle.

### D. Session TX Parameters

Files:

- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`

Required shape:

```rust
pub(crate) struct SessionTxParams {
    pub(crate) tx_offset: usize,
    pub(crate) max_payload_bytes: usize,
    pub(crate) max_segments: usize,
    pub(crate) segment_payload_capacity: usize,
}

pub(crate) trait SessionQueueProtocol<S> {
    fn tx_params(
        &mut self,
        context: &mut SessionQueueControlContext<'_, S>,
        session_id: SessionId,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<Option<SessionTxParams>>;
}
```

Reason:

- VPP transport exposes send parameters.
- TCP computes offset from `snd_nxt - snd_una`, send window, congestion window, MSS, pacing, and timer state.
- Session runtime packetizes from session-owned buffers without learning TCP header details.

### E. Outbound SACK Blocks On Existing TCP Segment

Files:

- Modify: `crates/hammer-core/src/protocol/tcp/options.rs`
- Modify: `crates/hammer-core/src/protocol/tcp/segment.rs`
- Modify: `crates/hammer-service/src/transport/tcp/segment.rs`

Required result:

- `TcpSegmentHeader` can write up to four SACK blocks.
- `TcpSegment` constructor accepts outbound SACK blocks or an approved compact equivalent.
- DSACK, when present, is emitted before regular SACK blocks.

Reason:

- Inbound SACK parsing already exists.
- Module 5/6 ACK output needs SACK/DSACK emission through the same TCP segment path.

### F. Recovery-Private Sent Accounting

Files:

- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`

Required result:

- Delete public construction of `TcpSentSegment`.
- `TcpRecoveryState::record_sent` accepts primitive TCP/recovery facts and creates its private record internally.
- `next_tlp_probe` returns enough facts for TCP/session retransmit scheduling without exposing the private record type publicly.

Reason:

- Recovery records are not public TCP API.
- Congestion events must remain transport-generic.

## Implementation Plan

### Task 0: Guardrails And Current-Code Cleanup

**Files:**

- Modify: `AGENTS.md`
- Modify: `docs/superpowers/plans/2026-06-18-tcp-modules-4-6-vpp-recovery-receive-timers.md`

- [ ] **Step 0.1: Add forbidden-surface scan**

Run before coding:

```bash
rg -n "runtime\\.copy_current_chain|TcpSentSegment" crates/hammer-service/src crates/hammer-service/tests
```

Expected:

- No session/TCP runtime-chain-copy path.
- No extra TCP output carrier.
- `TcpSentSegment` appears only before the recovery cleanup task.

- [ ] **Step 0.2: Confirm VPP references still exist**

Run:

```bash
rg -n "session_tx_fill_buffer|session_tx_fifo_read_and_snd_i" third_party/vpp/src/vnet/session/session_node.c
rg -n "vlib_buffer_attach_clone|vlib_buffer_free_inline" third_party/vpp/src/vlib/buffer_funcs.h
rg -n "tcp_session_enqueue_data|tcp_session_enqueue_ooo" third_party/vpp/src/vnet/tcp/tcp_input.c
```

Expected: all named VPP functions are found.

### Task 1: App Data Copies Directly Into Session-Owned Buffers

**Files:**

- Modify: `crates/hammer-runtime/src/app/data.rs`
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify tests in the same modules

- [ ] **Step 1.1: Add failing app copy test**

Add tests equivalent to:

```rust
#[test]
fn app_send_data_copy_into_writes_destination_slice() {
    let ring = AppRingHandle::with_data_area(4, 4, 256, 4).expect("ring");
    let send: AppSendData = ring
        .send_from_data(ring.alloc_data_for_bytes(b"abcdef").expect("data"))
        .try_into()
        .expect("send data");

    let mut dst = [0_u8; 3];
    let copied = send.copy_into(2, &mut dst).expect("copy into");

    assert_eq!(copied, 3);
    assert_eq!(&dst, b"cde");
    send.release();
}
```

Run:

```bash
cargo test -p hammer-runtime app::ring::tests::app_send_data_copy_into_writes_destination_slice
```

Expected: fails before `copy_into` exists.

- [ ] **Step 1.2: Implement app copy API**

Implementation rules:

- Validate app data address with existing app data validation.
- Bound by published length.
- Copy directly into caller-provided `dst`.
- Return the copied byte count.
- Do not allocate a vector.

- [ ] **Step 1.3: Verify**

Run:

```bash
cargo test -p hammer-runtime app::ring::tests::app_send_data_copy_into_writes_destination_slice
rg -n "copy_pending_send_bytes|commit_pending_send_bytes" crates/hammer-service/src/session crates/hammer-service/src/transport/tcp
```

Expected:

- Test passes.
- Session/TCP code has no app-send vector-copy use.

### Task 2: Generic Buffer Refcount Attach And Release

**Files:**

- Modify: `crates/hammer-adapter/src/buffer.rs`
- Modify buffer tests in the same file

- [ ] **Step 2.1: Add failing attach/refcount tests**

Required tests:

```rust
#[test]
fn attach_clone_keeps_tail_alive_until_both_chains_are_freed() {}

#[test]
fn freeing_head_with_attached_clone_does_not_free_session_tail() {}

#[test]
fn freeing_original_tail_after_output_head_returns_storage_once() {}
```

Run:

```bash
cargo test -p hammer-adapter attach_clone_keeps_tail_alive_until_both_chains_are_freed
```

Expected: fails before `attach_clone`/refcount behavior exists.

- [ ] **Step 2.2: Implement VPP-style attach**

Implementation rules:

- `head` must not already have `NEXT_PRESENT`.
- `head.next_buffer = Some(tail)`.
- `head.total_len_not_including_first = tail.current_len + tail.total_len_not_including_first`.
- Increment refcount for every buffer in the tail chain.
- Keep the API generic and transport-neutral.

- [ ] **Step 2.3: Implement refcount-aware release**

Implementation rules:

- `free_index` on a chain decrements each buffer.
- Only refcount zero resets and returns the slot to the existing cache/pool path.
- Chained release must not double-free a shared tail.
- Business code never calls a cache/pool-specific free path.

- [ ] **Step 2.4: Verify**

Run:

```bash
cargo test -p hammer-adapter buffer
rg -n "tcp|session|recovery" crates/hammer-adapter/src/buffer.rs
```

Expected:

- Buffer tests pass.
- Buffer layer has no TCP/session/recovery concepts.

### Task 3: Session-Owned TX Payload Buffers

**Files:**

- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`

- [ ] **Step 3.1: Add failing session ownership tests**

Replace old tests centered on vector-copy helpers with:

```rust
#[test]
fn pending_send_is_copied_once_into_session_owned_buffers() {}

#[test]
fn pending_send_lookup_uses_hash_after_multiple_sessions_complete() {}

#[test]
fn ack_cleanup_releases_only_confirmed_session_payload_bytes() {}
```

Run:

```bash
cargo test -p hammer-service session::app::tests::pending_send_is_copied_once_into_session_owned_buffers
```

Expected: fails while `SessionAppTxProgress` still stores `AppSendData`.

- [ ] **Step 3.2: Change `SessionAppTxProgress` fields**

Required result:

- Remove `send: AppSendData`.
- Remove `sent_len` as app-descriptor progress.
- Store session-owned buffer head/index facts and byte counters needed for:
  - pending length from first unacked byte;
  - new-data scheduling;
  - ACK cleanup;
  - buffer release on session close/drop.

Use existing `Pool<FifoQueue<SessionAppTxProgress>> + FlatHashTable<u64, PoolIndex>`.

- [ ] **Step 3.3: Rewrite `push_pending_send`**

Required result:

- `push_pending_send` returns `CoreResult<()>`.
- It allocates dataplane buffers from the existing buffer pool.
- It copies app bytes directly through the borrowed `Buffer` mutable tail.
- It commits bytes with `commit_writable_tail`.
- It chains buffers with existing generic chain operations.
- It releases `AppSendData` once session owns the copied bytes.
- It does not create an intermediate payload vector.

- [ ] **Step 3.4: Add session cleanup APIs**

Required behavior:

- Query pending bytes by session id through the hash table.
- Mark bytes as scheduled without freeing them.
- Drop ACKed bytes from the front of session-owned payload buffers.
- Remove the hash entry when the per-session queue becomes empty.

Do not add TCP-specific method names to `SessionAppRuntime`.

- [ ] **Step 3.5: Verify**

Run:

```bash
cargo test -p hammer-service session::app::tests
cargo test -p hammer-service session::runtime::tests
rg -n "copy_pending_send_bytes|commit_pending_send_bytes" crates/hammer-service/src/session crates/hammer-service/src/transport/tcp
```

Expected:

- Session tests pass.
- Old vector-copy TX helpers are gone from session/TCP.

### Task 4: VPP-Style TX Packetization From Session Buffers

**Files:**

- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs` only if header prepend needs the approved buffer push primitive

- [ ] **Step 4.1: Add failing TX packetization test**

Required behavior:

```rust
#[test]
fn session_tx_outputs_attached_payload_without_payload_copy_and_keeps_unacked_bytes() {}
```

The test must assert:

- app bytes are copied once into session-owned buffers;
- output packet payload points at the same underlying storage through refcount attachment;
- output free does not remove unacked session payload;
- ACK cleanup removes confirmed session payload.

Run:

```bash
cargo test -p hammer-service session_tx_outputs_attached_payload_without_payload_copy_and_keeps_unacked_bytes
```

Expected: fails before packetization changes.

- [ ] **Step 4.2: Replace `tx_payload_len` with `tx_params`**

Implement the approved `SessionTxParams` shape.

TCP returns:

- offset from first unacked byte;
- send budget from peer window, congestion window, MSS, and pacing;
- maximum segments for this runtime dispatch;
- segment payload capacity.

Session runtime uses the result to prepare one or more output packets.

- [ ] **Step 4.3: Build output packets by buffer attachment**

Required flow:

1. Allocate an output head buffer with normal buffer allocation.
2. Attach the selected session-owned payload chain with refcount attach.
3. Insert `TcpSegment` for the output head.
4. Enqueue output head to TCP output node.
5. If enqueue fails, remove the segment mapping and free the output head.

No TCP header fields are written in session runtime.

- [ ] **Step 4.4: Commit TX after output enqueue**

Required flow:

1. TCP records sent facts in recovery-private state.
2. TCP advances `snd_nxt`.
3. TCP calls `congestion.on_packet_sent`.
4. TCP returns exact timer kinds/ticks to arm.
5. Session arms only those exact timer tokens.

- [ ] **Step 4.5: Verify**

Run:

```bash
cargo test -p hammer-service session_tx_outputs_attached_payload_without_payload_copy_and_keeps_unacked_bytes
cargo test -p hammer-service transport::tcp::state_machine::tests::established_commit_records_sent_segment_and_congestion_send
rg -n "copy_current_chain|copy_pending_send_bytes|commit_pending_send_bytes" crates/hammer-service/src/session crates/hammer-service/src/transport/tcp
```

Expected:

- TX packetization test passes.
- No session/TCP payload vector-copy path remains.

### Task 5: ACK Cleanup, Recovery, And Congestion Events

**Files:**

- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/session/app.rs`

- [ ] **Step 5.1: Add failing ACK cleanup tests**

Required tests:

```rust
#[test]
fn cumulative_ack_removes_recovery_records_and_drops_session_payload() {}

#[test]
fn sack_ack_updates_recovery_without_dropping_unsafely_unconfirmed_payload() {}

#[test]
fn retransmitted_payload_does_not_generate_invalid_rtt_sample() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_ack_recovery
```

Expected: fails before ACK cleanup is connected to session payload retention.

- [ ] **Step 5.2: Make recovery records private**

Required result:

- Delete public `TcpSentSegment`.
- `record_sent` creates internal records from primitive facts.
- ACK/SACK cleanup returns confirmed byte count/facts needed by session cleanup and congestion events.
- `bytes_in_flight` remains derived from outstanding records.

- [ ] **Step 5.3: Wire ACK path**

Required flow:

1. TCP validates ACK.
2. TCP updates `snd_una` and window.
3. TCP passes cumulative ACK and inbound SACK blocks to recovery.
4. Recovery notifies congestion with `AckedPacket`, `LostPacket`, and `on_end_acks`.
5. TCP/session drops confirmed bytes from session-owned TX payload.
6. TCP/session re-arms or cancels RTO/RACK/TLP based on exact recovery state.

- [ ] **Step 5.4: Verify**

Run:

```bash
cargo test -p hammer-service --test tcp_ack_recovery
cargo test -p hammer-service transport::tcp::state_machine::tests
rg -n "TcpSentSegment" crates/hammer-service/src crates/hammer-service/tests
```

Expected:

- ACK/recovery tests pass.
- No public `TcpSentSegment` remains.

### Task 6: RTO, RACK, And TLP Timer Flow

**Files:**

- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`

- [ ] **Step 6.1: Add failing recovery timer tests**

Required tests:

```rust
#[test]
fn rto_retransmits_oldest_unacked_payload_and_backs_off() {}

#[test]
fn rack_timer_marks_elapsed_candidates_lost_and_notifies_congestion() {}

#[test]
fn tlp_timer_outputs_one_tail_probe_without_dropping_session_payload() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_recovery_timers
```

Expected: fails before timers drive established recovery.

- [ ] **Step 6.2: Implement exact timer handling**

Rules:

- `RETRANSMIT` retransmits the oldest eligible unacked payload.
- `RACK` marks elapsed candidates lost and schedules retransmission through the same TX path.
- `TLP` sends one probe using session-owned payload buffers.
- Timer handlers do not loop through all timer kinds.
- Timer handlers do not allocate private payload copies.

- [ ] **Step 6.3: Verify**

Run:

```bash
cargo test -p hammer-service --test tcp_recovery_timers
rg -n "TcpConnectionTimerKind::all|for timer in" crates/hammer-service/src/transport/tcp crates/hammer-service/src/session
```

Expected:

- Timer tests pass.
- No timer-kind scan exists.

### Task 7: Receive Ordering, SACK, And DSACK Without A New Queue Type

**Files:**

- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/segment.rs`
- Modify: `crates/hammer-core/src/protocol/tcp/options.rs`
- Modify: `crates/hammer-core/src/protocol/tcp/segment.rs`
- Create tests: `crates/hammer-service/tests/tcp_receive_sack.rs`

- [ ] **Step 7.1: Add failing receive tests**

Required tests:

```rust
#[test]
fn out_of_order_payload_is_retained_in_connection_state_and_sack_is_advertised() {}

#[test]
fn gap_fill_delivers_retained_payload_in_order() {}

#[test]
fn duplicate_payload_records_dsack_and_releases_duplicate_buffer() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_receive_sack
```

Expected: fails while established receive only accepts in-order payload.

- [ ] **Step 7.2: Expose inbound and outbound SACK on existing segment path**

Required result:

- `TcpPacket` carries inbound SACK blocks parsed by `hammer-core`.
- `TcpSegment` carries outbound SACK/DSACK blocks through its constructor.
- TCP output still writes only from `TcpSegment`.

- [ ] **Step 7.3: Add receive ordering state inside `TcpConnection`**

Required result:

- Retain future-sequence input buffers as `BufferIndex` ownership in TCP connection/session state.
- Store sequence start/end and payload length facts next to those buffer indexes.
- Store SACK blocks and optional DSACK fact in the same connection-owned state.
- Do not add a separate receive-side queue module/type in this plan.
- If the implementation becomes unreadable without a named internal record, stop and request approval with the exact type definition.

- [ ] **Step 7.4: Wire established receive**

Rules:

- In-order payload uses the existing `advance` + `truncate_chain` + app enqueue path.
- Future payload is retained and ACKed with SACK.
- Duplicate/overlap records DSACK and frees the duplicate input buffer.
- Gap close drains retained buffers in order through the existing app receive path.
- ACK generation emits DSACK first, then SACK blocks.

- [ ] **Step 7.5: Verify**

Run:

```bash
cargo test -p hammer-service --test tcp_receive_sack
cargo test -p hammer-service --test tcp_established_receive
rg -n "copy_current_chain" crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_receive_sack.rs
```

Expected:

- Receive tests pass.
- TCP receive path does not copy payload except app completion.

### Task 8: Delayed ACK, Persist, And Pacing

**Files:**

- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Create tests: `crates/hammer-service/tests/tcp_timers.rs`

- [ ] **Step 8.1: Add failing timer policy tests**

Required tests:

```rust
#[test]
fn clean_in_order_payload_arms_delayed_ack_timer() {}

#[test]
fn delayed_ack_timer_emits_one_ack_and_clears_pending_state() {}

#[test]
fn zero_window_with_pending_tx_arms_persist_timer() {}

#[test]
fn persist_timer_emits_probe_without_advancing_confirmed_cleanup() {}

#[test]
fn pacing_delay_arms_timer_and_runtime_later_marks_session_ready() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_timers
```

Expected: fails before Module 6 timer behavior exists.

- [ ] **Step 8.2: Implement delayed ACK**

Rules:

- Immediate ACK for FIN, RST response, invalid sequence, future payload, duplicate payload, DSACK, gap close, and second clean in-order segment before timer expiry.
- Delayed ACK for first clean in-order payload when policy allows.
- `DELAYED_ACK` is armed through session timer wheel only.

- [ ] **Step 8.3: Implement persist**

Rules:

- Peer zero window plus pending unsent payload arms `PERSIST`.
- Persist probe uses the same session-owned payload buffer machinery.
- Persist is separate from RTO.

- [ ] **Step 8.4: Implement pacing**

Rules:

- TCP asks congestion controller for `next_send_delay`.
- Non-zero delay arms `PACING` and returns no packet for that dispatch.
- Pacing timer expiry marks the same session ready.
- Congestion controller does not schedule nodes.

- [ ] **Step 8.5: Verify**

Run:

```bash
cargo test -p hammer-service --test tcp_timers
cargo test -p hammer-service transport::tcp::session::tests
rg -n "CongestionControlNode|cc sibling|sibling" crates/hammer-service/src/transport/tcp crates/hammer-service/src/session
```

Expected:

- Timer policy tests pass.
- No TCP congestion-control node/sibling scheduling path is introduced.

### Task 9: Final Verification

**Files:**

- All files changed by Tasks 1-8

- [ ] **Step 9.1: Run focused tests**

```bash
cargo test -p hammer-runtime app::ring
cargo test -p hammer-adapter buffer
cargo test -p hammer-service session::app::tests
cargo test -p hammer-service session::runtime::tests
cargo test -p hammer-service transport::tcp::state_machine::tests
cargo test -p hammer-service transport::tcp::session::tests
cargo test -p hammer-service --test tcp_ack_recovery
cargo test -p hammer-service --test tcp_recovery_timers
cargo test -p hammer-service --test tcp_receive_sack
cargo test -p hammer-service --test tcp_timers
```

- [ ] **Step 9.2: Run architecture scans**

```bash
rg -n "runtime\\.copy_current_chain|TcpSentSegment" crates/hammer-service/src crates/hammer-service/tests
rg -n "copy_pending_send_bytes|commit_pending_send_bytes" crates/hammer-service/src/session crates/hammer-service/src/transport/tcp
rg -n "TcpConnectionTimerKind::all|for timer in" crates/hammer-service/src/transport/tcp crates/hammer-service/src/session
rg -n "CongestionControlNode|cc sibling|sibling" crates/hammer-service/src/transport/tcp crates/hammer-service/src/session
```

Expected:

- No forbidden surfaces remain in session/TCP implementation.
- No timer-kind scan.
- No TCP congestion scheduling node.

- [ ] **Step 9.3: Run workspace verification**

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets
```

Expected:

- Formatting succeeds.
- Workspace tests pass.
- Clippy succeeds or each remaining warning is documented with the exact reason.

## Self Check

- Module 4 is covered by Tasks 1-5: app/session copy, session-owned buffers, no payload vector, TX attachment, ACK cleanup, and CC send events.
- Module 5 is covered by Tasks 5 and 7: inbound SACK, recovery SACK processing, connection-owned receive ordering, SACK/DSACK ACK output.
- Module 6 is covered by Tasks 6 and 8: RTO/RACK/TLP, delayed ACK, persist, pacing, and exact timer dispatch.
- This plan does not add TCP-specific buffer/runtime helpers.
- New or changed API surfaces are listed in Approval Gate and must be approved before Rust implementation.

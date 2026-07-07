# VPP Session TX Batch Commit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Hammer's normal Session TX seam with VPP-style send facts, batch transport commit, and post-commit graph visibility.

**Architecture:** Session Runtime owns TX FIFO byte selection, TX Batch preparation, ready-queue mechanics, and TX Batch Flush. TCP transport owns send eligibility facts, `push_header` batch commit, TCP Output Intent construction, `snd_nxt`, recovery, TCP timers, and custom TX. The old external `prepare_tx` / `cancel_tx` / `commit_tx` seam is removed rather than wrapped.

**Tech Stack:** Rust 2024, `hammer-service`, `hammer-runtime` app/session FIFOs, `hammer-adapter` data-plane buffers/frames, VPP reference under `third_party/vpp`.

## Global Constraints

- Use TDD: each task must write or update a failing behavior test before changing production code.
- Test only agreed seams: Session Runtime ↔ SessionQueueProtocol behavior, real TCP transport behavior, and source-level architecture guardrails.
- Respect `CONTEXT.md` terms: TX Batch, Transport TX Action, TX Batch Flush, Transport-Neutral TX Fact, Send Goal Size, Timer Token.
- Respect `docs/adr/0002-session-tx-commits-before-graph-visibility.md`.
- Normal TX must follow VPP `TRANSPORT_TX_PEEK`: normal TX may peek/copy TX FIFO bytes, but only ACK cleanup may drop retained TX FIFO bytes.
- Graph visibility must happen only after the transport-owned batch action commits transport state.
- Do not add TCP-specific runtime APIs, TCP-specific buffer APIs, output carriers, rollback/cancel transaction records, or io_uring-style app rings.
- Do not expose TCP header fields, TCP timer masks, recovery records, GSO metadata, offload metadata, TCP option length, or TCP Output Intent construction to Session Runtime.
- Buffer APIs must remain transport-neutral; use generic headroom, chain, and refcount behavior.
- Congestion control and TCP must not schedule graph nodes or directly own the Session Ready Queue.
- Use existing `hammer-infra` primitives before adding new local utilities.

---

## File Structure

- Modify `crates/hammer-service/src/session/runtime.rs`: own the new `SessionQueueProtocol` TX interface, batch preparation, post-commit TX Batch Flush, ready-queue scheduling from transport-neutral facts, and tests for fake protocol behavior.
- Modify `crates/hammer-service/src/session/protocol.rs`: keep only transport-neutral session control context; remove TCP timer-mask helper; expose generic timer/ready operations needed by transport-owned timer logic.
- Modify `crates/hammer-service/src/session/node.rs`: keep `SessionQueueOutput` as the graph-flush adapter; do not add TCP-specific output carriers.
- Modify `crates/hammer-service/src/session/app.rs`: continue TX FIFO peek/copy and ACK drop behavior; add helper only if it stays session/app generic.
- Modify `crates/hammer-service/src/transport/tcp/mod.rs`: implement VPP-shaped `send_params`, `push_header`, `custom_tx`, and exact timer-token handling for `TcpConnection<C>`.
- Modify `crates/hammer-service/src/transport/tcp/connection.rs`: adapt TCP send budget/header commit/timer refresh internals to the new transport-owned TX actions; preserve ACK cleanup.
- Modify `crates/hammer-service/src/transport/tcp/output.rs`: keep TCP header/output and GSO metadata ownership in TCP output.
- Modify `crates/hammer-service/tests/session_queue_dispatch.rs`: update integration tests to the new batch seam.
- Add or modify `crates/hammer-service/tests/vpp_session_tx_guardrails.rs`: source-level guardrail tests for forbidden seam leaks.

## Approved Interface Shape

The external normal TX surface must converge on these shapes. Names may be adjusted only if the final names are equally VPP-aligned and update all tests:

```rust
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct TransportSendFlags: u8 {
        const DESCHED = 1 << 0;
        const POSTPONE = 1 << 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportSendParams {
    pub snd_space: usize,
    pub tx_offset: usize,
    pub send_goal_size: usize,
    pub flags: TransportSendFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxBatchBuffer {
    pub index: hammer_adapter::BufferIndex,
    pub tx_offset: usize,
    pub payload_len: usize,
}
```

`SessionQueueProtocol` must expose VPP-shaped operations rather than the old three-step transaction:

```rust
pub trait SessionQueueProtocol: Sized {
    fn handle_expired_timer(
        &mut self,
        runtime: &hammer_adapter::DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        timer_id: u32,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> hammer_core::error::CoreResult<bool>;

    fn handle_ready_session(
        &mut self,
        runtime: &hammer_adapter::DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        close_requested: bool,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> hammer_core::error::CoreResult<bool>;

    fn send_params(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        pending_len: usize,
        now: std::time::Instant,
    ) -> hammer_core::error::CoreResult<TransportSendParams>;

    fn push_header(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        batch: &[TxBatchBuffer],
        now: std::time::Instant,
    ) -> hammer_core::error::CoreResult<()>;

    fn custom_tx(
        &mut self,
        runtime: &hammer_adapter::DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        max_burst: usize,
        now: std::time::Instant,
    ) -> hammer_core::error::CoreResult<usize>;

    fn on_close(&mut self, context: &mut crate::session::protocol::SessionQueueControlContext);
}
```

## Task 1: Replace normal Session TX with VPP batch commit end-to-end

**Issue:** #16

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/node.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/tests/session_queue_dispatch.rs`

**Interfaces:**
- Consumes: existing `SessionAppRuntime::pending_send_len`, `SessionAppRuntime::copy_tx_to_buffer`, `SessionQueueOutput::enqueue_frame`.
- Produces: `TransportSendFlags`, `TransportSendParams`, `TxBatchBuffer`, `SessionQueueProtocol::send_params`, `SessionQueueProtocol::push_header`, `SessionQueueProtocol::custom_tx`, and a compiling TCP normal new-data implementation of the new seam.

- [ ] **Step 1: Update fake protocol tests to describe the new seam**

Replace the fake protocol expectations in `crates/hammer-service/tests/session_queue_dispatch.rs` so the test protocol implements `send_params`, `push_header`, and `custom_tx`. Add a test named `session_tx_dispatch_commits_batch_before_graph_visibility`.

The test should use a fake protocol that records:

```rust
#[derive(Default)]
struct TestTxProtocol {
    offset: usize,
    send_params_calls: usize,
    push_header_calls: usize,
    pushed_batches: std::vec::Vec<std::vec::Vec<(usize, usize)>>,
}
```

The test should assert:

```rust
assert_eq!(protocol.send_params_calls, 1);
assert_eq!(protocol.push_header_calls, 1);
assert_eq!(protocol.pushed_batches, vec![vec![(0, 4), (4, 4), (8, 4), (12, 4)]]);
```

Use a 16-byte app send and a `send_goal_size` of 4 so the first TX Batch has four buffers.

- [ ] **Step 2: Run the updated focused test and verify RED**

Run:

```bash
cargo test -p hammer-service --test session_queue_dispatch session_tx_dispatch_commits_batch_before_graph_visibility
```

Expected: FAIL to compile or fail assertions because `SessionQueueProtocol` still requires `tx_offset`, `tx_payload_len`, `prepare_tx`, `cancel_tx`, and `commit_tx`.

- [ ] **Step 3: Replace `SessionQueueProtocol` normal TX methods**

In `crates/hammer-service/src/session/runtime.rs`, replace the old normal-TX methods with the approved interface shape from this plan. Remove `tx_offset`, `tx_payload_len`, `prepare_tx`, `cancel_tx`, and `commit_tx` from the trait.

Keep `handle_expired_timer`, `handle_ready_session`, and `on_close`.

- [ ] **Step 4: Implement Session Runtime TX Batch preparation and flush**

Rewrite the normal TX loop in `dispatch_session_queue_pending` so it:

1. Calls `send_params(context, total_len, now)`.
2. Handles `snd_space == 0` without allocating buffers.
3. Computes `pending_len = total_len.saturating_sub(params.tx_offset)`.
4. Uses `payload_len = min(pending_len, params.snd_space, params.send_goal_size)`.
5. Prepares up to `DEFAULT_TX_DISPATCH_BUDGET` buffers into a local `Frame<Next>` and a local `hammer_infra::vec::Vec<TxBatchBuffer>`.
6. Calls `push_header(context, batch.as_slice(), now)` before `output.enqueue_frame(runtime, owner)`.
7. Calls `output.enqueue_frame(runtime, owner)` only when `push_header` succeeds.

Do not drop TX FIFO bytes in this loop.

- [ ] **Step 5: Update module tests in `session/runtime.rs`**

Update `FakeTxProtocol` and `NoTxPayloadProtocol` in the module tests to the new trait. Keep the no-pending-send test asserting no transport TX call is made when the app TX FIFO is empty.

- [ ] **Step 6: Migrate TCP normal new-data TX enough to compile and pass**

Update `SessionQueueProtocol for TcpConnection<C>` so the real TCP implementation compiles against the new trait:

1. `send_params` returns `tx_offset` from the previous `tx_offset` logic.
2. `send_params` returns `snd_space` from the existing `tx_payload_budget` result.
3. `send_params` returns `send_goal_size` from the current TCP output payload length.
4. `push_header` constructs the existing `TcpSegment` internally for each `TxBatchBuffer`, writes the TCP header, and commits payload TX state.
5. `custom_tx` may initially return `Ok(0)` for this task if special TCP output is still handled by existing ready/timer paths; Task 2 will complete the custom TX separation.

Do not leave any call from Session Runtime to old `prepare_tx`, `cancel_tx`, or `commit_tx`.

- [ ] **Step 7: Run focused tests and verify GREEN**

Run:

```bash
cargo test -p hammer-service --test session_queue_dispatch
cargo test -p hammer-service session_tx_does_not_call_transport_when_app_has_no_pending_send
```

Expected: PASS.

- [ ] **Step 8: Verify old seam is gone from session runtime**

Run:

```bash
rg -n "prepare_tx|cancel_tx|commit_tx|tx_payload_len\\(|fn tx_offset" crates/hammer-service/src/session crates/hammer-service/tests/session_queue_dispatch.rs
```

Expected: no matches.

- [ ] **Step 9: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs crates/hammer-service/src/session/app.rs crates/hammer-service/src/session/node.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/src/transport/tcp/output.rs crates/hammer-service/tests/session_queue_dispatch.rs
git commit -m "hammer-service(Refactor): replace session tx with vpp batch seam"
```

## Task 2: Route TCP special output and timer-token handling through transport-owned paths

**Issue:** #18

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`

**Interfaces:**
- Consumes: `TransportSendParams`, `TxBatchBuffer`, `SessionQueueProtocol::send_params`, `push_header`, `custom_tx`.
- Produces: TCP custom TX implementation for special output and exact Timer Token handling.

- [ ] **Step 1: Write TCP seam regression tests**

In the existing TCP/session tests near the current TCP module tests, add tests that prove:

1. Special TCP output can be produced through `custom_tx` without normal new-data TX packetization.
2. ACK cleanup is still the only path that drops TX FIFO bytes.
3. Expired timer dispatch uses the exact timer token supplied by Session Runtime.

Name the tests:

```rust
tcp_custom_tx_handles_special_output_without_normal_packetization
tcp_normal_tx_retains_fifo_until_ack_cleanup
tcp_timer_dispatch_uses_exact_timer_token
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
cargo test -p hammer-service tcp_custom_tx_handles_special_output_without_normal_packetization
cargo test -p hammer-service tcp_normal_tx_retains_fifo_until_ack_cleanup
cargo test -p hammer-service tcp_timer_dispatch_uses_exact_timer_token
```

Expected: FAIL until TCP special output and timer-token ownership are fully moved.

- [ ] **Step 3: Implement TCP `custom_tx`**

Have `custom_tx` handle transport-owned special output that is currently produced from `handle_ready_session` or timer paths when it is not normal FIFO new-data TX. It may enqueue TCP output through the existing TCP output path, but Session Runtime must not construct TCP headers for those outputs.

- [ ] **Step 4: Remove TCP timer-mask helper from session protocol**

Remove `refresh_tcp_timers` and `SessionQueueControlContext::refresh_tcp_timers` from `crates/hammer-service/src/session/protocol.rs`. Keep generic accessors needed for TCP to arm or cancel the exact timer it owns.

- [ ] **Step 5: Run focused TCP/session tests**

Run:

```bash
cargo test -p hammer-service --test session_queue_dispatch
cargo test -p hammer-service tcp_custom_tx_handles_special_output_without_normal_packetization
cargo test -p hammer-service tcp_normal_tx_retains_fifo_until_ack_cleanup
cargo test -p hammer-service tcp_timer_dispatch_uses_exact_timer_token
```

Expected: PASS.

- [ ] **Step 6: Verify no TCP timer mask leak remains in session protocol**

Run:

```bash
rg -n "TCP_TIMER_COUNT|active_timer_mask|refresh_tcp_timers|timer_mask" crates/hammer-service/src/session crates/hammer-service/src/transport/tcp/mod.rs
```

Expected: no matches in `crates/hammer-service/src/session`; any remaining TCP matches must be TCP-private.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/src/transport/tcp/output.rs crates/hammer-service/src/session/protocol.rs
git commit -m "hammer-service(Refactor): move tcp tx commit behind push-header batch"
```

## Task 3: Add VPP scheduling facts and GSO-safe Send Goal Size

**Issue:** #17

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Test: `crates/hammer-service/tests/session_queue_dispatch.rs`

**Interfaces:**
- Consumes: `TransportSendParams` and `TransportSendFlags`.
- Produces: scheduling behavior from transport-neutral facts and Send Goal Size packetization.

- [ ] **Step 1: Add scheduling and Send Goal Size tests**

Add tests at the session queue dispatch seam:

```rust
session_tx_deschedules_without_push_header_when_send_space_is_zero
session_tx_packetizes_by_send_goal_size_without_gso_metadata
```

The zero-send-space test should assert `push_header_calls == 0` and captured graph output is empty.

The Send Goal Size test should use a fake protocol returning `send_goal_size = 12` with 24 bytes pending and assert `pushed_batches == vec![vec![(0, 12), (12, 12)]]`.

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
cargo test -p hammer-service --test session_queue_dispatch session_tx_deschedules_without_push_header_when_send_space_is_zero session_tx_packetizes_by_send_goal_size_without_gso_metadata
```

Expected: FAIL until scheduling facts and Send Goal Size behavior are wired.

- [ ] **Step 3: Implement deschedule/postpone handling in Session Runtime**

Update the TX loop to interpret `TransportSendFlags::DESCHED` and `TransportSendFlags::POSTPONE` without exposing TCP-specific state. The minimum behavior for this task:

- `snd_space == 0` with `DESCHED`: do not prepare buffers or flush output.
- `snd_space == 0` with `POSTPONE`: do not prepare buffers or flush output, and leave the session eligible for later ready handling through existing ready mechanics.
- No TCP or congestion-control code may directly manipulate graph nodes or the ready queue.

- [ ] **Step 4: Wire TCP Send Goal Size**

Update TCP `send_params` so `send_goal_size` is the payload sizing fact Session Runtime should use. Without full GSO offload support, this can equal the current MSS/output payload length. If TCP has a GSO capability bit available, compute a larger goal internally but expose only the numeric Send Goal Size.

- [ ] **Step 5: Keep GSO metadata out of Session Runtime**

Ensure GSO/offload metadata, TCP option length, and TCP header semantics are set only in TCP transport/output code. Do not add fields for them to `TransportSendParams` or `TxBatchBuffer`.

- [ ] **Step 6: Run focused tests**

Run:

```bash
cargo test -p hammer-service --test session_queue_dispatch
cargo test -p hammer-service --test tcp_output
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/src/transport/tcp/output.rs crates/hammer-service/tests/session_queue_dispatch.rs
git commit -m "hammer-service(Feat): add vpp tx scheduling facts"
```

## Task 4: Add architecture guardrails for VPP Session TX boundaries

**Issue:** #19

**Files:**
- Create: `crates/hammer-service/tests/vpp_session_tx_guardrails.rs`
- Modify: `crates/hammer-service/tests/session_queue_dispatch.rs`
- Modify: `docs/adr/0002-session-tx-commits-before-graph-visibility.md` only if implementation names intentionally differ from the approved names.

**Interfaces:**
- Consumes: final code from Tasks 1-3.
- Produces: behavior and source-level guardrails that prevent regression to the old seam.

- [ ] **Step 1: Add source-level guardrail tests**

Create `crates/hammer-service/tests/vpp_session_tx_guardrails.rs` with tests that read source files and assert forbidden patterns are absent:

```rust
#[test]
fn session_tx_external_seam_does_not_expose_prepare_cancel_commit() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/session/runtime.rs"
    ))
    .expect("read runtime source");
    assert!(!source.contains("fn prepare_tx("));
    assert!(!source.contains("fn cancel_tx("));
    assert!(!source.contains("fn commit_tx("));
}

#[test]
fn session_runtime_does_not_scan_tcp_timer_masks() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/session/protocol.rs"
    ))
    .expect("read protocol source");
    assert!(!source.contains("TCP_TIMER_COUNT"));
    assert!(!source.contains("active_timer_mask"));
    assert!(!source.contains("timer_mask"));
}
```

Add additional assertions for `refresh_tcp_timers` in session code and TCP/GSO-specific buffer API names if those risks appear in the final implementation.

- [ ] **Step 2: Run guardrail tests and verify RED if any forbidden surface remains**

Run:

```bash
cargo test -p hammer-service --test vpp_session_tx_guardrails
```

Expected: PASS only after Tasks 1-3 have removed forbidden surfaces.

- [ ] **Step 3: Add behavior guardrail coverage**

Keep or add behavior tests that prove:

- Graph visibility happens after transport commit.
- TX FIFO retention lasts until ACK cleanup.
- Custom TX does not use normal new-data packetization.
- Timer dispatch uses exact Timer Token.
- Send Goal Size is the only GSO-facing fact in Session Runtime.

These may live in existing test files if that keeps seams clearer.

- [ ] **Step 4: Run focused and package tests**

Run:

```bash
cargo test -p hammer-service --test vpp_session_tx_guardrails
cargo test -p hammer-service --test session_queue_dispatch
cargo test -p hammer-service --test tcp_output
cargo test -p hammer-service
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/tests/vpp_session_tx_guardrails.rs crates/hammer-service/tests/session_queue_dispatch.rs docs/adr/0002-session-tx-commits-before-graph-visibility.md
git commit -m "hammer-service(Test): guard vpp session tx boundaries"
```

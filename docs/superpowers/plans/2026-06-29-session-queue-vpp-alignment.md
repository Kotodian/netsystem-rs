# Session Enqueue Node VPP Alignment Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate 6 divergences from VPP session layer semantics in the session queue node, TCP TX path, and app/session interaction boundary.

**Architecture:** All changes confined to `crates/hammer-service/src/session/` and `crates/hammer-service/src/transport/tcp/`. No `hammer-adapter`, `hammer-infra`, `hammer-core` or `hammer-runtime` API changes. Existing `SessionQueueProtocol` trait stays unchanged.

**Tech Stack:** Rust 2024, `hammer_infra::vec::Vec`, `hammer_infra::pool::Pool`, `hammer_infra::fifo::Fifo`, `hammer_infra::rbtree::RbTree`. Tests via `cargo test -p hammer-service`.

## Global Constraints

- Per AGENTS.md: do **not** create intermediate payload `Vec`s or private payload copies on the TX/recovery path; retransmit from session-owned TX FIFO bytes.
- Per AGENTS.md: congestion control must **not** schedule nodes.
- Per AGENTS.md: timer expiry must dispatch the exact token supplied; no `TcpConnectionTimerKind` scans.
- Per AGENTS.md: no underscore-prefixed locals (`_value`); use bare `_`.
- Per AGENTS.md: no new business-name types like `Cursor`/`Helper`/`Util` for state records.
- Per AGENTS.md: do **not** add TCP-specific runtime/buffer APIs.
- 4-space indent, `snake_case` fn, `PascalCase` types, `SCREAMING_SNAKE_CASE` consts.
- Stay within `SessionQueueProtocol` trait surface; do not add sibling congestion nodes.

---

### Task 1: Loop-until-budget TX dispatch

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs:987-1091`
- Test: `crates/hammer-service/tests/session_queue_dispatch.rs` (new)

**Interfaces:**
- Consumes: `SessionDriverRuntime::poll_app`, `app.pending_send_len`, `SessionQueueProtocol::{tx_offset, tx_payload_len, prepare_tx, commit_tx}`
- Produces: same `dispatch_session_queue_pending` signature; introduces `const DEFAULT_TX_DISPATCH_BUDGET: usize = 64;` private to the module.

- [ ] **Step 1: Write the failing test**

`crates/hammer-service/tests/session_queue_dispatch.rs`:
```rust
use std::time::Instant;
use hammer_adapter::{DataPlaneRuntime, DataWorkerId, NodeId};
use hammer_service::session::runtime::{
    SessionDriverRuntime, dispatch_session_queue_for_ticks,
};
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p hammer-service --test session_queue_dispatch`
Expected: FAIL — current code emits one segment per dispatch; `captured_packets.len() == 1`.

- [ ] **Step 3: Replace the break-bounded inner loop with a budget-bounded loop**

In `crates/hammer-service/src/session/runtime.rs`, edit `dispatch_session_queue_pending`:
1. Add `const DEFAULT_TX_DISPATCH_BUDGET: usize = 64;` near `DEFAULT_SESSION_POOL_CAPACITY`.
2. Replace the `#[allow(clippy::never_loop)] loop { ... break; }` (lines 987-1091) with a budget-bounded loop.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p hammer-service --test session_queue_dispatch`
Expected: PASS — 25 segments captured in one dispatch.

- [ ] **Step 5: Run all session tests**

Run: `cargo test -p hammer-service --lib session::`
Expected: PASS, no regressions.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs \
        crates/hammer-service/tests/session_queue_dispatch.rs
git commit -m "hammer-runtime(Refactor): loop-until-budget TX dispatch in session queue"
```

---

### Task 2: Eliminate Attachment Vec clone

**Files:**
- Modify: `crates/hammer-service/src/session/node.rs:211-250`
- Test: `crates/hammer-service/tests/session_queue_node_attach.rs` (new)

**Interfaces:**
- API stable; `session_queue_node_process` keeps the same signature.
- Introduces `SESSION_QUEUE_NODES` iteration by index instead of `node.clone()`.

- [ ] **Step 1: Write the failing test**

`crates/hammer-service/tests/session_queue_node_attach.rs`:
```rust
#[test]
fn session_queue_node_dispatch_does_not_clone_attachments() {
    // Register N attachments, schedule the session-queue node, capture the
    // dispatch fn hit count via a thread_local counter the test reads after
    // process(). Assert N attachments dispatched and zero `Vec::clone()` calls.
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p hammer-service --test session_queue_node_attach`
Expected: FAIL — clone path counter > 0.

- [ ] **Step 3: Rewrite the dispatch loop to iterate by index**

In `crates/hammer-service/src/session/node.rs`, rewrite `session_queue_node_process` to iterate using a `loop { node.get(index).copied(); break if None; }` pattern instead of cloning the entire Vec.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p hammer-service --test session_queue_node_attach`
Expected: PASS — clone counter stays 0.

- [ ] **Step 5: Run all service tests**

Run: `cargo test -p hammer-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/session/node.rs \
        crates/hammer-service/tests/session_queue_node_attach.rs
git commit -m "hammer-runtime(Refactor): idx-iterate session queue attachments"
```

---

### Task 3: Remove unsafe split-borrow repetition

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs:905-1093`
- Test: existing tests cover correctness; add a structural borrow test in the existing in-module test suite.

**Interfaces:**
- Consumes: `SessionDriverRuntime<St, Seg>`, `SessionQueueProtocol` trait methods.
- Produces: new private struct `SessionTxWork<'a, St, Seg>` that packages the safe split borrow into a single handle.

- [ ] **Step 1: Write the failing test**

In `crates/hammer-service/src/session/runtime.rs` `#[cfg(test)] mod tests`, add a test that `SessionTxWork` compiles without raw pointers.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p hammer-service --lib session::tests::session_tx_work`
Expected: FAIL.

- [ ] **Step 3: Introduce `SessionTxWork` with a safe split-borrow API**

```rust
struct SessionTxWork<'a, St, Seg: Segment> {
    state: &'a mut St,
    app: &'a SessionAppRuntime<Seg>,
    buffers: &'a DataPlaneBuffers,
    timer_wheel: &'a mut TimerWheel1t2w2048sl<u32>,
    ready: &'a mut SessionReadyQueue,
    session_id: SessionId,
}
```

- [ ] **Step 4: Refactor `dispatch_session_queue_pending` to use `SessionTxWork`**

Replace the 7 unsafe blocks in the TX loop with safe `SessionTxWork` access.

- [ ] **Step 5: Run all session tests**

Run: `cargo test -p hammer-service --lib session::`
Expected: PASS — same behavior, no `unsafe` in the TX dispatch path.

- [ ] **Step 6: Check for residual `unsafe` in dispatch path**

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs
git commit -m "hammer-runtime(Refactor): replace unsafe split-borrow with SessionTxWork"
```

---

### Task 4: Lazy `has_pending_tx` evaluation

**Files:**
- Modify: `crates/hammer-service/src/session/protocol.rs:60-137`
- Modify: `crates/hammer-service/src/session/runtime.rs:905-918`
- Test: in-module test.

**Interfaces:**
- `SessionQueueControlContext::new` keeps the same signature.
- Adds `SessionQueueControlContext::refresh_has_pending_tx(&self, bool)` private method.
- Changes `has_pending_tx: bool` to `has_pending_tx: Cell<bool>`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn has_pending_tx_is_lazy_and_caches() { ... }
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Make `has_pending_tx` a `Cell<bool>`** and add `refresh_has_pending_tx`.

- [ ] **Step 4: Update `SessionTxWork::context` (Task 3) to call `refresh_has_pending_tx`**

- [ ] **Step 5: Run all session tests**

Run: `cargo test -p hammer-service --lib session::`

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/session/protocol.rs \
        crates/hammer-service/src/session/runtime.rs
git commit -m "hammer-runtime(Refactor): lazy has_pending_tx in session context"
```

---

### Task 5: Eliminate RX queue pool churn

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs:42-47,736-779`
- Test: in-module test.

**Interfaces:**
- `flush_session_rx` keeps its pub(crate) signature. Removes the pool slot deallocation on queue empty.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn flush_session_rx_reuses_pool_slot() {
    // enqueue then flush, then re-enqueue; assert pool length stayed at 1.
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Make `flush_session_rx` retain the pool slot** when the queue empties. Removal only on `release_session_rx`.

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Run all session tests**

Run: `cargo test -p hammer-service --lib session::`

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs
git commit -m "hammer-runtime(Refactor): retain rx queue pool slot across flush"
```

---

### Task 6: Simplify two-stage RX copy

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs:42-779`
- Modify: `crates/hammer-service/src/session/app.rs:143-164`
- Test: `crates/hammer-service/tests/session_rx_flush.rs` (new)

**Interfaces:**
- Consumes: `SessionRxQueue`, `AppSession::enqueue_rx`, `DataPlaneBuffers::chain`
- Produces: inline in-order RX copy directly to app `rx_fifo`, simplify `SessionRxQueue`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn in_order_rx_bypasses_session_rx_queue() { ... }
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Refactor `enqueue_rx` to fast-path in-order data** — offset==0 goes straight to `app.enqueue_rx` and frees the buffer.

- [ ] **Step 4: Add `copy_rx_from_buffer` to `SessionAppRuntime`**

- [ ] **Step 5: Update existing callers** (no change needed — same signature)

- [ ] **Step 6: Remove the `SessionRxQueue.delivered` FIFO field**

- [ ] **Step 7: Run all tests**

Run: `cargo test -p hammer-service`

- [ ] **Step 8: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs \
        crates/hammer-service/src/session/app.rs \
        crates/hammer-service/tests/session_rx_flush.rs
git commit -m "hammer-runtime(Refactor): direct in-order RX to app fifo, OOO cache simplified"
```

---

### Task 7: Final verification and cleanup

**Files:** All modified files.

- [ ] **Step 1: Run full workspace tests**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

- [ ] **Step 3: Run rustfmt**

Run: `cargo fmt --all`

- [ ] **Step 4: Clean and commit**

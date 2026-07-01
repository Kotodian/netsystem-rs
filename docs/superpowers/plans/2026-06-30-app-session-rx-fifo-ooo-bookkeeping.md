# AppSession RX FIFO OOO Bookkeeping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让生产 `AppSession` 创建出的 `rx_fifo` 默认启用 OOO bookkeeping，使 TCP 乱序 RX 写入 session/app 边界时不会因为 FIFO 未启用 OOO 表而直接失败。

**Architecture:** 只在 app/session 边界的 RX FIFO 上启用 `Fifo::enable_ooo()`，因为 RX 方向是 transport -> app，TCP 可能向 session FIFO 写入乱序 payload；TX FIFO 仍保持顺序 FIFO，不启用 OOO。这个计划只修复“OOO bookkeeping 不存在导致 `enqueue_ooo` 返回 `Err(())`”的问题，不修复 `Fifo::enqueue_ooo` 目前推进 visible tail 的语义问题；后者应由第二个 FIFO OOO 语义计划继续处理。

**Tech Stack:** Rust 2024, `hammer-runtime::app::AppSession`, `hammer_infra::fifo::Fifo`, tests via `cargo test -p hammer-runtime`.

---

## Defect

当前 `crates/hammer-runtime/src/app/session.rs` 的 `AppSession::new_in_segment` 创建 `rx_fifo` 后没有调用 `enable_ooo()`：

```rust
let rx_fifo = Arc::new(
    Fifo::<S>::new(seg.clone(), config.fifo_capacity)
        .map_err(|_| HammerError::internal("invalid rx fifo capacity"))?,
);
```

但 TCP/session 的乱序 RX 路径会调用：

```rust
session.rx_fifo().enqueue_ooo(offset, bytes)
```

`Fifo::enqueue_ooo` 在 OOO bookkeeping 缺失时会直接返回 `Err(())`。结果是生产 app session 遇到 TCP 乱序 payload 时，session RX enqueue 失败，TCP 后续 SACK/ACK 处理无法基于已接收乱序数据继续推进。

## Scope

In scope:
- 给 `AppSession::new_in_segment` 创建的 `rx_fifo` 启用 OOO bookkeeping。
- 增加 runtime 单测，证明 RX FIFO 支持 `enqueue_ooo`。
- 增加保护性单测，证明 TX FIFO 仍不启用 OOO，避免无意义状态和边界膨胀。

Out of scope:
- 不修 `Fifo::enqueue_ooo` 的 visible tail 语义。
- 不修 OOO RX payload `Vec` 临时拷贝。
- 不改 TCP receive window。
- 不新增跨层 API。

## File Map

- `crates/hammer-runtime/src/app/session.rs`
  - 修改 `AppSession::new_in_segment` 的 RX FIFO 构造逻辑。
  - 在现有 `#[cfg(test)] mod tests` 中新增两个测试。

## Tasks

### Task 1: Add RED Tests For AppSession RX OOO Bookkeeping

**Files:**
- Modify: `crates/hammer-runtime/src/app/session.rs`

- [ ] **Step 1: Add failing test for RX FIFO OOO support**

在 `crates/hammer-runtime/src/app/session.rs` 的 `#[cfg(test)] mod tests` 里追加：

```rust
#[test]
fn app_session_rx_fifo_enables_ooo_bookkeeping() {
    let session = new_session(AppSessionConfig::new(64, 4), 1);

    let result = session.rx_fifo().enqueue_ooo(5, b"world");

    assert!(result.is_ok(), "app rx fifo should enable OOO bookkeeping");
    assert_eq!(session.rx_fifo().ooo_enqueued(), 1);
}
```

- [ ] **Step 2: Add guard test that TX FIFO remains ordered-only**

同一个 test module 继续追加：

```rust
#[test]
fn app_session_tx_fifo_does_not_enable_ooo_bookkeeping() {
    let session = new_session(AppSessionConfig::new(64, 4), 1);

    let result = session.tx_fifo().enqueue_ooo(5, b"world");

    assert!(result.is_err(), "app tx fifo should remain ordered-only");
    assert_eq!(session.tx_fifo().ooo_enqueued(), 0);
}
```

- [ ] **Step 3: Run the focused RED test**

Run:

```bash
cargo test -p hammer-runtime --lib app::session::tests::app_session_rx_fifo_enables_ooo_bookkeeping -- --exact
```

Expected:
- FAIL。
- 失败原因是 `result.is_ok()` assertion 失败，因为当前 `rx_fifo` 未启用 OOO bookkeeping。

- [ ] **Step 4: Run the TX guard test**

Run:

```bash
cargo test -p hammer-runtime --lib app::session::tests::app_session_tx_fifo_does_not_enable_ooo_bookkeeping -- --exact
```

Expected:
- PASS。
- 这个测试锁定 TX FIFO 不开启 OOO 的边界，防止修复时把 RX/TX 都一刀切打开。

### Task 2: Enable OOO Bookkeeping Only On RX FIFO

**Files:**
- Modify: `crates/hammer-runtime/src/app/session.rs`

- [ ] **Step 1: Replace RX FIFO construction**

在 `AppSession::new_in_segment` 中，把当前 RX FIFO 构造：

```rust
let rx_fifo = Arc::new(
    Fifo::<S>::new(seg.clone(), config.fifo_capacity)
        .map_err(|_| HammerError::internal("invalid rx fifo capacity"))?,
);
```

替换为：

```rust
let mut rx_fifo = Fifo::<S>::new(seg.clone(), config.fifo_capacity)
    .map_err(|_| HammerError::internal("invalid rx fifo capacity"))?;
rx_fifo.enable_ooo();
let rx_fifo = Arc::new(rx_fifo);
```

不要修改 TX FIFO 构造，保持：

```rust
let tx_fifo = Arc::new(
    Fifo::<S>::new(seg.clone(), config.fifo_capacity)
        .map_err(|_| HammerError::internal("invalid tx fifo capacity"))?,
);
```

- [ ] **Step 2: Run focused GREEN tests**

Run:

```bash
cargo test -p hammer-runtime --lib app::session::tests::app_session_rx_fifo_enables_ooo_bookkeeping -- --exact
cargo test -p hammer-runtime --lib app::session::tests::app_session_tx_fifo_does_not_enable_ooo_bookkeeping -- --exact
```

Expected:
- Both PASS。

### Task 3: Regression Verification

**Files:**
- Test only.

- [ ] **Step 1: Run all app session tests**

Run:

```bash
cargo test -p hammer-runtime --lib app::session
```

Expected:
- PASS。
- 现有 send/recv、RX event、TX dequeue notification、clear、invalid capacity、event queue capacity 测试不回退。

- [ ] **Step 2: Run full hammer-runtime tests**

Run:

```bash
cargo test -p hammer-runtime
```

Expected:
- PASS。

- [ ] **Step 3: Format check**

Run:

```bash
cargo fmt --all -- --check
```

Expected:
- PASS。

### Task 4: Commit

**Files:**
- Modify: `crates/hammer-runtime/src/app/session.rs`

- [ ] **Step 1: Commit tests and fix together**

Run:

```bash
git add crates/hammer-runtime/src/app/session.rs
git commit -m "hammer-runtime(Fix): enable OOO bookkeeping on app rx fifo"
```

## Risk And Follow-up

This change makes TCP OOO RX stop failing at the session FIFO boundary, but it also means the existing `Fifo::enqueue_ooo` semantics become reachable in production. The next plan must fix FIFO OOO storage semantics so future bytes do not advance app-visible tail before gaps are filled.

Recommended landing strategy:
- Implement this plan and the FIFO OOO visible-tail fix in the same branch.
- Do not treat this plan alone as a complete TCP OOO RX fix.

## Completion Criteria

- `AppSession::new_in_segment` enables OOO only for `rx_fifo`.
- `app_session_rx_fifo_enables_ooo_bookkeeping` passes.
- `app_session_tx_fifo_does_not_enable_ooo_bookkeeping` passes.
- `cargo test -p hammer-runtime` passes.
- `cargo fmt --all -- --check` passes.

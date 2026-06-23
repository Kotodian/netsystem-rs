# TCP Delayed ACK Persist Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补齐 established 主路径里的 delayed ACK 和 zero-window persist，使 ACK 与 probe0 都通过现有 session timer + `TcpSegment` 输出路径完成。

**Architecture:** 这份计划依赖 `2026-06-23-tcp-session-tx-recovery-timestamps.md` 已经完成，也就是 session 已经持有 TX 字节。delayed ACK / persist 都只扩展 `TcpConnection` 私有状态和现有 `TcpTimerState`，不新增新的 runtime carrier，不把 TCP 选项泄漏到 session/runtime。

**Tech Stack:** Rust 2024, `hammer-service::{session,transport::tcp}`, `hammer-core::protocol::tcp`, `hammer-infra`, `third_party/vpp`.

---

## Scope

这份计划只做：

1. clean in-order payload 的 delayed ACK policy；
2. zero-window 且存在 unsent session-owned TX bytes 时的 persist arm/probe；
3. exact timer token dispatch。

这份计划**不**做 keepalive，也不做 TIME_WAIT。

## File Structure

**Modify**

- `crates/hammer-service/src/transport/tcp/connection.rs`
  - 扩展 `TcpTimerState` 的私有字段，补 delayed ACK / persist 逻辑。
- `crates/hammer-service/src/session/runtime.rs`
  - 复用现有 retained TX queue 查询 unsent bytes，不加 TCP 专用 runtime API。
- `crates/hammer-service/src/session/protocol.rs`
  - 只在已有 protocol context 上复用通用 timer/ready/buffer 能力。

**Test**

- `crates/hammer-service/tests/session_runtime.rs`
- `crates/hammer-service/tests/tcp_connection_state.rs`

## 最终数据结构

不新增新的业务类型，只扩展现有私有 `TcpTimerState`：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpTimerState {
    active: TcpConnectionTimerKind,
    pending: TcpConnectionTimerKind,
    delayed_ack_pending: bool,
    delayed_ack_budget: u8,
    persist_backoff: u8,
    persist_probe_pending: bool,
}
```

语义：

- `delayed_ack_pending`：已经 arm 了 `DELAYED_ACK`，等待超时或第二个 clean segment。
- `delayed_ack_budget`：自上一次立即 ACK 之后累计的 clean in-order segment 数。
- `persist_backoff`：`PERSIST` 重发回退指数。
- `persist_probe_pending`：当前 token expiry 需要发 probe，而不是重新扫描 timer kind。

## Approval Gate

### 审批 A：只扩展 `TcpTimerState`，不新增 `TcpDelayedAckState` / `TcpPersistState`

涉及文件：

- `crates/hammer-service/src/transport/tcp/connection.rs`

拟改结果：

- 在现有 `TcpTimerState` 上加 4 个私有字段。
- 不新增第二个 timer wrapper，也不新增 runtime/scheduler API。

原因：

- delayed ACK / persist 都是 timer policy，本来就属于当前这层私有状态。
- 这样能把新增类型数量压到最少。

## Task 1: clean in-order payload 的 delayed ACK

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/tests/tcp_connection_state.rs`

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_connection_state.rs` 增加：

```rust
#[test]
fn tcp_first_clean_in_order_payload_arms_delayed_ack() {
    let mut connection = established_connection_for_test();
    let packet = in_order_payload_packet_for_test(5);

    let output = connection.receive_data_for_test(packet).expect("receive");

    assert!(output.is_none());
    assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::DELAYED_ACK));
}

#[test]
fn tcp_second_clean_in_order_payload_forces_immediate_ack() {
    let mut connection = established_connection_for_test();
    let first = in_order_payload_packet_for_test(5);
    let second = next_in_order_payload_packet_for_test(5);

    let _ = connection.receive_data_for_test(first).expect("first");
    let output = connection.receive_data_for_test(second).expect("second");

    assert!(output.is_some());
    assert!(!connection.tcp_timer_is_active(TcpConnectionTimerKind::DELAYED_ACK));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test tcp_connection_state tcp_first_clean_in_order_payload_arms_delayed_ack -- --exact`

Expected: FAIL，因为当前 clean in-order payload 到达后会立刻 ACK。

- [ ] **Step 3: 实现最小逻辑**

在 `crates/hammer-service/src/transport/tcp/connection.rs` 增加：

```rust
fn on_clean_in_order_payload(&mut self) -> bool {
    self.timers.delayed_ack_budget = self.timers.delayed_ack_budget.saturating_add(1);
    if self.timers.delayed_ack_budget >= 2 {
        self.timers.delayed_ack_pending = false;
        self.tcp_timer_reset(TcpConnectionTimerKind::DELAYED_ACK);
        self.timers.delayed_ack_budget = 0;
        return true;
    }
    self.timers.delayed_ack_pending = true;
    self.tcp_timer_set(TcpConnectionTimerKind::DELAYED_ACK);
    false
}
```

并在 `receive_data()` 中只对这些场景跳过 delayed ACK：

- gap / overlap / DSACK
- FIN
- immediate ACK required by duplicate ACK/SACK

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service --test tcp_connection_state tcp_second_clean_in_order_payload_forces_immediate_ack -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/tests/tcp_connection_state.rs
git commit -m "tcp(Feat): add delayed ack policy for clean in-order payload"
```

## Task 2: zero-window persist arm 与 probe0

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-service/tests/session_runtime.rs`

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/session_runtime.rs` 增加：

```rust
#[test]
fn tcp_zero_window_with_unsent_session_tx_arms_persist_timer() {
    let (mut driver, session_id) = session_with_session_tx_for_test();
    close_peer_window(&mut driver, session_id);

    dispatch_session_queue_once_for_test(&mut driver).expect("dispatch");

    let connection = driver.session(session_id).expect("session");
    assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::PERSIST));
}

#[test]
fn tcp_persist_timer_emits_probe_without_releasing_confirmed_bytes() {
    let (runtime, mut driver, session_id) = zero_window_session_with_unsent_tx_for_test();
    arm_persist_for_test(&mut driver, session_id);

    expire_timer_for_test(&runtime, &mut driver, session_id, TcpConnectionTimerKind::PERSIST)
        .expect("expire");

    assert!(driver.has_session_tx(session_id));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test session_runtime tcp_zero_window_with_unsent_session_tx_arms_persist_timer -- --exact`

Expected: FAIL，因为当前 `PERSIST` 没有真实语义。

- [ ] **Step 3: 实现最小逻辑**

在 `connection.rs` 收口成：

```rust
fn should_arm_persist(&self, pending_unsent: usize) -> bool {
    self.state == TcpState::Established
        && self.snd_wnd == 0
        && pending_unsent != 0
}

fn persist_probe_len(&self, pending_unsent: usize) -> usize {
    pending_unsent.min(1)
}
```

在 `handle_ready_session()` / `handle_timer_expiry()` 里复用这套逻辑：

- ready path 只负责 arm `PERSIST`
- exact `PERSIST` token expiry 只发 1-byte probe
- probe 不推进 `snd_una`
- probe 不释放 retained TX bytes

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test session_runtime tcp_zero_window_with_unsent_session_tx_arms_persist_timer -- --exact
cargo test -p hammer-service --test session_runtime tcp_persist_timer_emits_probe_without_releasing_confirmed_bytes -- --exact
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "tcp(Feat): add zero-window persist probe handling"
```

## Self-Review

- **Spec coverage:** 覆盖 delayed ACK 与 persist，两者都复用现有 session timer 路径。
- **Placeholder scan:** 无 TODO/TBD。
- **Type consistency:** 没有新 public type；只扩展 `TcpTimerState` 私有字段。

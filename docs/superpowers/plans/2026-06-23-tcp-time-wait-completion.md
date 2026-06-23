# TCP Time-Wait Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `TIME_WAIT` 补到可用状态：进入后保留 tuple，duplicate FIN 重新 ACK 并 re-arm timer，expiry 后由 runtime 清理 session/index。

**Architecture:** 不新增 `time_wait.rs` 以外的第二层状态容器，也不新增 runtime 的 TCP 私有生命周期 API。`TcpConnection` 只给出协议事实和 `TIME_WAIT` timer 决策；session/runtime 继续负责 exact token 调度、索引刷新和 session 最终释放。

**Tech Stack:** Rust 2024, `hammer-service::{session,transport::tcp}`, `hammer-core::protocol::tcp`, `hammer-infra`, `third_party/vpp`.

---

## Scope

这份计划只做：

1. `FinWait1/FinWait2/Closing -> TimeWait` 后的 timer arm；
2. `TIME_WAIT` 收到 duplicate FIN / old ACK / RST 的处理；
3. timer expiry 之后 tuple/session cleanup。

## File Structure

**Modify**

- `crates/hammer-service/src/transport/tcp/connection.rs`
- `crates/hammer-service/src/transport/tcp/mod.rs`
- `crates/hammer-service/src/session/runtime.rs`

**Test**

- `crates/hammer-service/tests/tcp_time_wait.rs`

## 最终数据结构

这份计划不新增新的业务类型：

- 继续复用现有 `TcpConnectionTimerKind::TIME_WAIT`
- 继续复用 `TcpState::TimeWait`
- 继续复用 runtime 现有 `close_session()` / `refresh_session_route()` / route index cleanup

唯一的数据结构要求是：`tcp_timer_ticks()` 必须能为 `TIME_WAIT` 给出固定 ticks。

## Approval Gate

### 审批 A：不新增 `TcpTimeWaitState`

涉及文件：

- `crates/hammer-service/src/transport/tcp/connection.rs`

拟改结果：

- `TIME_WAIT` 只复用现有 `TcpConnection` 字段与 `TIME_WAIT` timer。
- 不新增专门的 `TcpTimeWaitState` wrapper。

原因：

- 当前逻辑只需要定时保留、duplicate FIN 回 ACK、expiry close，不值得再加一个状态容器。

## Task 1: 进入 TIME_WAIT 时 arm timer 并保留 tuple

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Test: `crates/hammer-service/tests/tcp_time_wait.rs`

- [ ] **Step 1: 写失败测试**

创建 `crates/hammer-service/tests/tcp_time_wait.rs`：

```rust
#[test]
fn tcp_fin_path_enters_time_wait_and_retains_tuple_until_expiry() {
    let (mut driver, session_id, local, remote) = time_wait_session_for_test();

    drive_fin_ack_to_time_wait(&mut driver, session_id).expect("drive");

    let state = driver.session(session_id).expect("session");
    assert_eq!(state.state(), TcpState::TimeWait);
    assert!(driver.session_route_by_tuple(local, remote).is_some());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test tcp_time_wait tcp_fin_path_enters_time_wait_and_retains_tuple_until_expiry -- --exact`

Expected: FAIL

- [ ] **Step 3: 实现最小逻辑**

在 `connection.rs` 进入 `TimeWait` 的所有分支里统一 arm timer：

```rust
self.state = TcpState::TimeWait;
self.tcp_timer_set(TcpConnectionTimerKind::TIME_WAIT);
```

在 `tcp_timer_ticks()` 里补：

```rust
if timer == TcpConnectionTimerKind::TIME_WAIT {
    return Some(TCP_TIME_WAIT_TICKS);
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service --test tcp_time_wait tcp_fin_path_enters_time_wait_and_retains_tuple_until_expiry -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/tests/tcp_time_wait.rs
git commit -m "tcp(Feat): retain tuples across time-wait"
```

## Task 2: duplicate FIN re-ACK + re-arm

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/tests/tcp_time_wait.rs`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn tcp_time_wait_duplicate_fin_rearms_timer_and_reemits_ack() {
    let (mut connection, packet) = duplicate_fin_for_time_wait_test();

    let output = connection.receive_rcv_process_for_test(packet).expect("receive");

    assert!(output.is_some());
    assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::TIME_WAIT));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test tcp_time_wait tcp_time_wait_duplicate_fin_rearms_timer_and_reemits_ack -- --exact`

Expected: FAIL，因为当前 `receive_time_wait()` 几乎什么都不做。

- [ ] **Step 3: 实现最小逻辑**

在 `receive_time_wait()` 里改成：

```rust
if packet.flags.contains(TcpSegmentFlags::RST) {
    self.close_reason = Some(TcpCloseReason::RemoteReset);
    self.state = TcpState::Closed;
    return Ok(None);
}

if packet.flags.contains(TcpSegmentFlags::FIN) {
    self.tcp_timer_set(TcpConnectionTimerKind::TIME_WAIT);
    return Ok(Some(self.control_segment(packet, TcpSegmentFlags::ACK, None)));
}

Ok(None)
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service --test tcp_time_wait tcp_time_wait_duplicate_fin_rearms_timer_and_reemits_ack -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/tests/tcp_time_wait.rs
git commit -m "tcp(Fix): ack duplicate fin in time-wait"
```

## Task 3: TIME_WAIT expiry 后由 runtime 清理 tuple 与 session

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-service/tests/tcp_time_wait.rs`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn tcp_time_wait_timer_expiry_releases_tuple_and_session() {
    let (runtime, mut driver, session_id, local, remote) = time_wait_session_for_runtime_test();

    expire_time_wait_timer_for_test(&runtime, &mut driver, session_id).expect("expire");

    assert!(driver.session(session_id).is_none());
    assert!(driver.session_route_by_tuple(local, remote).is_none());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test tcp_time_wait tcp_time_wait_timer_expiry_releases_tuple_and_session -- --exact`

Expected: FAIL

- [ ] **Step 3: 实现最小逻辑**

在 exact `TIME_WAIT` token expiry 路径里收口：

```rust
if kind == TcpConnectionTimerKind::TIME_WAIT {
    self.close_reason = Some(TcpCloseReason::Timeout);
    self.state = TcpState::Closed;
    return Ok(());
}
```

然后复用现有 `refresh_session_route()` / `close_session()` 收尾，不新增 `replace_session_state()` 一类生命周期 API。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service --test tcp_time_wait`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/tests/tcp_time_wait.rs
git commit -m "tcp(Feat): close time-wait sessions on timer expiry"
```

## Self-Review

- **Spec coverage:** 覆盖了 retain tuple、duplicate FIN re-ACK、expiry cleanup。
- **Placeholder scan:** 无 TODO/TBD。
- **Type consistency:** 没有新增 `TcpTimeWaitState` 或 runtime 私有 TCP 生命周期 API。

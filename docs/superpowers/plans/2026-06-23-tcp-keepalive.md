# TCP Keepalive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不破坏现有 transport/session 分层的前提下，给 TCP established idle path 补上可配置的 keepalive probe 与 probe-limit close。

**Architecture:** keepalive 是连接私有协议 policy，不是 session/runtime 的新状态机。实现上只扩展 `TcpConnection` 私有字段与现有 `TcpConnectionTimerKind::KEEP_ALIVE`；runtime 只负责 exact token expiry 调度。默认 keepalive 关闭，只有 connection 上存在显式配置时才 arm。

**Tech Stack:** Rust 2024, `hammer-service::transport::tcp`, `hammer-core::protocol::tcp`, `hammer-service::session`.

---

## Scope

这份计划只做 keepalive：

1. idle established connection arm keepalive；
2. timeout 发空 probe；
3. probe limit 到达后关闭 connection。

## File Structure

**Modify**

- `crates/hammer-service/src/transport/tcp/connection.rs`
- `crates/hammer-runtime/tests/tcp_control_plane.rs`

**Test**

- `crates/hammer-service/tests/tcp_connection_state.rs`

## 最终数据结构

不新增新业务类型，只在 `TcpConnection<C>` 上增加 4 个私有字段：

```rust
keepalive_idle_ticks: Option<u64>,
keepalive_interval_ticks: u64,
keepalive_probe_limit: u8,
keepalive_probes_sent: u8,
```

语义：

- `keepalive_idle_ticks == None` 表示关闭 keepalive。
- `keepalive_interval_ticks` / `keepalive_probe_limit` 只有 keepalive 开启时生效。
- `keepalive_probes_sent` 只由 exact `KEEP_ALIVE` timer expiry 推进。

## Approval Gate

### 审批 A：keepalive 配置只放在 `TcpConnection` 私有字段，不改 session/runtime trait

涉及文件：

- `crates/hammer-service/src/transport/tcp/connection.rs`

拟改结果：

- 只在连接私有状态里加 4 个字段。
- 不新增 `SessionQueueProtocol` 方法，不新增 control-plane 中间 carrier。

原因：

- keepalive 是 transport policy，不属于 session/runtime 通用抽象。
- 当前目标是把协议行为落好，不扩散控制面设计。

## Task 1: keepalive idle probe

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/tests/tcp_connection_state.rs`

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_connection_state.rs` 增加：

```rust
#[test]
fn tcp_keepalive_timer_emits_probe_on_idle_established_connection() {
    let mut connection = established_connection_with_keepalive_for_test(30, 10, 3);
    arm_keepalive_for_test(&mut connection);

    let output = connection
        .on_tcp_timer_expiry(TcpConnectionTimerKind::KEEP_ALIVE);

    assert!(output.is_some());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test tcp_connection_state tcp_keepalive_timer_emits_probe_on_idle_established_connection -- --exact`

Expected: FAIL，因为当前 `KEEP_ALIVE` 没有实现。

- [ ] **Step 3: 实现最小逻辑**

在 `crates/hammer-service/src/transport/tcp/connection.rs` 增加：

```rust
fn keepalive_enabled(&self) -> bool {
    self.keepalive_idle_ticks.is_some() && self.state == TcpState::Established
}

fn keepalive_probe_segment(&mut self) -> CoreResult<TcpSegment> {
    let local = self.local().ok_or_else(|| CoreError::internal("keepalive requires local"))?;
    Ok(TcpSegment::new(
        local,
        self.remote(),
        self.snd_una().wrapping_sub(1),
        self.rcv_nxt(),
        self.advertised_receive_window(self.rcv_wnd),
        self.output_flags(TcpSegmentFlags::ACK),
        self.local_capabilities(),
        None,
        None,
        None,
        0,
    ))
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service --test tcp_connection_state tcp_keepalive_timer_emits_probe_on_idle_established_connection -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/tests/tcp_connection_state.rs
git commit -m "tcp(Feat): add keepalive idle probe"
```

## Task 2: probe limit close

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/tests/tcp_connection_state.rs`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn tcp_keepalive_probe_limit_closes_unresponsive_connection() {
    let mut connection = established_connection_with_keepalive_for_test(30, 10, 2);
    arm_keepalive_for_test(&mut connection);

    let _ = connection.on_tcp_timer_expiry(TcpConnectionTimerKind::KEEP_ALIVE);
    let _ = connection.on_tcp_timer_expiry(TcpConnectionTimerKind::KEEP_ALIVE);
    let _ = connection.on_tcp_timer_expiry(TcpConnectionTimerKind::KEEP_ALIVE);

    assert_eq!(connection.state(), TcpState::Closed);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test tcp_connection_state tcp_keepalive_probe_limit_closes_unresponsive_connection -- --exact`

Expected: FAIL

- [ ] **Step 3: 实现最小逻辑**

```rust
if self.keepalive_probes_sent >= self.keepalive_probe_limit {
    self.close_reason = Some(TcpCloseReason::Timeout);
    self.state = TcpState::Closed;
    self.tcp_timer_reset(TcpConnectionTimerKind::KEEP_ALIVE);
    return None;
}

self.keepalive_probes_sent = self.keepalive_probes_sent.saturating_add(1);
```

并在任意收到合法 ACK / payload 时把 `keepalive_probes_sent` 清零。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service --test tcp_connection_state tcp_keepalive_probe_limit_closes_unresponsive_connection -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/tests/tcp_connection_state.rs
git commit -m "tcp(Feat): close unresponsive keepalive connections"
```

## Self-Review

- **Spec coverage:** 覆盖 idle probe 与 probe limit close。
- **Placeholder scan:** 无 TODO/TBD。
- **Type consistency:** 没有新增新的公开配置类型，只是连接私有字段。

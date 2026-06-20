# Task 2 Brief

- Source plan: `docs/superpowers/plans/2026-06-19-tcp-sack-dsack-ooo-plan.md`
- Branch: `codex/hammer-app-ring-zero-copy`
- Base commit before Task 2: `9663be4be857095bec0c2d4979ee9939906024c3`（Task 1 完成后会更新为当时的 HEAD）

---

### Task 2: 删除公开 `TcpSentSegment`，把 ACK/SACK 处理收回 recovery 私有状态

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/tests/tcp_rack_tlp.rs`（若相关覆盖已完整迁入 `recovery.rs` 的 `#[cfg(test)]`，再删除）

**Interfaces:**
- Consumes: `TcpConnection<Established, C>::commit_payload_tx`, `TcpRecoveryState::on_sack_blocks`, `TcpConnection::apply_ack`
- Produces:
  - recovery sent-record 构造回收至 `recovery.rs` 私有范围
  - `TcpConnection<Established, C>::receive_ack(...)` 在一个 ACK 事件里只做一次 cumulative ACK 清理，并在有入站 SACK 时把 blocks 喂给 recovery

- [ ] **Step 1: 先写失败测试，锁住“不能再依赖公开 sent type”**

把 recovery 行为测试搬进 `recovery.rs` 的 `#[cfg(test)] mod tests`，用私有 helper 填充 outstanding packets：

```rust
fn record_sent_for_test(
    recovery: &mut TcpRecoveryState,
    packet_number: PacketNumber,
    sequence: u32,
    end_sequence: u32,
    bytes: u32,
    sent_at: Instant,
) {
    // 这里调用 recovery 内部最终批准后的私有 sent-record 入口；
    // 断言目标是“外部测试不再依赖公开 TcpSentSegment”，不是写死最终函数签名。
}

#[test]
fn rack_marks_older_unacked_segment_lost_after_later_sack() {
    let now = Instant::now();
    let mut recovery = TcpRecoveryState::new();
    let mut controller = RecordingController::new(1_460);

    record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
    record_sent_for_test(
        &mut recovery,
        2,
        2_000,
        3_000,
        1_000,
        now + Duration::from_millis(1),
    );

    recovery.on_sack_blocks(
        ack(1_000, now + Duration::from_millis(30), 40),
        &[TcpSackBlock {
            left_edge: 2_000,
            right_edge: 3_000,
        }],
        &mut controller,
    );

    recovery.on_rack_timeout(now + Duration::from_millis(56), &mut controller);
    assert_eq!(controller.lost[0].packet_number, 1);
}
```

Run:

```bash
cargo test -p hammer-service transport::tcp::recovery::tests -- --nocapture
```

Expected: FAIL，因为当前 recovery 外部测试入口仍依赖公开 `TcpSentSegment`。

- [ ] **Step 2: 把 sent-record 构造收回 `recovery.rs` 私有范围，但不预设新类型名或最终入口签名**

要求：

```rust
// recovery 内部必须能维护这些事实：
// - packet_number
// - sequence / end_sequence
// - bytes
// - sent_at
// - retransmitted
// - is_probe
//
// 但计划阶段不预设它一定要落成 `SentPacket` 这样的新类型名。
```

`TcpSentSegment` 删除，`mod.rs` 停止 re-export。recovery 内部如何组织这些事实、`record_sent()` 最终长什么样，执行前如需新增私有业务记录或改动对外签名，必须单独审批。

- [ ] **Step 3: 让 `commit_payload_tx()` 不再公开构造 `TcpSentSegment`**

```rust
// commit_payload_tx() 的结果要求：
// - 调用点不再手工构造 `TcpSentSegment`
// - recovery 仍然拿到 packet_number / sequence range / bytes / sent_at / probe/retransmit facts
// - 具体参数形状按审批 C 最终批准的 diff 落地
```

这里顺手把公开类型名从调用点完全删掉。

- [ ] **Step 4: ACK path 直接把入站 SACK blocks 喂给 recovery，但不再沿用当前 `apply_ack()` 的双重清理语义**

```rust
// 结果要求：
// - 当前 `apply_ack()` / `ack_sent_data()` 这一层要拆开，让 ACK 事件只做一次 cumulative ACK 清理
// - `receive_ack()` 在已有 cumulative ACK 推进时，更新 snd_una / snd_wnd / RTT sample / recovery / congestion
// - 如果入站带 SACK blocks，则同一个 ACK 事件里把 blocks 交给 recovery
// - 不能先走一次当前 `apply_ack()` 的 recovery.on_ack()，再额外对同一个 ACK 调 `on_sack_blocks()`
```

要求：

- `receive_ack()` 对每个 ACK 事件只能让 recovery 跑一次 cumulative ACK 清理。
- 有 SACK blocks 时走 `on_sack_blocks()`，没有时走 `on_ack()`，不能先 `apply_ack()` 再额外 `on_sack_blocks()`。
- 连接层需要能区分“累计 ACK 前进了多少字节”和“这次 ACK 是否还带了 SACK facts”，但计划阶段不写死辅助函数名。
- 纯 SACK 不得伪装成 cumulative ACK 推进；只有累计确认前进时才允许后续 TX cleanup 前进。

- [ ] **Step 5: 验证**

Run:

```bash
cargo test -p hammer-service transport::tcp::recovery::tests -- --nocapture
rg -n "TcpSentSegment" crates/hammer-service/src crates/hammer-service/tests
```

Expected:

- recovery 测试通过。
- `TcpSentSegment` 不再出现在公开 API 或外部测试入口。

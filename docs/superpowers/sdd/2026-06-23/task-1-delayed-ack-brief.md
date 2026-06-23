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


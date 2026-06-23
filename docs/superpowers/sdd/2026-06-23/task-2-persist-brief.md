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
    let (mut driver, session_id) = session_with_retained_tx_for_test();
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

    assert!(driver.has_retained_tx(session_id));
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

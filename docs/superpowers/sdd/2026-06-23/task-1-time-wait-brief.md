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


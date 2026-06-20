## 前置任务 1：先把 typed state 恢复/回写收回现有 session 落库边界

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_rcvd.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/close_wait.rs`
- Modify: `crates/hammer-service/src/transport/tcp/fin_wait1.rs`
- Modify: `crates/hammer-service/src/transport/tcp/fin_wait2.rs`
- Modify: `crates/hammer-service/src/transport/tcp/closing.rs`
- Modify: `crates/hammer-service/src/transport/tcp/last_ack.rs`
- Modify: `crates/hammer-service/src/transport/tcp/time_wait.rs`
- Modify: `crates/hammer-service/tests/tcp_state_machine.rs`

**Interfaces:**
- Consumes:
  - `SessionDriverRuntime::session_mut(&mut self, id: SessionId) -> Option<&mut SessionEntry<S>>`
  - `SessionDriverRuntime::session_state_mut(&mut self, id: SessionId) -> Option<&mut S>`
  - `TcpConnectionState<C>` 的 typed state variant
- Produces:
  - TCP 主路径不再依赖 `take_connection()`
  - TCP 主路径不再依赖 `replace_session_state()` 做 typed state 回写

**Deliverable:**

- close / ready / timer / passive-open / established receive 这些 TCP 主路径，统一改成通过现有 `session_mut()` / `session_state_mut()` 恢复状态、执行 typed TCP 事件、原地落库。
- `listen` / `syn_sent` / `syn_rcvd` / `established` / `close_wait` / `fin_wait1` / `fin_wait2` / `closing` / `last_ack` / `time_wait` 这些 node 入口同步迁移，不能留下继续依赖 `take_connection()` 的 active main path。
- `take_connection()` 从 TCP 主路径移除；如果迁移后没有剩余合法调用，直接删除。
- `replace_session_state()` 不再作为 TCP 主路径的状态回写方式；如果 runtime 仍为其他未迁移路径保留它，也必须从 TCP 主路径完全移除。

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_state_machine.rs` 增加一个覆盖原地落库边界的测试模块，至少包含：

```rust
#[test]
fn tcp_close_path_updates_session_state_without_take_replace_flow() {
    let mut queue = test_tcp_queue();
    let session_id = queue.insert_session(established_connection_state());

    drive_close_submission(&mut queue, session_id);

    let state = queue.session_state(session_id).expect("state");
    assert!(matches!(state, TcpConnectionState::FinWait1(_)));
}

#[test]
fn tcp_syn_sent_timer_expiry_updates_session_state_without_take_replace_flow() {
    let mut queue = test_tcp_queue();
    let session_id = queue.insert_session(syn_sent_connection_state_with_retransmit_pending());

    expire_retransmit_timer(&mut queue, session_id);

    let state = queue.session_state(session_id).expect("state");
    let TcpConnectionState::SynSent(connection) = state else {
        panic!("expected syn-sent after retransmit expiry");
    };
    assert!(connection.tcp_timer_is_armed(TcpConnectionTimerKind::RETRANSMIT));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine tcp_close_path_updates_session_state_without_take_replace_flow
cargo test -p hammer-service --test tcp_state_machine tcp_syn_sent_timer_expiry_updates_session_state_without_take_replace_flow
```

Expected: FAIL

- [ ] **Step 3: 完整实现原地 typed state 落库**

在 `crates/hammer-service/src/transport/tcp/session.rs`、各状态 node 文件里把以下路径统一收口成同一种模式：

```rust
// borrow current TcpConnectionState<C> via session_mut()/session_state_mut()
// match exact typed variant
// run typed TcpConnection<S, C> event
// write resulting typed state back in place
// let runtime own timer/index/output/session-lifecycle side effects
```

实现结果要求：

- 协议侧落库只使用现有 `session_mut()` / `session_state_mut()`。
- 不新增 take/replace/transition 风格的 session state 接口。
- TCP 事件仍然由 typed `TcpConnection<S, C>` 承担，不把状态机降级回手写 raw-state 判断。
- node 主路径不能再通过 `take_connection()` 恢复 typed state。
- TCP 主路径不能再通过 `replace_session_state()` 回写下一状态。

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs \
  crates/hammer-service/src/transport/tcp/session.rs \
  crates/hammer-service/src/transport/tcp/state_machine.rs \
  crates/hammer-service/src/transport/tcp/listen.rs \
  crates/hammer-service/src/transport/tcp/syn_sent.rs \
  crates/hammer-service/src/transport/tcp/syn_rcvd.rs \
  crates/hammer-service/src/transport/tcp/established.rs \
  crates/hammer-service/src/transport/tcp/close_wait.rs \
  crates/hammer-service/src/transport/tcp/fin_wait1.rs \
  crates/hammer-service/src/transport/tcp/fin_wait2.rs \
  crates/hammer-service/src/transport/tcp/closing.rs \
  crates/hammer-service/src/transport/tcp/last_ack.rs \
  crates/hammer-service/src/transport/tcp/time_wait.rs \
  crates/hammer-service/tests/tcp_state_machine.rs
git commit -m "tcp(Refactor): restore typed state through existing session storage boundary"
```

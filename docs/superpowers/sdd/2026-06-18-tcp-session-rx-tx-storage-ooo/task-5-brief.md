## Task 5: RX 改成 session 持有，乱序接收走同一套存储

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`

**结果要求：**
- [ ] `SessionDriverRuntime::enqueue_rx()` 在没有 pending recv SQE 时不再直接 free buffer。
- [ ] in-order payload 进入 session RX 存储，等待 app 的 recv。
- [ ] out-of-order payload 也进入同一套 session RX 存储，用 sequence/offset 维护 hole，而不是单独 recv queue。
- [ ] 当 hole 被填平或 app 后续提交 recv SQE 时，session 把可交付部分 drain 给 app。
- [ ] FIN/close 和 RX 存储状态保持一致，不出现“payload 丢了但连接推进了”的情况。

**实现边界：**
- 乱序接收属于 session 存储能力，不属于 TCP 私有 queue。
- 对 app 暴露的仍然是现有 recv completion 语义，不改 app ring 模型。


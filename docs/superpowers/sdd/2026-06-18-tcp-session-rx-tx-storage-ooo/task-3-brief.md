## Task 3: 重写 TX 热路径，让 session TX 存储直接变成 output 输入

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`

**结果要求：**
- [ ] `flush_one_session_tx()` 不再调用 `copy_pending_send_bytes()`，也不再分配空 buffer 后 `append(Vec)`。
- [ ] session runtime 先问 TCP 当前允许发送多少字节，再从 session TX 存储里拿出对应前缀，直接交给 output 路径。
- [ ] output node 看到的仍然是 `TcpSegment` + payload buffer chain；header prepend 仍然只发生在 output node。
- [ ] 部分发送时，推进的是 session TX 存储自己的头部/偏移，不是重新复制 payload。
- [ ] 一次发送完成后，TCP 只更新连接状态、恢复状态、计时器事实和拥塞控制事实。

**实现边界：**
- 不引入 TCP 专用 runtime copy/rebuild helper。
- 不把 header 预写入 session TX 存储。
- 不新增“output segment carrier”“tx metadata wrapper”之类的中间类型。


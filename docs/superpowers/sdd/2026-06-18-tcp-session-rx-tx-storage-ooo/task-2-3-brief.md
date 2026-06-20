## Combined Task 2 + Task 3

### Why combined

Task 2 and Task 3 are executed together because Task 2 is blocked on Task 3's TX dispatch shape:

- Task 2 requires moving the app/session copy boundary into `SessionAppRuntime::drain_submissions()`.
- Once that copy moves forward, the current TX hot path in `crates/hammer-service/src/session/runtime.rs` can no longer keep using `copy_pending_send_bytes() -> Vec<u8> -> alloc_index() -> append()`.
- To avoid introducing a new generic persistent buffer sub-range/view API at the buffer layer, implement Task 2 and Task 3 together.

### Task 2: 把 app/session copy boundary 前移到 send SQE 进入 session 的时刻

**Files:**
- Modify: `crates/hammer-runtime/src/app/data.rs`
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-service/src/session/app.rs`

**结果要求：**
- [ ] `SessionAppRuntime::drain_submissions()` 处理 send SQE 时，立即把 `AppSendData` 拷入 session 自己的 TX buffer chain。
- [ ] 拷贝完成后立刻释放 `AppSendData`，不再把它挂在 pending send 队列里。
- [ ] pending send 队列只保留 session 自己的发送状态，不再存在 `copy_pending_send_bytes() -> Vec<u8>` 这种热路径接口。
- [ ] app data area 如需补能力，只补通用的“把现有 app data 直接写入现有目的存储”的能力，不加 session/TCP 专用 helper。

**实现边界：**
- 复制仍然只发生一次，而且只发生在 app/session 边界。
- session 自己的 TX 持久化形态是 buffer chain，不是 `Vec<u8>`，也不是继续持有 app data descriptor。

### Task 3: 重写 TX 热路径，让 session TX 存储直接变成 output 输入

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

### Additional controller constraints

- Build on the approved Task 1 `attach_clone/refcount` foundation in `crates/hammer-adapter/src/buffer.rs`.
- Do not introduce any new public wrapper type.
- Prefer reusing existing buffer primitives plus the approved `attach_clone`; do not add buffer range/view APIs.
- If you discover that ACK cleanup/retransmit accounting must move together with this work for correctness, keep changes narrowly compile-safe and behavior-safe, but do not proactively implement Task 4/Task 6 business-surface cleanup unless required by this combined task.

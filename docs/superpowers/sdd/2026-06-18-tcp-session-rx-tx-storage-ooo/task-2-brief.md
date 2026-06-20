## Task 2: 把 app/session copy boundary 前移到 send SQE 进入 session 的时刻

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


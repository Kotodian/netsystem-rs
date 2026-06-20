## Task 4: ACK 清理和重传改成围绕 session TX 存储工作

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`

**结果要求：**
- [ ] ACK 到来后，session 能按确认进度回收已经确认的 TX bytes / buffer chain。
- [ ] recovery 只保留 outstanding packet 的内部 accounting：包号、序号范围、发送时间、probe/loss 状态。
- [ ] 重传不再依赖私有 payload 副本，而是重新从 session TX 存储取对应未确认字节。
- [ ] 拥塞控制更新继续来自 typed TCP events，不特殊持有 TCP session/runtime 状态。

**实现边界：**
- recovery 内部允许有私有记录，但这个记录不能重新暴露成公开 sent-segment 类型。
- 这一层只处理 outstanding/loss/ack 事实，不接管调度。


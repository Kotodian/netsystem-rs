## Task 6: 删掉 `TcpSentSegment` 公开面，并清理相关测试

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/tests/tcp_rack_tlp.rs`

**结果要求：**
- [ ] `TcpSentSegment` 不再是公开类型。
- [ ] `tcp/mod.rs` 不再 re-export 该类型。
- [ ] recovery 测试改成验证 ACK、SACK、RACK、TLP 行为，不再手动构造公开 sent-segment 作为测试入口。

## 需要顺手清掉的坏面

- [ ] `SessionAppRuntime::copy_pending_send_bytes()` 及其基于 `Vec` 的测试入口
- [ ] `flush_one_session_tx()` 里的 `alloc_index() + append(Vec)`
- [ ] 所有把 `AppSendData` 延迟挂到 session TX 队列里的路径
- [ ] `TcpSentSegment` 的 re-export 和外部构造路径
- [ ] 任何为这次功能再额外引入的 TX/RX 专用 wrapper / helper / carrier / view / range API

## 验证

- `cargo test -p hammer-adapter buffer`
- `cargo test -p hammer-service session::runtime`
- `cargo test -p hammer-service transport::tcp::session`
- `cargo test -p hammer-service --test tcp_rack_tlp`
- `cargo test -p hammer-service tcp`

## 交付标准

- TX 热路径里不再出现 `AppSendData -> copy_range() -> Vec<u8> -> append()`。
- 没有 pending recv 时，RX payload 不再直接丢弃。
- 乱序接收不通过 standalone recv queue 实现。
- `TcpSentSegment` 不再出现在公开 API 或测试入口中。
- 若执行时仍然发现必须新增公开 type/API，先停下来审批，而不是边写边加。

# TCP Session RX/TX 存储与乱序接收实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把当前 TCP session 的 RX/TX 存储改成真正由 session 持有，去掉 TX 热路径里的 `Vec` 中间态，补齐乱序接收，并删除公开的 `TcpSentSegment`。

**Architecture:** app/session 仍然是唯一允许复制 payload 的边界，但复制必须前移到 session 收到 send SQE 时完成；一旦字节进入 session，自后续 TX、重传、ACK 清理、OOO、output 直到释放，全程都围绕 session 持有的 buffer chain 运转。TCP 仍然只负责序号、ACK/SACK、恢复、计时器和输出意图，output node 仍然只负责 prepend TCP header。

**Tech Stack:** Rust 2024, `hammer-adapter` buffer pool, `hammer-runtime` app ring/data area, `hammer-service` session runtime + TCP typestate state machine.

## Global Constraints

- 以当前仓库代码为准，不回退到旧设计。
- session runtime 是唯一调度者；不引入 cc sibling/node。
- 不存在 standalone recv queue；乱序接收属于 session RX 存储能力。
- app/session 是唯一允许复制 payload 的边界；session/TCP/output/recovery 之后不再产生中间 `Vec` 或私有 payload 副本。
- output node 只 prepend header；session/runtime 不知道 TCP header 细节。
- `TcpSentSegment` 必须删除出公开面；恢复记录只允许留在 recovery 内部。
- 这份计划不预先引入新的公开 wrapper/type。若现有 surface 真不够，只允许在 owning layer 补一个通用 primitive，并且必须先单独审批。

## 这次明确不做的东西

- 不加 `BufferChainRef`
- 不加 `AppSendData::copy_into_slice`
- 不加 TCP 专用 runtime/buffer helper
- 不再扩散 range/view/carrier 一类 API
- 不加 standalone recv queue

## 最终结果

1. send SQE 到达 session 时，payload 立即被拷入 session 自己的 buffer chain，然后释放 `AppSendData`。
2. `flush_one_session_tx()` 不再经过 `AppSendData -> copy_range() -> Vec<u8> -> append()` 这条链。
3. TX 正常发送、重传、ACK 清理都围绕 session 持有的 TX buffer chain 做，不额外复制 payload。
4. RX 不再在没有 pending recv 时直接丢包；in-order 和 out-of-order payload 都进入 session RX 存储。
5. `TcpSentSegment` 从公开面消失；恢复状态只保留内部 outstanding packet/accounting。

## File Map

- Modify: `crates/hammer-adapter/src/buffer.rs`
  - 收紧 buffer chain 共享/释放语义，保证 session TX/RX 存储和 output/recovery 能复用同一套通用 buffer 机制。
- Modify: `crates/hammer-runtime/src/app/data.rs`
  - 把 app data area 的现有读写能力补到足以直接写入 session buffer chain，不再经过 `Vec`。
- Modify: `crates/hammer-runtime/src/app/ring.rs`
  - 保留 app ring 语义，但不再让 session TX 热路径继续持有 `AppSendData`。
- Modify: `crates/hammer-service/src/session/app.rs`
  - 把 pending send 从 “挂着 `AppSendData` 等以后再拷” 改成 “session 已持有的 TX buffer chain 队列”。
- Modify: `crates/hammer-service/src/session/runtime.rs`
  - 重写 TX 热路径，去掉 `Vec` 中间态，改成直接从 session TX 存储产生 output buffer chain。
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
  - RX 路径接入 session RX 存储；TX 路径接入 session TX 存储与 ACK 清理。
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - 把 payload 接收和发送后的恢复更新改成 typed TCP facts，不再依赖公开 sent-segment carrier。
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
  - 删除公开 `TcpSentSegment`，只保留 recovery 私有 outstanding state。
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
  - 停止 re-export `TcpSentSegment`。
- Modify: `crates/hammer-service/tests/tcp_rack_tlp.rs`
  - 测试改成面向 recovery 行为，而不是依赖公开 sent-segment 类型。

## Task 1: 收紧 buffer 共享语义，只留 infra 里的通用能力

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs`

**结果要求：**
- [ ] buffer chain 可以在不复制 payload 的前提下被 session/output/recovery 复用。
- [ ] 共享能力留在现有 buffer infra 语义里，不新增公开 wrapper owner type。
- [ ] `free_index()` 继续是释放入口；引用计数、回 cache、回 pool 都收在 buffer 层内部，不把 C 风格 free API 扩散出去。
- [ ] 不新增 TCP 专用 buffer API。
- [ ] 不继续扩散 `with_current_chain_range` / view / range 这一类面。

**实现边界：**
- 如果现有 `BufferIndex + free_index()` 语义无法表达“复制 header、共享 backing”，只允许补一个通用 buffer clone primitive。
- 这个 primitive 必须属于 `hammer-adapter::buffer`，不能带 TCP/session 业务语义。
- 计划阶段不预设新类型名；执行前如果需要新增公开 API，先单独审批。

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

# Hammer L5 App Session Layer 设计

- **日期：** 2026-06-13
- **状态：** 草案，已按最新讨论收敛到 OSI/L5 session 分层
- **范围：** 只定义 app-facing session 公共层的边界与首轮拆分目标；不实现 TCP 发送推进，不引入 QUIC 状态机，不改变现有 app ring 行为

## 1. 背景

当前 `crates/hammer-service/src/transport/tcp/session.rs` 已经承担了两类职责：

1. TCP 专属职责：
   - TCP connection table；
   - TCP lookup id；
   - TCP timer kind；
   - 后续 TCP FIN、retransmit、persist、output pacing 的状态推进。

2. session 层通用职责：
   - app flow/backend 绑定；
   - app SQ ring 同步 polling；
   - app shutdown queue 同步 polling；
   - worker-local ready queue；
   - worker-local timer wheel；
   - `session-queue` style input/DriverNode poll；
   - worker-local runtime registry。

如果把这些公共职责继续放在 `transport/tcp`，后续接 QUIC 时会自然复制第二套 app ring、ready queue、timer wheel 和 worker poll 机制。用户要求按 7 层分层，所以公共 session 管理不应放在 `transport/` 下。

## 2. 分层结论

Hammer service 侧目录按职责分层：

```text
crates/hammer-service/src/
  net/             # L3: IP, lookup, routing-adjacent data-plane nodes
  transport/       # L4: TCP, UDP, QUIC packet/transport state machines
  session/         # L5: app-facing session/flow orchestration
  dns/             # L7-ish: DNS application protocol and transports
  app/             # app-facing service integration and runtime glue
  tun/             # interface/driver boundary
```

`session/` 是 L5，不是 L4 transport。它管理“app flow 如何在 worker 本地被 polling、ready、timer-driven”，但不理解 TCP 序号、QUIC packet number、TLS secret、拥塞算法细节或 IP/port lookup。

QUIC 的工程落点可以在后续单独讨论。无论 QUIC 代码放在 `transport/quic` 还是独立协议目录，L5 `session/` 的边界都不变：它只提供 session 原语，QUIC 自己负责 connection id、stream、datagram、crypto、packet number space 和 transport output。

## 3. VPP 与 io_uring 对标

VPP 的 session layer 把 application/message queue、session worker、transport protocol 分开。`src/vnet/session/session_node.c` 注册了独立 input node：

```text
VLIB_REGISTER_NODE (session_queue_node)
  .name = "session-queue"
  .type = VLIB_NODE_TYPE_INPUT
  .function = session_queue_node_fn
```

`session_queue_node_fn` drain app/session message queue，dispatch control events，dispatch new/old IO events，flush pending TX buffers，并按 session 的 transport protocol / session type 调 transport callbacks 或 output next。这个形状说明：公共 session queue node 应当存在，但 TCP/QUIC 的传输状态机不应上移到 session 层。

Hammer 的映射是：

- VPP `session-queue` input node -> Hammer `SessionQueueNode`；
- VPP session worker/app queue handling -> Hammer `WorkerSessionRuntime` 的 app polling、ready queue、timer expiry；
- VPP transport protocol callbacks/vft -> Hammer `SessionProtocolOps` registry；
- VPP transport output node registration -> Hammer protocol ops 记录 protocol-private output/next scheduling；
- VPP worker thread affinity -> Hammer `DataWorkerId` owner worker 和 empty-frame polling。

Hammer 的偏离点是 app ring 已经存在于 `hammer-runtime::app`，而且它更接近 io_uring 的 request/completion 模型：app 提交 SQE，worker-local session runtime poll submission，协议层最终产出 completion。Linux io_uring 的主语义是 SQE/CQE，不是通用 event bus。因此 L5 session 不引入泛泛的 `Event` API；它只暴露 submission/command、completion、timer expiry 和 ready id 这些具体队列项。

## 4. 设计目标

1. 把 app-facing session queue 从 `transport/tcp/session.rs` 中移出，形成 `crates/hammer-service/src/session/SessionQueueNode`。
2. 让 `SessionQueueNode` 成为 L5 独立 input/DriverNode，负责 app SQ polling、timer expiry、ready dispatch 和 protocol ops 调用。
3. 为 QUIC 预留复用点，避免复制 app ring polling、ready queue、timer wheel 和 worker-local runtime registry。
4. 保持现有 TCP 行为不变。本轮是边界拆分，不做 TCP progression 新语义。
5. 遵守 data-plane 约束：hot path 容器优先使用 `hammer-infra`，worker-local 状态不放到 control-plane task 中推进。

## 5. 非目标

1. 不把 TCP connection table 泛化成所有协议共用的 connection table。
2. 不把 TCP/QUIC 的 timer kind 合并成一个大 enum。
3. 不把 QUIC 提前实现为泛型参数或 trait object 状态机。
4. 不改变 app ring 的生产者接口。
5. 不删除旧 TCP output helper，除非实现 TCP progression 时已经有等价替代。

## 6. 模块结构

首轮新增：

```text
crates/hammer-service/src/session/
  mod.rs
  app.rs
  node.rs
  protocol.rs
  ready.rs
  timer.rs
  worker.rs
```

职责如下：

- `mod.rs`
  - 对外导出 L5 session 原语；
  - 对外导出 `SessionQueueNode` 和 protocol registration；
  - 不导出 TCP/QUIC 类型。

- `app.rs`
  - 保存 app backend attachment；
  - 从 `AppBackend` 同步 pop SQE 和 shutdown；
  - 把 app ring 输入转成协议无关的 app session submission；
  - 把协议层 completion 写回 app backend；
  - 校验 flow/object ref 与绑定 flow 一致。

- `node.rs`
  - 定义 `SessionQueueNode`；
  - 作为独立 input/DriverNode 通过 empty frame poll；
  - process 时 drain L5 runtime，并按 session protocol 调用 `SessionProtocolOps`。

- `protocol.rs`
  - 定义 `SessionProtocolId`, `SessionProtocolOps`, `SessionProtocolRegistry`；
  - 协议实现注册 callbacks，不让 L5 依赖 TCP/QUIC 类型；
  - 记录 protocol-private next/output scheduling 入口。

- `ready.rs`
  - worker-local ready session queue；
  - 以 session id 去重；
  - 支持 drain/take，供 `SessionQueueNode` 按 protocol 分派。

- `timer.rs`
  - worker-local timer wheel wrapper；
  - 支持 arm/cancel/expire；
  - 返回 session id 和协议私有 timer token；
  - 不解释 token 的协议语义。

- `worker.rs`
  - 组合 app、ready、timer；
  - 管理 `DataWorkerId`；
  - 提供 `poll_once_at` / `poll_once_for_ticks`；
  - 提供 worker-local runtime slot/registry 的公共帮助函数。

## 7. 核心类型

公共 L5 类型只表达 session 层概念：

```rust
pub struct AppSessionId(u64);

pub enum AppSessionSubmission {
    Send(AppSessionSend),
    Recv(AppSessionRecv),
    Close(AppSessionClose),
    Shutdown(AppSessionShutdown),
}

pub struct AppSessionCompletion {
    session_id: AppSessionId,
    user_data: AppUserData,
    result: i32,
    flags: AppCqeFlags,
    data: AppCqeData,
}

pub struct AppSessionTimerToken(u32);

pub struct AppSessionTimerExpiry {
    session_id: AppSessionId,
    token: AppSessionTimerToken,
}

pub struct SessionProtocolId(u16);

pub struct SessionProtocolContext<'a> {
    worker: DataWorkerId,
    runtime: &'a mut WorkerSessionRuntime,
}

pub trait SessionProtocolOps {
    fn handle_submission(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        submission: AppSessionSubmission,
    ) -> CoreResult<()>;

    fn handle_timer_expiry(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        expiry: AppSessionTimerExpiry,
    ) -> CoreResult<()>;

    fn handle_ready(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        session_id: AppSessionId,
    ) -> CoreResult<()>;
}

pub struct SessionProtocolRegistry {
    protocols: hammer_infra::vec::Vec<SessionProtocolSlot>,
}

pub struct SessionQueueNode {
    worker: DataWorkerId,
}

pub struct SessionQueueRuntime {
    sessions: WorkerSessionRuntime,
    protocols: SessionProtocolRegistry,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    app: AppSessionAppIngress,
    ready: AppSessionReadyQueue,
    timers: AppSessionTimerWheel,
}
```

`SessionProtocolId` 是 L5 分派键，不定义 `Tcp`/`Quic` enum variant。TCP、QUIC 或后续协议通过 registry 获得自己的 id，并把该 id 绑定到 `AppSessionId`。

`AppSessionId` 是 L5 stable id，不等于 TCP `TcpConnectionId`，也不等于 QUIC connection id。协议实现可以在自己的 state 中维护映射：

```text
AppSessionId -> TcpConnectionId
TcpConnectionId -> AppSessionId
```

QUIC glue 后续可以维护：

```text
AppSessionId -> QuicConnectionId / stream id
Quic stream id -> AppSessionId
```

Timer token 只作为协议私有值保存。TCP 可以把 token 编码为 retransmit/persist/output pacing；QUIC 可以编码为 PTO、idle timeout、ACK delay 或 stream timer。

## 8. SessionQueueNode 运行模型

`SessionQueueNode` 是 L5 独立 input/DriverNode，对标 VPP `session-queue`。它不属于 `transport/tcp`，也不属于 `transport/quic`。

每次被 empty-frame poll 时，它按固定顺序推进：

1. 从 app backend ring drain submissions 和 shutdowns。
2. 根据 `AppSessionId` 查 session 绑定的 `SessionProtocol`。
3. 调用对应 `SessionProtocolOps::handle_submission`。
4. 推进 L5 timer wheel，得到 `AppSessionTimerExpiry`。
5. 调用对应 `SessionProtocolOps::handle_timer_expiry`。
6. drain ready session ids。
7. 调用对应 `SessionProtocolOps::handle_ready`。
8. flush protocol ops 暂存的 output/next scheduling。
9. 如果还有 pending L5 work，则 schedule 自己。

这个 node 可以是 Hammer 的 `DriverNode`，因为 Hammer 当前用 driver role 表示外部输入边界和 runtime polling。它在语义上对标 VPP `VLIB_NODE_TYPE_INPUT`，在 Hammer 实现上使用 empty-frame DriverNode。

## 9. TCP 接入方式

TCP 通过 `SessionProtocolOps` 注册到 L5 session registry，而不是暴露 `TcpSessionNode` 或 `TcpAppCommand*`：

```text
TcpSessionProtocol
  - connections: TcpConnectionTable
  - app_session_to_connection: FlatHashTable<u64, TcpConnectionId>
  - connection_to_app_session: FlatHashTable<u64, AppSessionId>
  - tcp_private_timer_tokens
  - tcp_private_output_next
```

TCP 侧只保留 protocol-private 的实现细节：

- `AppSessionId` 与 `TcpConnectionId` 的映射；
- TCP connection table；
- TCP timer token 编码/解码；
- TCP send/recv/close/shutdown 状态推进；
- TCP output/next scheduling。

`TcpAppCommand`、`TcpAppSend`、`TcpAppRecv`、`TcpAppClose`、`TcpAppShutdownCommand` 不再是目标设计的一部分；如果迁移中短暂存在，只能是文件内 private 过渡实现，并应在首轮拆分结束前删除。

App submission 转换流程：

1. L5 `WorkerSessionRuntime::poll_app_submissions` 产出 `AppSessionSubmission`。
2. `SessionQueueNode` 根据 session protocol 选择 TCP ops。
3. TCP ops 根据 `AppSessionId` 找到 `TcpConnectionId`。
4. TCP ops 直接把 submission 应用到 TCP connection state 或 TCP-private pending work。
5. TCP progression 消费 TCP-private pending work，更新 TCP state 并发出 output。

Timer/ready 转换流程：

1. L5 timer wheel 产出 `AppSessionTimerExpiry`，同时 ready queue 记录对应 `AppSessionId`。
2. `SessionQueueNode` 根据 session protocol 选择 TCP ops。
3. TCP ops 根据 timer token 还原 TCP-private timer kind。
4. TCP ops 根据 `AppSessionId` 找到 `TcpConnectionId` 并运行 TCP 专属 timer handler。

这样可以把公共 app/timer/ready 机制移到 L5，同时避免 TCP 类型泄漏成未来 QUIC 也必须兼容的公共 API。

## 10. QUIC 预留方式

QUIC 后续接入时复用：

- app backend attachment；
- app SQ/CQ polling；
- shutdown/close submission；
- completion 写回；
- ready session queue；
- timer wheel；
- `SessionQueueNode` worker-local input/DriverNode poll 模式；
- `SessionProtocolOps` registration。

QUIC 自己实现：

- UDP substrate 读写；
- QUIC connection id lookup；
- stream id 到 app session 的映射；
- datagram frame；
- packet number space；
- TLS/crypto state；
- loss recovery 和 PTO；
- stream-level flow control。

因此 L5 session 层不需要提前包含 QUIC-specific enum，也不需要为 QUIC 设计 trait object 状态机。

## 11. Data Flow

App send/shutdown 路径：

```text
AppContext
  -> owner worker AppBackend ring
  -> SessionQueueNode
  -> WorkerSessionRuntime::poll_app_submissions
  -> AppSessionSubmission
  -> SessionProtocolOps::handle_submission
  -> protocol-specific ready connection/session
  -> protocol-specific output
  -> AppSessionCompletion
  -> owner worker AppBackend CQ
```

Timer 路径：

```text
SessionProtocolOps arms L5 timer with AppSessionId + protocol token
  -> session::timer expires on owner worker
  -> SessionQueueNode dispatches AppSessionTimerExpiry
  -> SessionProtocolOps::handle_timer_expiry
  -> protocol-specific timer handler
```

Ready 路径：

```text
session::ready dedupes AppSessionId
  -> SessionQueueNode drains ready ids
  -> SessionProtocolOps::handle_ready
  -> protocol-specific table lookup
  -> protocol-specific step
```

## 12. Error Handling

L5 session returns `CoreResult` and uses `CoreError::internal` for invariant violations already present in the current TCP session runtime:

- app submission object does not match attached flow；
- shutdown flow does not match attached flow；
- send SQE is missing registered buffer；
- duplicate app backend attachment；
- timer arm for missing session；
- invalid timer slot。

Protocol glue owns protocol-specific errors:

- missing TCP connection for `AppSessionId`；
- unsupported TCP command；
- QUIC stream lookup failures；
- QUIC crypto/loss recovery failures。

## 13. Testing Strategy

首轮测试按 TDD 做结构迁移：

1. 新增 `crates/hammer-service/tests/session_runtime.rs`，覆盖 L5 原语：
   - `SessionQueueNode` runs as empty-frame DriverNode；
   - app send SQE polling produces `AppSessionSubmission::Send`；
   - shutdown polling produces `AppSessionSubmission::Shutdown`；
   - registered protocol ops receive submissions by `AppSessionId`；
   - completion helper writes CQE back to the attached app backend；
   - ready queue dedupes session ids；
   - timer expiry dispatches `AppSessionTimerExpiry` to protocol ops；
   - deterministic clock advances timer wheel。

2. 保留并调整 TCP session tests：
   - TCP protocol ops 仍能 install connection；
   - TCP protocol ops 仍能 bind `AppSessionId` 到 `TcpConnectionId`；
   - app send/shutdown submission 能通过 `SessionQueueNode` mark 对应 TCP connection ready；
   - TCP 不再需要 public `TcpSessionNode` 或 `TcpAppCommand*`。

3. 不新增 QUIC 测试。QUIC 还没有实现，首轮只保证 L5 API 不含 TCP 名称。

Verification commands:

```bash
cargo fmt --all
cargo test -p hammer-service --test session_runtime
cargo test -p hammer-service --test session_queue_node
cargo test -p hammer-service --test tcp_session_protocol
git diff --check
```

## 14. Migration Plan

1. 建 `crates/hammer-service/src/session/`，先复制当前 TCP session runtime 中的 app/ready/timer 公共逻辑。
2. 给公共层引入 `AppSessionId` 和 token 化 timer，不带 TCP 命名。
3. 新增 `SessionQueueNode`，作为独立 empty-frame DriverNode。
4. 新增 `SessionProtocolOps` registry。
5. 将 TCP session runtime 改成 TCP protocol ops 注册到 `SessionQueueNode`，不要保留 `TcpAppCommand*` public API。
6. 跑 L5 session tests 和 TCP session tests，保证行为不变。
7. 后续实现 TCP progression 时，TCP 只通过 protocol ops 消费 L5 submissions、timer expiries 和 ready ids，不再把 app ring polling 留在 TCP 文件里。

## 15. Acceptance Criteria

1. `crates/hammer-service/src/session/` 存在，并且不依赖 `transport::tcp`。
2. L5 session public API 暴露 `SessionQueueNode` 和 protocol registration，但不暴露 TCP/QUIC 协议类型。
3. `SessionQueueNode` 是独立 empty-frame DriverNode，对标 VPP `session-queue` input node。
4. TCP 通过 `SessionProtocolOps` 接入，而不是通过 public `TcpSessionNode` 或 `TcpAppCommand*`。
5. `transport::tcp` 不 re-export `TcpAppCommand`、`TcpAppSend`、`TcpAppRecv`、`TcpAppClose`、`TcpAppShutdownCommand` 这类 app session wrapper。
6. App ring polling、ready queue、timer wheel 从 TCP 文件中移到 L5 session 文件。
7. `cargo test -p hammer-service --test session_runtime` 通过。
8. `cargo test -p hammer-service --test session_queue_node` 通过。
9. `cargo test -p hammer-service --test tcp_session_protocol` 通过。
10. `git diff --check` 通过。

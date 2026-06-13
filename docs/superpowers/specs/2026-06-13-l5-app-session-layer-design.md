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
   - empty-frame DriverNode poll；
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

VPP 的 session layer 把 application/message queue、session worker、transport protocol 分开。`src/vnet/session/session_node.c` 中 session worker drain app/session message queue，并通过 transport protocol 字段把 listen/connect/shutdown/disconnect 等操作交给对应 transport。这个形状说明：公共 session worker 层应当存在，但 TCP/QUIC 的传输状态机不应上移到 session 层。

Hammer 的映射是：

- VPP session worker/app queue handling -> Hammer `session/` worker-local app polling 和 ready/timer 原语；
- VPP transport protocol callbacks -> Hammer `transport/tcp`、后续 `transport/quic` 的协议专属推进；
- VPP worker thread affinity -> Hammer `DataWorkerId` owner worker 和 DriverNode empty-frame polling。

Hammer 的偏离点是 app ring 已经存在于 `hammer-runtime::app`，而且它更接近 io_uring 的 request/completion 模型：app 提交 SQE，worker-local session runtime poll submission，协议层最终产出 completion。Linux io_uring 的主语义是 SQE/CQE，不是通用 event bus。因此 L5 session 不引入泛泛的 `Event` API；它只暴露 submission/command、completion、timer expiry 和 ready id 这些具体队列项。

## 4. 设计目标

1. 把 app-facing session 管理从 `transport/tcp/session.rs` 中移出，形成 `crates/hammer-service/src/session/`。
2. 让 TCP 的 session node 变薄，只把 L5 submissions、timer expiries 和 ready ids 映射到 TCP connection state。
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
  ready.rs
  timer.rs
  worker.rs
```

职责如下：

- `mod.rs`
  - 对外导出 L5 session 原语；
  - 不导出 TCP/QUIC 类型；
  - 不注册具体 TCP/QUIC node。

- `app.rs`
  - 保存 app backend attachment；
  - 从 `AppBackend` 同步 pop SQE 和 shutdown；
  - 把 app ring 输入转成协议无关的 app session submission；
  - 把协议层 completion 写回 app backend；
  - 校验 flow/object ref 与绑定 flow 一致。

- `ready.rs`
  - worker-local ready session queue；
  - 以 session id 去重；
  - 支持 drain/take，供协议层逐个推进。

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
    result: i32,
    flags: AppCqeFlags,
    data: AppCqeData,
}

pub struct AppSessionTimerToken(u32);

pub struct AppSessionTimerExpiry {
    session_id: AppSessionId,
    token: AppSessionTimerToken,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    app: AppSessionAppIngress,
    ready: AppSessionReadyQueue,
    timers: AppSessionTimerWheel,
}
```

`AppSessionId` 是 L5 stable id，不等于 TCP `TcpConnectionId`，也不等于 QUIC connection id。TCP glue 可以维护：

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

## 8. TCP 适配方式

`crates/hammer-service/src/transport/tcp/session.rs` 首轮保留 TCP 对外 API，但内部改为组合 L5 原语：

```text
TcpSessionRuntime
  - worker: DataWorkerId
  - sessions: WorkerSessionRuntime
  - connections: TcpConnectionTable
  - app_session_to_connection: FlatHashTable<u64, TcpConnectionId>
  - connection_to_app_session: FlatHashTable<u64, AppSessionId>
```

TCP 专属类型仍留在 TCP 模块：

- `TcpSessionNode`
- `TcpAppCommand`
- `TcpAppSend`
- `TcpAppRecv`
- `TcpAppClose`
- `TcpAppShutdownCommand`
- `TcpSessionTimerKind`

App submission 转换流程：

1. L5 `WorkerSessionRuntime::poll_app_submissions` 产出 `AppSessionSubmission`。
2. TCP glue 根据 `AppSessionId` 找到 `TcpConnectionId`。
3. TCP glue 转成现有 `TcpAppCommand`，并 mark TCP connection ready。
4. 后续 TCP progression 再消费 `TcpAppCommand`，更新 TCP state 并发出 output。

Timer/ready 转换流程：

1. L5 timer wheel 产出 `AppSessionTimerExpiry`，同时 ready queue 记录对应 `AppSessionId`。
2. TCP glue 根据 timer token 还原 `TcpSessionTimerKind`。
3. TCP glue 根据 `AppSessionId` 找到 `TcpConnectionId` 并运行 TCP 专属 timer handler。

这样可以保持现有测试中的 TCP API 表面稳定，同时把公共 app/timer/ready 机制移到 L5。

## 9. QUIC 预留方式

QUIC 后续接入时复用：

- app backend attachment；
- app SQ/CQ polling；
- shutdown/close submission；
- completion 写回；
- ready session queue；
- timer wheel；
- worker-local DriverNode poll 模式。

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

## 10. Data Flow

App send/shutdown 路径：

```text
AppContext
  -> owner worker AppBackend ring
  -> session::WorkerSessionRuntime::poll_app_submissions
  -> AppSessionSubmission
  -> transport/tcp or transport/quic glue
  -> protocol-specific ready connection/session
  -> protocol-specific output
  -> AppSessionCompletion
  -> owner worker AppBackend CQ
```

Timer 路径：

```text
protocol glue arms L5 timer with AppSessionId + protocol token
  -> session::timer expires on owner worker
  -> WorkerSessionRuntime marks AppSessionId ready
  -> protocol glue decodes token
  -> protocol-specific timer handler
```

Ready 路径：

```text
session::ready dedupes AppSessionId
  -> protocol glue drains ready ids
  -> protocol-specific table lookup
  -> protocol-specific step
```

## 11. Error Handling

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

## 12. Testing Strategy

首轮测试按 TDD 做结构迁移：

1. 新增 `crates/hammer-service/tests/session_runtime.rs`，覆盖 L5 原语：
   - app send SQE polling produces `AppSessionSubmission::Send`；
   - shutdown polling produces `AppSessionSubmission::Shutdown`；
   - completion helper writes CQE back to the attached app backend；
   - ready queue dedupes session ids；
   - timer expiry marks a session ready and returns token；
   - deterministic clock advances timer wheel。

2. 保留并调整 `crates/hammer-service/tests/tcp_session_node.rs`：
   - TCP API 仍能 install connection；
   - TCP session runtime 仍能 attach app backend；
   - app send/shutdown 仍转为 `TcpAppCommand`；
   - empty-frame `TcpSessionNode` 仍能 poll。

3. 不新增 QUIC 测试。QUIC 还没有实现，首轮只保证 L5 API 不含 TCP 名称。

Verification commands:

```bash
cargo fmt --all
cargo test -p hammer-service --test session_runtime
cargo test -p hammer-service --test tcp_session_node
git diff --check
```

## 13. Migration Plan

1. 建 `crates/hammer-service/src/session/`，先复制当前 TCP session runtime 中的 app/ready/timer 公共逻辑。
2. 给公共层引入 `AppSessionId` 和 token 化 timer，不带 TCP 命名。
3. 修改 `TcpSessionRuntime` 组合 `WorkerSessionRuntime`，保留 TCP-facing public API。
4. 移动 thread-local registry helper，使 `TcpSessionNode` 仍能注册 worker-local runtime。
5. 跑现有 TCP session tests，保证行为不变。
6. 后续实现 TCP progression 时，TCP 只消费 L5 submissions、timer expiries 和 ready ids，不再把 app ring polling 留在 TCP 文件里。

## 14. Acceptance Criteria

1. `crates/hammer-service/src/session/` 存在，并且不依赖 `transport::tcp`。
2. L5 session public API 不暴露 TCP/QUIC 协议类型。
3. TCP session runtime 保持现有测试语义。
4. App ring polling、ready queue、timer wheel 从 TCP 文件中移到 L5 session 文件。
5. `cargo test -p hammer-service --test session_runtime` 通过。
6. `cargo test -p hammer-service --test tcp_session_node` 通过。
7. `git diff --check` 通过。

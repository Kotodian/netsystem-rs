# TCP Lab GitHub Actions 完善方案

## Context

当前 `.github/workflows/tcp-integration.yml` 只验证 Linux/macOS TUN 上的 TCP 三次握手，没有验证真实应用收发、拥塞控制、keepalive、SACK、RACK 或 TLP。生产 listener 路径还存在 attach 生命周期缺口：TCP worker 已通过 `SessionWorker::create_app_session` 创建权威 `SessionHandle` 和真实 RX/TX FIFO，但这些 segment/queue 是 local mapping；`AppServer::accept` 反而另建一套不属于该 TCP connection 的 FIFO，`AppClient::connect` 还要求调用者伪造 handle。普通三次握手完成路径也没有调用 `sessions.connected(session_id)`。

本次按 vendored VPP 的 accepted-session 生命周期修复该问题，并补齐完整 TUN CI：

1. VPP `session_stream_accept`：分配 transport session、保存 listener 身份。
2. VPP `app_worker_init_accepted`：选择 listener app worker，并由该 worker 的 segment manager 分配真实 session FIFO。
3. VPP `session_lookup_add_connection`：注册权威 session handle。
4. VPP `app_worker_accept_notify`：lookup 成功后才通知应用；通知失败则回滚 lookup、FIFO 和 session。

Hammer 保留自己的调度模型：main thread 的 Tokio 只处理一次性的 Unix attach 控制面；`AppWorker`、session FIFO、TCP connection、timer、RACK/TLP 和 packet output 全部继续在 data worker/`SessionQueueNode` 中运行。

## 1. 发布真实 Listener AppSession

### 线程与数据边界

- Hammer main thread 独占 Unix listener/stream，只执行 `accept`、attach metadata 编码和 `SCM_RIGHTS` fd 传递。
- Data worker 不执行 Unix socket I/O，只通过 bounded `tokio::sync::mpsc::Sender::try_send` 非阻塞发布 attach descriptor。
- Unix socket 不承载 TCP payload。attach 完成后，外部 app 与 `AppWorker` 通过真实共享 FIFO、消息队列和 signal fd 交互：
  - network → app：TCP worker 写 `rx_fifo`，向 per-session `evt_q` 投递 `RxEnq`。
  - app → network：app 写 `tx_fifo`，向 worker-shared `tx_evt_q` 投递事件；worker `FileMain` 唤醒 `SessionQueueNode`，TCP 发送并在 ACK 后丢弃已确认 FIFO 字节。
  - `Connect`/`Close` 继续走 per-session `evt_q`。
- main thread 不读取或修改 TCP connection、session FIFO 或 worker timer。

### 新增和修改的 Rust 类型/API

在 `hammer-runtime` attach/app 边界增加：

```rust
pub struct AppSessionLayout {
    pub rx_fifo_offset: u64,
    pub tx_fifo_offset: u64,
    pub event_queue_offset: u64,
}

pub struct AppSessionPublication { /* authoritative session + mapping metadata */ }

#[derive(Clone)]
pub struct AppSessionPublisher { /* bounded channel sender */ }

impl AppServer {
    pub fn bind(path: &str, capacity: usize) -> RuntimeResult<Self>;
    pub fn publisher(&self) -> AppSessionPublisher;
    pub async fn serve(self: Arc<Self>) -> RuntimeResult<()>;
}

impl AppSessionPublisher {
    pub fn try_publish(&self, publication: AppSessionPublication) -> RuntimeResult<()>;
}

impl AppClient {
    pub fn connect(path: &str) -> Result<AppSession, AppClientError>;
}
```

`AppSessionPublication` 持有：

- 真实 `Arc<AppSession>`，确保 session 关闭但尚未完成 attach 时 allocation 不会复用。
- 权威 `SessionHandle`。
- listener session segment、RX/TX FIFO 和 per-session event queue offsets。
- worker-shared TX event queue segment 和 offset。
- session event queue read endpoint、worker TX event queue write endpoint的可发送引用；`SCM_RIGHTS` 在接收进程中产生独立 fd，不转移 daemon 原 fd 的所有权。

给 `SessionMsgQueue` 增加受控的 write-fd accessor；保留已有 read-fd accessor。把 `AppSession::from_segment` 改成从两个 mapping 重建：listener session segment + worker TX-event segment，并接收 server 发送的权威 handle。删除 `AppClient::connect(path, handle)` 这一可伪造身份的 API。

### Segment 和 AppWorker 所有权

- attach 未配置时继续使用 `Segment::local`，不增加普通部署的共享内存开销。
- attach 已配置时：
  - 每个 listener 的 `SegmentManager` 使用 `Segment::shared` 创建 session segment。
  - 每个 data worker 使用一个独立 `Segment::shared` 存放 worker-shared `tx_evt_q`。
  - segment name 包含 worker/listener/segment index；实际身份仍由 descriptor 和 offset 决定。
- 将 `AppWorker::session_slots` 的 tuple 改成私有 slot 结构，保存 `SessionId`、真实 `Arc<AppSession>` 以及尚未发布的 `Option<AppSessionPublication>`。
- `SessionWorker::{new, with_app_session_config}` 接收可选 `AppSessionPublisher` 并返回 `RuntimeResult<Self>`，让 shared mapping 创建错误进入正常初始化失败路径。
- `SegmentManager::retired` 继续依赖 `Weak<AppSession>` 延迟 offset 复用；publication 持有的强引用自然参与该生命周期，不新增第二套 session storage。

### Attach wire format

发送四个 descriptor，固定顺序：

1. listener session segment fd。
2. worker TX-event segment fd。
3. per-session event queue read fd。
4. worker TX-event queue write fd。

metadata 使用 versioned、固定 endian 的整数编码，包含：protocol version、权威 handle、两个 segment size、三个 session offsets 和 worker queue offset。客户端拒绝未知版本、metadata 长度错误、descriptor 数量错误、control truncation、整数溢出以及越界 offset。

`AppServer::serve` 同时维护 bounded pending clients 和 publications，允许 app 先连接或 TCP session 先建立；一名 Unix client 仍对应一个 accepted session，保持当前协议范围。短写或 client 断开时关闭该 client，并把同一个 publication 留给下一个 client，不重新分配 FIFO/session。

### Accepted-session 顺序和回滚

统一 Fast Open 和普通三次握手路径：

1. `insert_tcp_session` 创建 session entry、真实 AppSession 和 TCP connection。
2. 完成 TCP state transition 和需要保留的 Fast Open payload 入队。
3. `publish_tcp_connection` 注册 tuple → 权威 session handle lookup。
4. 最后调用可变的 `sessions.connected(session_id)`：
   - 先向真实 per-session queue 投递 `Connect`。
   - 再 `try_publish` attach descriptor。
5. `connected` 成功后不再执行可能导致本次 accept 失败的操作。

普通 `tcp_complete_listener_open` 补上当前缺失的 `sessions.connected(session_id)`。Fast Open 路径把 notify 移到所有 payload/lookup 操作之后。event queue full、publication channel full/closed 或 descriptor 准备失败都返回错误，由现有 listener error path 执行 `forget_session`、`forget_pending_open` 和 `rollback_tcp_session`，对应 VPP 的 failed `app_worker_accept_notify` 回滚。

自然 close 若发生在外部 app 完成 attach 前，publication 继续持有同一真实 session；app attach 后会按队列顺序观察 `Connect` 和 `Close`，不创建替代 session。

### Tokio 主循环接入

- `configure_attach_server` 使用 session pool capacity 创建 bounded `AppServer`，worker init 只取得其 publisher clone。
- `crates/hammer/src/main.rs` 在现有 current-thread Tokio runtime 中并发驱动 `ipc_loop::clnt_loop` 和可选 `AppServer::serve`；任一控制服务异常结束都进入现有 daemon shutdown。
- 不新增 Tokio TCP timer task、TCP graph node或 recovery loop。
- 将 `examples/tun/tcp_echo.rs` 简化为真正的外部 attached echo app：连接现有 daemon，接收 server 给出的 handle，读取 `rx_fifo` 并写回 `tx_fifo`。CI 单独启动 Hammer daemon 和该 echo app。

## 2. TCP Recovery 可观测性

复用 `hammerctl status` 已输出的 worker node error counters，不增加 metrics IPC。

在 `TcpConnection::on_typed_timer_expiry` 返回私有 `TcpTimerOutcome`，除 `Option<TcpSegment>` 外标记本次有效 action：

```rust
enum TcpTimerAction {
    RtoRetransmit,
    RackRetransmit,
    TlpProbe,
    PersistProbe,
    KeepaliveProbe,
}
```

只有 timer 真正选出 segment/sample 或设置有效 TX intent 时才返回 action；stale token、无可恢复 sample、状态不匹配不计数。`TcpWorker::update_time` 在当前 `SessionQueueNode` dispatch 上把 action 记录到 node counter：

- `RackTimeout` 重命名为同 discriminant 的 `RackRetransmit`。
- `TlpProbe` 保持。
- `Retransmit` 明确表示 RTO-selected retransmission。
- `PersistTimer` 重命名为同 discriminant 的 `PersistProbe`。
- 追加 `KeepaliveProbe`，不移动已有 discriminant。

RACK/TLP established-data action 在 timer 选中恢复样本并安排 worker TX 时计数；CI 同时要求 pcap 出现对应绝对 sequence 的实际重发，避免把“已选择但未发出”单独当成通过证据。

不伪造 `BbrCongestion` 计数。BBR/CUBIC 算法状态由确定性 Rust 测试验证；TUN 大流量测试验证所配置 controller 能通过真实 ACK/FIFO/output 路径持续传输。

## 3. 确定性测试

补充或调整 Rust 测试，覆盖：

- attach metadata 中权威 handle round-trip，客户端不再提供 handle。
- 两个 shared mapping、四个 descriptor、offset 和 queue direction reconstruction。
- app 先连接/session 先发布两种 pairing 顺序。
- publication channel full/closed 时 listener rollback，不留下 lookup、TCP connection 或 AppSession allocation。
- Unix client 传输失败后复用同一 publication，不重新分配 session。
- publication/attached app 引用释放前 segment offset 不复用。
- 普通 handshake 和 Fast Open 都只在 lookup/payload 成功后发一次 Connect。
- RACK、TLP、RTO、persist、keepalive 仅在有效 timer action 时增加 counter；stale/empty expiry 不增加。
- 保留并运行现有 BBR、CUBIC、SACK/DSACK、RACK/TLP selection、keepalive 和 timer generation tests。

测试使用显式 `Instant`/现有 timer harness，不使用 wall-clock sleep。

## 4. TUN GitHub Actions 场景

将 workflow 组织成一个协议语义 job 和一个共享启动/清理逻辑的显式 TUN scenario matrix。

### `tcp-protocol-semantics`

在 Ubuntu 运行：

- `cargo test --locked -p hammer-plugin-tcp`：TCP connection、SACK/DSACK、RACK/TLP、keepalive、timer 和 BBR integration。
- `cargo test --locked -p hammer-service transport::congestion`：BBR/CUBIC controller 状态和 loss/ACK 行为。
- attach/session 相关 package tests。

### TUN matrix

- Linux + macOS `attached-echo-keepalive`：真实 attached echo、跨多个 FIFO chunk 的逐字节数据校验、空闲超过 3 秒、socket 仍存活、pcap 中出现 `SEQ = SND.NXT - 1` 的零长度 keepalive probe。
- Linux `congestion-bbr` 和 `congestion-cubic`：分别生成对应配置，传输明显超过 initial congestion window 和 FIFO capacity 的 payload，逐字节验证并要求足够数量的 server data/ACK packet；算法内部状态仍由 semantic job 判定。
- Linux `receiver-sack`：握手后解析 client ISN，在 TUN egress 的 `tc clsact/u32` 规则按绝对 TCP sequence 丢一个中间 client data segment；要求 Hammer ACK 含有效 SACK block，rule counter 命中一次，最终 echo 完整。
- Linux `sender-rack`：按 server ISN 在 TUN ingress 丢一个非尾部 Hammer data segment，允许后续 segment 到达并产生 client SACK；要求 `RackRetransmit` counter 增加、pcap 出现目标 sequence 重发且时间早于 RTO、最终 echo 完整。
- Linux `sender-tlp`：只丢最后一个 Hammer data segment且不再发送后续数据；要求 `TlpProbe` counter 增加、目标 tail sequence 重发、RTO counter 在 probe 前不增加、最终 echo 完整。

为 sender recovery 场景把 lab RTO initial/min 调高到约 2 秒，确保 RACK/TLP deadline 先于 RTO。客户端在 `connect()` 前设置固定 `TCP_MAXSEG`，payload 使用 MSS 的明确倍数，workflow 从握手 pcap 得到绝对目标 sequence。`tc -s filter` 和 pcap共同证明指定方向/sequence 的 fault rule 实际命中。

DSACK 不作为 TUN 门禁：重复包时序容易受 runner 影响，继续由生产 `sack.rs` 的确定性测试负责。

## 5. CI Helper 修改

### `scripts/tcp-connect-probe.py`

在现有 idle/send/ready/continue 基础上：

- 显式创建 IPv4 TCP socket，在 connect 前设置 `TCP_MAXSEG`，connect 后设置 `TCP_NODELAY`。
- 增加 exact echo 模式，发送 position-dependent deterministic payload 并精确读取同样字节；支持 bounded send/read 交错，避免 payload 大于 FIFO 时双向阻塞。
- ready file 在握手完成后创建，continue file 用于等待 workflow 安装绝对 sequence fault rule。
- 对 mismatch、early EOF、缺字节、意外尾随数据和 idle close 明确失败。

### `scripts/tcp-pcap-assert.py`

替换当前“所有 pure ACK 都算 keepalive”的实现：

- 解析 `tcpdump -nn -S` 的 timestamp、direction、flags、absolute seq range、ack、length、SACK permitted 和 SACK blocks。
- 输出握手摘要和 client/server first-data sequence，供 workflow 安装 fault rule。
- 独立断言 handshake、keepalive、receiver SACK、sender RACK retransmission、sender TLP tail retransmission和 minimum data transfer。
- RACK/TLP 的内部归因读取 `hammerctl status` counter；packet shape 只证明目标 sequence 实际重发。

Workflow 还会：

- 用 `${{ runner.temp }}`/`$GITHUB_ENV` 初始化 diagnostics，修复当前 `$RUNNER_TEMP` 字面量问题。
- 在解析前 `SIGINT` 并等待 tcpdump flush，再使用 `tcpdump -nn -S`。
- `always()` 删除 `tc clsact` filters/qdisc、route、socket，停止 echo/daemon/tcpdump。
- 上传 pcap、absolute decode、probe/echo/daemon logs、status before/after、`tc -s`、route/interface/process diagnostics。

## 6. 关键文件

- `crates/hammer-runtime/src/attach.rs`
- `crates/hammer-runtime/src/app/session.rs`
- `crates/hammer-runtime/src/app/session_msg_queue.rs`
- `crates/hammer-app/src/attach.rs`
- `crates/hammer-service/src/session/{mod.rs,app.rs,runtime.rs}`
- `crates/hammer-plugins/transport/tcp/src/{listen.rs,connection.rs,worker.rs,lib.rs}`
- `crates/hammer/src/main.rs`
- `examples/tun/tcp_echo.rs`
- `examples/tun-tcp-echo.toml`
- `scripts/tcp-connect-probe.py`
- `scripts/tcp-pcap-assert.py`
- `.github/workflows/tcp-integration.yml`

不修改 `.claude/settings.local.json`、`docs/superpowers/plans/2026-07-23-hugepage.md` 等无关工作树内容。

## 7. VPP 参考边界

- Session/FIFO/app worker lifecycle：`third_party/vpp/src/vnet/session/session.c`、`application_worker.c`。
- FIFO 方向和 app-worker event queue ownership：VPP session/application worker 实现。
- RTO/output ownership：`third_party/vpp/src/vnet/tcp/tcp_output.c::tcp_timer_retransmit_handler`。
- OOO/SACK：`tcp_input.c::tcp_session_enqueue_ooo`、`tcp_sack.c`。
- Congestion controller ownership：`tcp_cc.h`。

RACK、TLP 和 Hammer keepalive 在 vendored VPP 中没有完全等价实现时，按 Hammer/RFC 语义实现和命名，不声称由 VPP 代码直接派生。VPP 约束的是 session ownership、lookup-before-notify、FIFO direction、worker-local protocol state和 failure rollback。

## 8. 执行与验证纪律

不在本地运行 tests、build、benchmark 或 TUN lab；完成后仅由 GitHub Actions 执行上述验证。每个 coherent Rust/business-code batch 完成后运行 `simplify`，明确禁止其运行本地测试/build/benchmark或修改无关文件。Python helper 和 workflow-only 修改不运行 `simplify`。

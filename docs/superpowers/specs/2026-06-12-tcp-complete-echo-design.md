# Hammer TCP 层完整实现设计（含 echo app 端到端验证）

- **日期：** 2026-06-12
- **状态：** 已与用户逐节确认
- **基线：** 分支 `codex/hammer-app-ring-zero-copy`，含未提交的零拷贝 buffer range 与 TCP output/拥塞控制改动（约 +2600 行），本设计直接在其上叠加，不先提交。
- **取代：** `docs/superpowers/plans/2026-06-10-vpp-tcp-node-completion.md` 的接收路径部分（该计划基于 06-10 代码状态，audit 已过期，保留作参考）。

## 1. 目标与成功标准

完整实现 hammer 的 TCP 层，使 echo app（被动打开的 echo server）可靠运行。

**成功标准（按用户确认）：**
1. 先完整完成 TCP 协议实现（Phase 1–4），再做 e2e 验证（Phase 5）。
2. 最终验证环境：macOS 本机 utun 设备，hammer 服务 + echo server 监听，宿主用 nc/客户端通过 TUN 地址连入，完成基本 echo、大数据流、中途断开、半关闭场景。
3. 全程 `cargo test` 集成测试覆盖每个状态转换与机制。

**范围（按用户确认为「接近 RFC 完整」）：**

纳入：被动/主动打开全状态机、established 收发、完整关闭流程（含 TIME_WAIT 全语义）、RTO 重传定时器联通、challenge ACK（RFC 5961 含速率限制）、delayed ACK、window-update ACK、persist probe、SACK、fast retransmit/fast recovery、timestamps/PAWS、ECN 闭环、listen backlog 与 SYN cookie、PMTU/MTU-aware MSS、接收窗口与 app backpressure 联动、keep-alive、URG 指针、TCP MD5 (RFC 2385)/TCP-AO (RFC 5925)、Nagle（per-socket 选项）。

不做：TCP Fast Open、MPTCP、窗口缩放以外的实验性扩展。

## 2. 现状基线

发送路径已基本就绪（当前未提交改动）：
- `output.rs`：重传队列（`TcpOutputRetransmitQueue`、`sent_at`、`acknowledge_through_with_sample`）、发送窗口 `min(peer_wnd, cwnd) - in_flight`、`TcpOutputBackend` 真实出口。
- `congestion.rs` / `congestion_control.rs`：BBR 风格控制器、per-connection `TcpCongestionState`、pacing 截止时间、loss/RTT/delivery 反馈口。
- `options.rs`：MSS/WSCALE/TS/SACK 解析；握手 MSS 已初始化拥塞状态。
- `connection.rs`：per-connection 数据面状态 + `TcpConnectionSnapshot`（control/观察视图）。

接收路径与生命周期是缺口：
- `listen.rs`：盲转发到 accept，无 SYN_RCVD 状态机，SYN 一到就完成 accept。
- `established.rs`：缺统一 segment validate、challenge ACK、established RST、dup ACK 计数。
- 关闭流程：FIN 仅检测，无 FIN_WAIT/CLOSING/LAST_ACK/TIME_WAIT 完整转换，无 2MSL。
- `service.rs`：`TcpWorkerEvent::TimerExpired` 是空操作（service.rs:3069、3236），定时器未端到端联通。
- `rcv_process.rs`：仅有序交付，无乱序缓冲。

## 3. 架构决策

**保持不变：**
- `hammer-runtime::protocol::tcp::TcpControlPlane` 仍是唯一共享控制面与定时器注册表（`TcpTimerKind::{Connect, Retransmit, DelayedAck, Persist, KeepAlive, TimeWait}` 六类已定义）。
- worker-owned 状态（`transport/tcp/connection.rs` + `lookup.rs`）驱动数据面决策；control thread 不在包路径上。
- node graph 拓扑不新增 next 分支：dispatch table（`state.rs`）按 `(TcpState, flags)` 扩展条目，SYN_RCVD 路由到 `TcpListenNode`、所有 established 之后状态路由到 `TcpEstablishedNode`，节点内部按快照 `tcp_state` 分支。
- ACK/SYN-ACK/challenge ACK 等控制包复用 `TcpOutputBackend` 输出路径（零 payload 输出包），不新建 reply writer 抽象。

**新增决策：**
- 乱序段缓冲在 `rcv_process.rs`（SACK 生成的前提），不再「ACK 后丢弃」。
- 所有进入 Closed 的路径（RST、LAST_ACK 完成、TIME_WAIT 到期、keep-alive 判死）汇聚到 service 侧同一个清理函数：移除 registration、republish lookup/app ingress/连接快照三件套。
- Nagle 与 pacing 正交组合：Nagle 决定小段「能不能发」，pacing 决定「何时发」；per-socket 选项，默认开启。
- MD5/AO 哈希原语**不依赖 btls**（用户明确要求，btls 与 TCP 无关）：在 hammer 内自带纯 Rust 实现（MD5、HMAC-SHA1/SHA-256、AO 的 KDF），放在 TCP 模块内部或 hammer-infra，无外部依赖；密钥配置经 control plane 下发。

## 4. 分阶段设计

### Phase 1 — 核心状态机（echo 最小完整路径）

**1.1 被动打开 `LISTEN → SYN_RCVD → ESTABLISHED`**

```
SYN 到达 → TcpInputNode（lookup 命中 listener）
  → dispatch (Listen, SYN) → TcpListenNode
  → 校验：纯 SYN、无 ACK、backlog 未满（Phase 1 仅有界计数，cookie 在 Phase 4）
  → 建半开条目（lookup.rs pending 表）：生成 ISS，irs=seg.seq，rcv_nxt=seg.seq+1
  → 解析对端 MSS/WSCALE（options.rs）
  → 经 TcpOutputBackend 发 SYN-ACK，注册 SynRcvd 超时定时器
  → worker event 上报，service 写 SynRcvd 快照并 republish

最终 ACK → lookup 命中半开条目（快照 state==SynRcvd）
  → dispatch (SynRcvd, ACK) → TcpListenNode 内部分支
  → 校验 ack==iss+1、seq==rcv_nxt
  → 升级 ESTABLISHED：初始化 snd_una/snd_nxt/窗口，对端 MSS 初始化拥塞状态（复用现有路径）
  → 取消 SynRcvd 定时器 → 此时才走 TcpAcceptNode 完成 accept，投递 AppCqeData::Accepted
```

- 重复 SYN 幂等：重发 SYN-ACK，不新建条目。
- SYN_RCVD 收 RST：回收半开条目。
- 校验失败/backlog 满：走 `TcpResetNode` 现有 RST 合成。

**1.2 主动打开 `SYN_SENT → ESTABLISHED` 补全**

现有 `TcpSynSentNode` 仅观察不处理，升级为真实处理：校验 SYN-ACK 的 ack 号与窗口、拒绝非法 SYN-ACK/杂散 ACK/RST（RST 经序号校验后终止连接并投递错误 CQE）、升级 ESTABLISHED、经 `TcpOutputBackend` 发最终 ACK、取消 `Connect` 定时器。保持现有 phase-1 app 语义：不新增 connect 完成 CQE。

**1.3 established 入口校验（VPP `tcp_segment_validate` 等价）**

所有 ESTABLISHED 及之后状态的包的公共入口，处理顺序：
1. 序号校验：段落在接收窗口内（含 payload 部分重叠判断）。失败 → challenge ACK 后丢；带 RST 则静默丢。
2. RST（过序号校验）：进 Closed，统一清理，pending recv/send CQE 投递 reset 错误。
3. 窗口内 SYN：challenge ACK（RFC 5961；速率限制在 Phase 4 补全）。
4. ACK：
   - `[snd_una, snd_nxt]` 内 → 推进 `snd_una`，喂 `acknowledge_through_with_sample`（重传队列 + 拥塞控制，发送侧已就绪，本阶段只联通）。
   - 旧 ACK 忽略；超前 ACK 回 ACK 后丢。
   - dup ACK 计数：Phase 1 仅记录，fast retransmit 在 Phase 2。
5. payload：Phase 1 仅有序段（seq==rcv_nxt）推进 rcv_nxt、回 ACK、转 TcpRcvProcessNode；乱序段回 ACK 后丢（Phase 2 改为缓冲）。
6. FIN：转关闭状态机。

**1.4 关闭全状态机**

对端先关（echo server 常见）：
```
ESTABLISHED 收 FIN（seq==rcv_nxt）→ rcv_nxt+=1，回 ACK → CLOSE_WAIT
  → app 投递 shutdown（pending recv 返回 EOF 完成）
  → CLOSE_WAIT 期间 app 仍可 send（半关闭，echo 正确性必要条件）
app Close SQE → 排空发送队列 → 发 FIN → LAST_ACK
LAST_ACK 收 FIN 的 ACK → Closed，统一清理，投递 Closed CQE
```

本端先关：
```
app Close SQE → 发 FIN → FIN_WAIT_1
  收 FIN 的 ACK → FIN_WAIT_2；收 FIN+ACK → TIME_WAIT
  FIN_WAIT_2 收 FIN → 回 ACK → TIME_WAIT
  同时关闭：FIN_WAIT_1 收 FIN → CLOSING → 收 ACK → TIME_WAIT
TIME_WAIT：注册 2MSL 定时器；重复 FIN 重发 ACK；到期回收（全语义在 Phase 4）
```

- FIN 占一个序号，进重传队列，RTO 重传与数据段同机制（`consumes_sequence_space` 已支持）。
- Close SQE 语义 = 优雅关闭（排空再 FIN），不是 abort。
- dispatch table 为 FinWait1/FinWait2/CloseWait/Closing/LastAck/TimeWait 补 `(state, flags)` 条目，全部路由到 `TcpEstablishedNode`。

**1.5 定时器端到端联通**

service 侧 `TimerExpired` 真实处理（替换现有空操作）：
- `Retransmit`：触发队首重传，喂拥塞 loss 反馈，RTO 指数退避；超过最大重传次数 → 连接判死。
- `Connect`：主动打开超时 → 清理 + app 错误 CQE。
- SYN_RCVD 超时：使用 `Retransmit` 类别（重发 SYN-ACK 本质是重传），重发达到上限后回收半开条目。
- `TimeWait`：2MSL 到期回收。
- `DelayedAck`/`Persist`/`KeepAlive`：Phase 1 维持非终结 no-op，分别在 Phase 3/2/4 联通。

**1.6 app 生命周期联通**

- Close SQE → 排空 → FIN 全链路。
- Closed/RST 时 pending SQE 的错误 CQE 投递。

**里程碑：** 集成测试完整握手 → 双向收发 → 双向关闭（含同时关闭）全链路通过；RTO 重传与超时清理有回归测试。

### Phase 2 — 可靠性增强

1. **乱序重排缓冲**：`rcv_process.rs` 扩展为真正 OOO 缓冲，按序拼接后批量交付 app；缓冲上限与 rcv_wnd 联动。
2. **SACK**：接收侧从 OOO 缓冲生成 SACK 块（最多 3-4 块，含最近块优先）；发送侧 scoreboard 标记已 SACK 段、重传跳过；D-SACK 接收识别（发现 spurious retransmit 喂拥塞）。
3. **fast retransmit / fast recovery**：3 dup ACK（或 SACK 推断丢段）触发重传最旧未确认段，进入 fast recovery，喂拥塞控制 loss 事件（区别于 RTO loss）。
4. **timestamps**：协商、每包携带、TSecr 回显规则、TS-based RTT 采样（替代/补充现有采样）、PAWS 校验（失败回 ACK 后丢）。
5. **persist probe**：对端零窗口 → 启动 Persist 定时器，指数退避发 1 字节探测；窗口恢复即停。
6. **PMTU**：出包置 DF；处理 ICMP Fragmentation-Needed（ICMP 错误框架已有）→ per-route PMTU 缓存 → MSS 钳制 → 重传超限段；MSS 初始值从出接口 MTU 推导。

**里程碑：** 丢包/乱序/零窗口注入测试下数据流不卡死、不重复交付、不丢数据。

### Phase 3 — 流控与发送策略

1. **delayed ACK**：`DelayedAck` 定时器联通；每 2 个满段立即 ACK，否则延迟；有数据可捎带时取消。
2. **接收窗口 ↔ app backpressure**：`rcv_wnd` 由 app ring 缓冲占用动态推导——app 不取数据窗口收缩至零；app 消费后窗口重开 → 主动发 window-update ACK。
3. **SWS 抑制**：接收侧（窗口增量不足 min(MSS, buf/2) 不通告）+ 发送侧（Nagle 之外的小窗口判断）。
4. **Nagle**：per-socket 选项（默认开）；未确认小段在途时聚合后续小段；与 pacing 正交。
5. **ECN**：握手协商（SYN/SYN-ACK 的 ECE+CWR）、出包 IP 头打 ECT、收 CE → 回 ECE 直至对端 CWR、本端收 ECE → 拥塞反馈 + 出包置 CWR。

**里程碑：** 慢消费 app 场景窗口正确收缩/恢复；小包合并符合 Nagle 预期；ECN 闭环有注入测试。

### Phase 4 — 防护与边缘语义

1. **listen backlog / SYN flood**：半开队列上限 + accept 队列上限；超限切 SYN cookie 模式（无状态 SYN-ACK，ISS 编码 cookie，最终 ACK 验证后重建连接；cookie 模式下 MSS 取近似档位，WSCALE/SACK 降级）。
2. **challenge ACK 全局速率限制**（RFC 5961）。
3. **完整 TIME_WAIT**：暗杀保护（窗口内 RST 不提前终止）、RFC 6191 新 SYN 复用（seq 大于 rcv_nxt 的 SYN 允许复用五元组）。
4. **keep-alive**：`KeepAlive` 定时器联通；per-socket 选项；空闲超时发探测，连续 N 次无响应判死走统一清理。
5. **URG 指针**：解析 URG 标志与紧急指针，紧急数据标记经 app CQE flags 透传；不实现 OOB 单独通道。
6. **TCP MD5 (RFC 2385) / TCP-AO (RFC 5925)**：选项解析与校验（校验失败静默丢）、出包签名生成；哈希/HMAC/KDF 原语在 hammer 内自带纯 Rust 实现，不引入 btls 等外部加密库；per-listener/per-connection 密钥经 control plane 配置；AO 含 KeyID/RNextKeyID 轮换。

**里程碑：** SYN flood 注入测试存活（cookie 生效、正常连接不受阻）；MD5/AO 签名互通测试通过。

### Phase 5 — e2e 验证（按用户要求置于最后）

1. `examples/tun/tcp_echo.rs`：仿 `host_ping.rs`，macOS utun + TUN→IP→TCP→app ring 完整 graph + echo server（`hammer-app` 的 `run_tcp_echo`）。
2. 宿主验证场景：nc 基本 echo、大数据流（跨多窗口）、客户端中途断开（RST 路径）、半关闭（nc 关写端后收尾部数据）。
3. 缺陷回修，必要时补集成测试回归。

**里程碑：** 上述四个场景在真实 utun 上全部通过。

## 5. 错误处理原则

- 数据面节点对畸形包一律：能归因到连接 → 按协议回应（challenge ACK/RST），不能归因 → 走 `TcpResetNode` 或丢弃；绝不 panic。
- service 侧事件处理失败记录指标后继续，不中断 control loop。
- app 侧所有 pending 操作在连接异常终止时必须收到带错误的 CQE，不允许悬挂。
- 缓冲资源（OOO 缓冲、重传队列）均有上限，超限按协议降级（丢段等重传），不无界增长。

## 6. 测试策略

- 沿用现有模式：集成测试构造包注入节点 graph（`tcp_input_nodes.rs` 等 15 个测试文件的风格），每个状态转换、每个定时器路径、每个降级路径都有焦点测试。
- 每阶段先写失败测试再实现（TDD），按现有 plan 文档的 RED→GREEN→commit 节奏。
- 注入类测试：丢包/乱序/零窗口/SYN flood/ECN CE 标记均在模拟链路完成；真实 utun 只做 Phase 5。

## 7. 实施方式

一份总 spec（本文档），实施计划按阶段拆为 5 份 plan 文档（每份独立可执行），依次进入 `superpowers:writing-plans` 流程。Phase 之间串行，前一阶段里程碑通过才进入下一阶段。

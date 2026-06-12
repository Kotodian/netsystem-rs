# TCP Phase 1 — 核心状态机 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补全 hammer TCP 接收路径与连接生命周期的核心状态机，使被动打开的 echo server 能完成「握手 → 双向收发 → 双向关闭」全链路，所有控制 ACK 真实上线、定时器到期真实回收连接。

**Architecture:** 在分支 `codex/hammer-app-ring-zero-copy` 的未提交改动之上叠加。VPP 风格 node graph 拓扑不变；dispatch table 按 `(TcpState, flags)` 扩展；被动开 SYN_RCVD 在 `TcpListenNode` 内按快照分支；标准控制包（pure ACK / challenge ACK / SYN-ACK / 最终 ACK）通过 service 侧的输出后端 `emit_buffer` 合成上线，复用现有 `tcp_output_packet_flags` 构包器；定时器到期在 service 的 `handle_tcp_worker_event` 真实处理。

**Tech Stack:** Rust 2024, `hammer-service`, `hammer-runtime::protocol::tcp::TcpControlPlane`, `hammer-core::protocol::tcp`, 现有 node 宏与 `TcpOutputBackend`，焦点 `cargo test` 覆盖。

---

## 修正后的基线（执行前必读）

阅读工作区**当前（含未提交改动）**代码后，spec 第 2 节描述的基线已过期。真实现状：

**已就绪（不要重做）：**
- `established.rs` 已实现统一接收校验：序号可接受性判断 `tcp_segment_is_sequence_acceptable`、ACK 校验 `tcp_validate_acknowledgment`、RST 关闭、FIN 处理、challenge ACK **计算**（`tcp_established_action_for_index`，established.rs:652+）。它通过 `observe_receive_ack(TcpReceiveAckObservation)` 把「该发什么 ACK」交给后端。
- 数据发送路径已就绪：`drive_tcp_output`（service.rs:2471）发包、`arm_tcp_retransmit_timer`（service.rs:2593+）武装 RTO、回 ACK 释放重传记录。`runtime_service_retransmits_unacked_payload_after_timeout` 已验证数据段 RTO 重传可用。
- 主动开升级逻辑 `promote_pending_syn_sent_connection`（service.rs:1233）已做状态升级 + 通过 shared action `CancelTimer{Connect}` 取消连接定时器 + 唤醒输出流。
- Close 相关：`tcp_state_requires_local_fin`（service.rs:3585）、`tcp_apply_deferred_local_shutdown`、输出 `include_fin` 路径（output.rs:459+）已存在。

**真实 Phase 1 缺口（本计划覆盖）：**
1. **标准控制包不上线**：`observe_receive_ack` 在 service 是空实现（service.rs:3425-3427）。established 算出的 pure ACK / challenge ACK 从不发出。
2. **被动开无 SYN_RCVD**：`TcpListenNode` 仍盲转发到 accept（listen.rs:31-47），SYN 一到就完成 accept；不发 SYN-ACK、无半开条目、不做最终 ACK 校验。
3. **主动开最终 ACK 不上线**：`promote_pending_syn_sent_connection` 升级了状态但握手第三个 ACK 没被合成发出（纯 ACK 不进 `drive_tcp_output` 的 Work，见 output.rs:483 `payload_len==0 && !include_fin`）。
4. **TimerExpired 空操作**：`TcpWorkerEvent::TimerExpired => Ok(())`（service.rs:3069 与重复处）。Connect 超时、重传次数上限判死、TimeWait 2MSL 回收都没接。
5. **关闭终态回收与 pending CQE 错误投递** 未端到端联通。

**核心抽象（贯穿全计划）：** 缺口 1/2/3 都归结到同一个原语——**合成并上线一个标准控制包**。先在 Task 1 建好这个原语（`reply.rs` + 接 `observe_receive_ack`），后续 SYN-ACK / 最终 ACK 复用它。

---

## 文件结构

- **新建** `crates/hammer-service/src/transport/tcp/reply.rs`
  控制包合成：`synthesize_ipv4_tcp_control`（pure ACK / challenge ACK / SYN-ACK / RST|ACK 字节构造，镜像 `reset.rs` 布局），以及把它装进 buffer 并经输出后端上线的 `emit_tcp_control_packet`。单一职责：从「(local, remote, seq, ack, window, flags, options)」到「一个上线的控制包」。
- **修改** `crates/hammer-service/src/transport/tcp/mod.rs` — re-export `reply`。
- **修改** `crates/hammer-service/src/service.rs` — 接 `observe_receive_ack`（Task 1）、被动开 service 侧 install/promote（Task 3）、`TimerExpired` 真实处理（Task 5）、关闭终态回收（Task 6）。
- **修改** `crates/hammer-service/src/transport/tcp/listen.rs` — 被动开状态机（Task 2/3）。
- **修改** `crates/hammer-service/src/transport/tcp/state.rs` — dispatch 表补 `(SynRcvd, ACK) → Listen`（已存在，确认）、被动开所需条目。
- **修改** `crates/hammer-service/src/transport/tcp/syn_sent.rs` — 主动开最终 ACK 触发（Task 4）。
- **测试** `crates/hammer-service/tests/tcp_reply_nodes.rs`（新建，Task 1/2/4 节点级），`crates/hammer-service/tests/tcp_input_nodes.rs`（改 listen 期望，Task 2），service 库内测试模块（Task 3/5/6）。

---

## Task 1: 标准控制包上线原语（pure ACK / challenge ACK）

把 established 已计算好的 `TcpReceiveAckObservation` 真实合成成包并上线，替换 service 的空实现。

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/reply.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/service.rs:3425-3427`
- Test: `crates/hammer-service/tests/tcp_reply_nodes.rs`

- [ ] **Step 1: 先确认参考实现的精确字节布局**

阅读并对照（不改）：
- `crates/hammer-service/src/transport/tcp/reset.rs:391-486`（`synthesize_ipv4_tcp_reset`：源/目地址端口交换、seq/ack 计算、flags、TCP 校验和、IP 校验和、metadata 反转）。
- `crates/hammer-service/src/service.rs:360-410`（输出 buffer 如何经 `output.emit_buffer(buffers, index)` 上线）。
- `crates/hammer-service/tests/tcp_input_nodes.rs:2097-2178`（`ipv4_tcp_packet_with_seq_ack` / `write_tcp_segment` / `ipv4_l4_checksum` 的字节布局，合成必须与之一致以便测试断言）。

- [ ] **Step 2: 写失败测试（合成正确性）**

新建 `crates/hammer-service/tests/tcp_reply_nodes.rs`：

```rust
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use hammer_service::transport::tcp::{synthesize_ipv4_tcp_control, TcpControlFlags};

const ACK: u8 = 0x10;

fn v4(a: u8, b: u8, c: u8, d: u8, port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), port)
}

#[test]
fn synthesize_control_ack_has_reversed_tuple_and_fields() {
    // local = 我方 (echo server)，remote = 对端
    let local = v4(192, 0, 2, 10, 443);
    let remote = v4(10, 0, 0, 1, 50_001);
    let packet = synthesize_ipv4_tcp_control(
        local,
        remote,
        /* send_sequence (seq) */ 1001,
        /* receive_acknowledgment (ack) */ 7001,
        /* window */ 65_535,
        TcpControlFlags::from_bits(ACK),
        /* options */ &[],
    )
    .expect("synthesize control ack");

    // IPv4 头：source = local.ip()，destination = remote.ip()
    assert_eq!(&packet[12..16], &Ipv4Addr::new(192, 0, 2, 10).octets());
    assert_eq!(&packet[16..20], &Ipv4Addr::new(10, 0, 0, 1).octets());
    // TCP 头从 offset 20 开始：source_port=443, dest_port=50001
    assert_eq!(u16::from_be_bytes([packet[20], packet[21]]), 443);
    assert_eq!(u16::from_be_bytes([packet[22], packet[23]]), 50_001);
    // seq / ack
    assert_eq!(u32::from_be_bytes([packet[24], packet[25], packet[26], packet[27]]), 1001);
    assert_eq!(u32::from_be_bytes([packet[28], packet[29], packet[30], packet[31]]), 7001);
    // flags（offset 33）含 ACK
    assert_eq!(packet[33] & ACK, ACK);
    // window（offset 34..36）
    assert_eq!(u16::from_be_bytes([packet[34], packet[35]]), 65_535);
    // TCP 校验和非零（已计算）
    assert_ne!(u16::from_be_bytes([packet[36], packet[37]]), 0);
}
```

- [ ] **Step 3: 运行测试确认 RED**

Run: `cargo test -p hammer-service --test tcp_reply_nodes synthesize_control_ack_has_reversed_tuple_and_fields -- --exact`
Expected: 编译失败，`synthesize_ipv4_tcp_control` / `TcpControlFlags` 不存在。

- [ ] **Step 4: 实现 `reply.rs` 合成器**

在 `crates/hammer-service/src/transport/tcp/reply.rs` 写入。`TcpControlFlags` 是 `u8` 薄包装（与 output.rs 的 `TCP_FLAG_*` 常量一致）。`synthesize_ipv4_tcp_control` 镜像 `reset.rs` 与 `write_tcp_segment` 的布局：IPv4 头 20 字节 + TCP 头 20 字节(+options)；填端口/seq/ack/flags/window；用 `ipv4_l4_checksum` 等价逻辑算 TCP 校验和；算 IP 头校验和。仅支持 IPv4（v6 留 Phase 后续），remote/local 非 v4 返回 `CoreError::internal`。

关键签名（必须与测试一致）：

```rust
use std::net::SocketAddr;
use hammer_core::error::{CoreError, CoreResult};

pub const TCP_FLAG_FIN: u8 = 0x01;
pub const TCP_FLAG_SYN: u8 = 0x02;
pub const TCP_FLAG_RST: u8 = 0x04;
pub const TCP_FLAG_PSH: u8 = 0x08;
pub const TCP_FLAG_ACK: u8 = 0x10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpControlFlags(u8);

impl TcpControlFlags {
    #[inline]
    pub const fn from_bits(bits: u8) -> Self { Self(bits) }
    #[inline]
    pub const fn bits(self) -> u8 { self.0 }
}

/// 合成一个 IPv4 TCP 控制段。`local` 是本端（包源），`remote` 是对端（包目的）。
pub fn synthesize_ipv4_tcp_control(
    local: SocketAddr,
    remote: SocketAddr,
    send_sequence: u32,
    receive_acknowledgment: u32,
    window: u16,
    flags: TcpControlFlags,
    options: &[u8],
) -> CoreResult<Vec<u8>> {
    let (local_ip, remote_ip) = match (local.ip(), remote.ip()) {
        (std::net::IpAddr::V4(l), std::net::IpAddr::V4(r)) => (l, r),
        _ => return Err(CoreError::internal("ipv4 control packet requires v4 addrs")),
    };
    debug_assert_eq!(options.len() % 4, 0, "tcp options must be 4-byte aligned");
    let tcp_header_len = 20 + options.len();
    let mut packet = vec![0u8; 20 + tcp_header_len];
    // --- IPv4 header ---
    packet[0] = 0x45;
    let total_len = packet.len() as u16;
    packet[2..4].copy_from_slice(&total_len.to_be_bytes());
    packet[8] = 64;     // TTL
    packet[9] = 6;      // protocol = TCP
    packet[12..16].copy_from_slice(&local_ip.octets());
    packet[16..20].copy_from_slice(&remote_ip.octets());
    // --- TCP header ---
    let tcp = &mut packet[20..];
    tcp[0..2].copy_from_slice(&local.port().to_be_bytes());
    tcp[2..4].copy_from_slice(&remote.port().to_be_bytes());
    tcp[4..8].copy_from_slice(&send_sequence.to_be_bytes());
    tcp[8..12].copy_from_slice(&receive_acknowledgment.to_be_bytes());
    tcp[12] = ((tcp_header_len / 4) as u8) << 4; // data offset
    tcp[13] = flags.bits();
    tcp[14..16].copy_from_slice(&window.to_be_bytes());
    if !options.is_empty() {
        tcp[20..20 + options.len()].copy_from_slice(options);
    }
    // --- checksums ---
    let checksum = ipv4_l4_checksum(local_ip, remote_ip, 6, &packet[20..]);
    packet[20 + 16..20 + 18].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    Ok(packet)
}
```

`ipv4_l4_checksum` / `update_ipv4_header_checksum` / `internet_checksum`：若 `reset.rs` 已有等价私有函数则复用并 `pub(crate)` 暴露；否则把 `tests/tcp_input_nodes.rs:2185-2210` 的实现搬到 `reply.rs` 作为内部函数（同一算法）。

- [ ] **Step 5: 导出并跑合成测试确认 GREEN**

在 `mod.rs` 加 `pub mod reply; pub use reply::{synthesize_ipv4_tcp_control, TcpControlFlags};`。
Run: `cargo test -p hammer-service --test tcp_reply_nodes synthesize_control_ack_has_reversed_tuple_and_fields -- --exact`
Expected: PASS。

- [ ] **Step 6: 实现上线辅助并接入 service `observe_receive_ack`**

在 `reply.rs` 增加把合成包装进 data-plane buffer 并经输出后端上线的函数：

```rust
use hammer_adapter::DataPlaneBuffers;
use hammer_core::error::CoreResult;
use crate::transport::tcp::output::TcpOutputBackend;
use hammer_core::net::RouteMetadata; // 路径以现有 import 为准

/// 分配 buffer、写入 IPv4/TCP 字节、设置 cursor，并经输出后端上线。
pub fn emit_tcp_control_packet(
    buffers: &DataPlaneBuffers,
    output: &dyn TcpOutputBackend,
    packet: &[u8],
    metadata: RouteMetadata,
) -> CoreResult<()> {
    let index = buffers.alloc_index_with_bytes(metadata, packet)?;
    // 设置 packet cursor，使下游/驱动按 TCP 段处理；镜像 tests::stamp_tcp_cursor 的逻辑
    set_ipv4_tcp_cursor(buffers, index, packet)?;
    output.emit_buffer(buffers, index)
}
```

`alloc_index_with_bytes` / cursor 设置的精确 API 以 `service.rs` 中 `drive_tcp_output` 周边（service.rs:360-414 的 `emit_tcp_output_buffer`/`tcp_output_payload_source`）和 `tests/tcp_input_nodes.rs:2036-2061` 的 `stamp_tcp_cursor` 为准——实现 `set_ipv4_tcp_cursor` 时直接照搬 `stamp_tcp_cursor` 的 cursor 计算（IPv4 头长、TCP 头长、payload offset）。

在 `service.rs` 把空实现替换为真实上线（observation 给出 local/remote/seq/ack/window/kind）：

```rust
fn observe_receive_ack(&self, observation: TcpReceiveAckObservation) -> Result<(), CoreError> {
    self.emit_tcp_receive_ack(observation)
        .map_err(|err| CoreError::internal(format!("runtime tcp receive ack: {err}")))
}
```

`emit_tcp_receive_ack` 内：取 data-plane buffers（`hammer_runtime::spawn::with_data_plane_buffers(Clone::clone)`，见 service.rs:2481）、合成 `synthesize_ipv4_tcp_control(local, remote, send_sequence, receive_acknowledgment, advertised_window as u16, TcpControlFlags::from_bits(TCP_FLAG_ACK), &[])`、构造反向 metadata（source=local, destination=remote，`Network::Tcp`）、调 `emit_tcp_control_packet(&buffers, &self.tcp_output, &packet, metadata)`。`kind` 为 `Ack` 或 `Challenge` 都发 ACK，字段已由 established 算好，无需区分。

- [ ] **Step 7: 写失败测试（established 收到不可接受段 → 上线 challenge ACK）**

此测试在 service 库测试模块（能装真实输出后端并断言上线包）。参照 `service.rs` 中现有 established 接收测试（搜 `observe_receive_ack`/`tcp_established` 相关 service 测试，或 `runtime_service_*established*`）搭建：建立一个 Established 连接，注入一个序号在窗口外的段，断言输出后端捕获到 1 个 ACK 包、其 ack 字段 == 连接 `rcv_nxt`、tuple 反转。

```rust
#[test]
fn runtime_service_out_of_window_segment_emits_challenge_ack() {
    // 1. 用现有 helper 建立 Established 连接（参照同文件中既有 established 测试的 setup）。
    // 2. 注入 seq 远超 rcv_nxt+rcv_wnd 的段（不可接受）。
    // 3. 运行 established 节点 + 处理 observe_receive_ack。
    // 4. 断言：输出后端记录到 1 个包，flags & ACK != 0，ack == rcv_nxt，源端口==本地端口。
}
```

- [ ] **Step 8: 跑测试确认 GREEN，然后提交**

Run: `cargo test -p hammer-service --test tcp_reply_nodes`
Run: `cargo test -p hammer-service runtime_service_out_of_window_segment_emits_challenge_ack`
Expected: PASS。

```bash
git add crates/hammer-service/src/transport/tcp/reply.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/service.rs crates/hammer-service/tests/tcp_reply_nodes.rs
git commit -m "hammer-service(Feat): emit standalone tcp control acks"
```

---

## Task 2: 被动开 part A — SYN 建半开 SYN_RCVD 并发 SYN-ACK（不完成 accept）

`TcpListenNode` 从盲转发改为：收纯 SYN 时建半开条目、发 SYN-ACK、**不** posts Accepted。

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state.rs`
- Modify: `crates/hammer-service/src/service.rs`（被动开半开注册 + SYN-ACK 上线）
- Modify: `crates/hammer-service/tests/tcp_input_nodes.rs`（更新 listen 不再首包完成 accept 的期望）
- Test: `crates/hammer-service/tests/tcp_reply_nodes.rs`

- [ ] **Step 1: 先确认 listen/accept 现状与 SYN-ACK 所需输入**

阅读（不改）：`listen.rs` 全文、`accept.rs:417-520`（accept 如何 take pending、读握手观察、调 `backend.accept`）、`syn_sent.rs:543-593`（如何从 buffer 读 `tcp_handshake_observation` 与 metadata 提取 local/remote）。确认半开需要的字段：local/remote、irs=seg.seq、ISS（本端生成）、对端 MSS/WSCALE（`tcp_handshake_observation` 已解析）。

- [ ] **Step 2: 写失败测试（纯 SYN → 发 SYN-ACK，不完成 accept）**

在 `tcp_reply_nodes.rs` 增加 service 级测试：注册一个 listener，注入纯 SYN，断言：(a) 输出后端记录到 1 个 SYN-ACK 包（flags 含 SYN|ACK、ack==seg.seq+1）；(b) 不产生 `AppCqeData::Accepted`（accept backend 未被调用 / app CQE 队列无 Accepted）。

```rust
#[test]
fn runtime_service_listen_syn_emits_syn_ack_without_accept() {
    // 1. 注册 listener（参照同文件/服务测试既有 listener setup）。
    // 2. 注入 ipv4_tcp_packet(remote, rport, local, LISTEN_PORT, tcp_flags(false,true,false,false), &[])。
    // 3. 运行 graph。
    // 4. 断言输出后端有 1 个 SYN|ACK 包；ack == syn_seq + 1。
    // 5. 断言无 Accepted CQE / accept backend 未触发。
}
```

- [ ] **Step 3: 跑测试确认 RED**

Run: `cargo test -p hammer-service runtime_service_listen_syn_emits_syn_ack_without_accept -- --exact`
Expected: FAIL（当前 listen 直接转 accept，首包就完成 accept、且不发 SYN-ACK）。

- [ ] **Step 4: 改 `TcpListenNode` 为被动开分支**

`process` 内按已发布的连接快照分支：若该包命中 listener（无 established 半开）且为纯 SYN → 走「建半开 + 发 SYN-ACK」路径（通过 listen 节点的后端 hook 上报 service，service 负责注册半开 + 经 Task 1 原语上线 SYN-ACK）；若命中半开条目（state==SynRcvd）且为 ACK → 走「最终 ACK」路径（Task 3）。拓扑不变——仍只有一个 `Accept` next，分支在节点内按快照完成；纯 SYN 与 challenge 情形不再无条件转 accept。

给 `TcpListenNode` 增加一个 backend hook（镜像 `TcpSynSentNode` 的 `observe_syn_ack` 模式，syn_sent.rs:26-55 与 backend slot 模式）：`observe_passive_open(TcpPassiveOpenObservation { listener_id, local, remote, irs, capabilities })`。节点从 buffer 读 `tcp_handshake_observation` 构造 observation 上报后丢弃 SYN（SYN-ACK 由 service 合成，不沿 graph 转发原 SYN）。

- [ ] **Step 5: service 侧注册半开 + 上线 SYN-ACK**

在 service 实现 `TcpListenBackend::observe_passive_open`：
- 生成 ISS（用现有 ISS 生成逻辑；若无则 `iss = 初值`，Phase 4 再换安全 ISS）。
- 在 `tcp_connections` 注册半开条目：state=`SynRcvd`、local/remote、iss、irs、`rcv_nxt = irs + 1`、`snd_nxt = iss`、`snd_una = iss`、应用对端 capabilities（复用 `tcp_apply_handshake_capabilities`，service.rs:3552）。
- 在 lookup 的 pending(syn_sent) 表插入键（`TcpV4PendingConnectionKey::new(scope, local_port, remote_ip, remote_port)`），`publish_tcp_lookup()` + `publish_tcp_app_ingress()` + 发布连接快照。
- 合成并上线 SYN-ACK：`synthesize_ipv4_tcp_control(local, remote, iss, irs+1, win, SYN|ACK, &options)`，options 含本端 MSS（复用 `options.rs` 的构造；Phase 1 至少回 MSS）。经 `emit_tcp_control_packet`。
- 武装 SYN_RCVD 重传定时器：`arm_tcp_retransmit_timer(connection_id)`（已存在）或等价 `arm_once(connection_id, Retransmit, rto, ..)`。

- [ ] **Step 6: 更新 dispatch 表与既有 listen 测试期望**

确认 `state.rs` 有 `(SynRcvd, ACK) → Listen`（基线审计显示已存在，第 4 条）。更新 `tcp_input_nodes.rs` 中「listen 首个 SYN 即完成 accept / 转发到 listen_sink」的旧断言：现在纯 SYN 被节点消费（上报 + 丢弃），listen_sink 不再收到原 SYN。把受影响测试改为断言上报路径或调整为新语义（逐个改，不留 TODO）。

- [ ] **Step 7: 跑测试确认 GREEN，提交**

Run: `cargo test -p hammer-service runtime_service_listen_syn_emits_syn_ack_without_accept -- --exact`
Run: `cargo test -p hammer-service --test tcp_input_nodes`
Expected: PASS。

```bash
git add crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/src/transport/tcp/state.rs crates/hammer-service/src/service.rs crates/hammer-service/tests/tcp_input_nodes.rs crates/hammer-service/tests/tcp_reply_nodes.rs
git commit -m "hammer-service(Feat): passive-open syn-rcvd emits syn-ack"
```

---

## Task 3: 被动开 part B — 最终 ACK 升级 SYN_RCVD→ESTABLISHED 并完成 accept

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Test: `crates/hammer-service/tests/tcp_reply_nodes.rs`

- [ ] **Step 1: 写失败测试（半开 + 最终 ACK → Established + Accepted）**

```rust
#[test]
fn runtime_service_syn_rcvd_final_ack_promotes_and_completes_accept() {
    // 前置：复用 Task 2 测试把 listener 推进到对某对端的 SYN_RCVD（注入 SYN）。
    // 1. 注入最终 ACK：seq = irs+1, ack = iss+1, flags = ACK。
    // 2. 运行 graph + 处理上报。
    // 3. 断言：连接 state == Established；产生 1 个 AppCqeData::Accepted；
    //    校验失败的 ACK（ack != iss+1）不应升级（可加第二条负向断言）。
}
```

- [ ] **Step 2: 跑测试确认 RED**

Run: `cargo test -p hammer-service runtime_service_syn_rcvd_final_ack_promotes_and_completes_accept -- --exact`
Expected: FAIL。

- [ ] **Step 3: listen 节点处理 SynRcvd 的最终 ACK**

`TcpListenNode` 命中半开条目（快照 state==SynRcvd）且包为 ACK 时：从 buffer 读握手观察，上报 `observe_passive_final_ack(TcpPassiveFinalAck { local, remote, seg_seq, seg_ack })`，丢弃该 ACK。

- [ ] **Step 4: service 升级 + 完成 accept**

实现 `observe_passive_final_ack`：
- 定位半开 registration；校验 `seg_ack == iss + 1` 且 `seg_seq == rcv_nxt`，否则丢弃不升级（记录指标）。
- 升级：state=`Established`、`snd_una = seg_ack`、初始化发送窗口、用对端 MSS 初始化拥塞状态（复用 `promote` 路径中相同逻辑，service.rs:1320-1334）。
- 取消 SYN_RCVD 重传定时器：`CancelTimer{Retransmit}` / `cancel(connection_id, Retransmit)`。
- 从 pending 表移除键、在 established 表插入键；`publish_tcp_lookup()` + ingress + 快照。
- **此时**才完成 accept：复用 accept 完成路径投递 `AppCqeData::Accepted`（调用与 `IncomingConnection` 处理相同的 accept 完成函数，service.rs:3013/3115 附近）。

- [ ] **Step 5: 跑测试确认 GREEN，提交**

Run: `cargo test -p hammer-service runtime_service_syn_rcvd_final_ack_promotes_and_completes_accept -- --exact`
Expected: PASS。

```bash
git add crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/src/service.rs crates/hammer-service/tests/tcp_reply_nodes.rs
git commit -m "hammer-service(Feat): syn-rcvd final ack completes accept"
```

---

## Task 4: 主动开 — 最终 ACK 上线

`promote_pending_syn_sent_connection` 已升级状态并取消 Connect 定时器，但握手第三个 ACK 没发出。补发。

**Files:**
- Modify: `crates/hammer-service/src/service.rs`（在 promote 成功后上线最终 ACK）
- Test: `crates/hammer-service/tests/tcp_reply_nodes.rs`

- [ ] **Step 1: 写失败测试（SYN-ACK 观察 → 升级 + 上线最终 ACK，不新增 connect CQE）**

```rust
#[test]
fn runtime_service_active_open_emits_final_ack_on_syn_ack() {
    // 1. 注册一个 pending SYN_SENT（参照 promote_pending_syn_sent_for_test 既有用法，service.rs:5272/6485）。
    // 2. 通过 promote_pending_syn_sent_for_test 喂一个 SYN-ACK 观察（含 acknowledgment、window）。
    // 3. 断言：输出后端记录到 1 个 ACK 包，ack == syn_ack_seq + 1，seq == iss + 1。
    // 4. 断言：不产生新的 “connected” CQE（保持 phase-1 语义）。
}
```

- [ ] **Step 2: 跑测试确认 RED**

Run: `cargo test -p hammer-service runtime_service_active_open_emits_final_ack_on_syn_ack -- --exact`
Expected: FAIL（升级了但没发 ACK）。

- [ ] **Step 3: 在 promote 成功后上线最终 ACK**

在 `promote_pending_syn_sent_connection`（service.rs:1233）升级为 Established 且 `observation` 存在时，于发布快照后合成并上线最终 ACK：`synthesize_ipv4_tcp_control(local, remote, snd_nxt /*=iss+1*/, rcv_nxt /*=irs+1*/, win, ACK, &[])` → `emit_tcp_control_packet`。把上线动作放在 `PromotePendingSynSentResult` 处理处（拿得到 buffers/输出后端的地方），或在 `handle_tcp_syn_sent_observation`（service.rs:2797 附近调用 promote 之处）紧随其后执行。不引入新 CQE。

- [ ] **Step 4: 跑测试确认 GREEN，提交**

Run: `cargo test -p hammer-service runtime_service_active_open_emits_final_ack_on_syn_ack -- --exact`
Run: `cargo test -p hammer-service --test app_tcp_connect_runtime`
Expected: PASS（既有主动开 phase-1 测试仍绿）。

```bash
git add crates/hammer-service/src/service.rs crates/hammer-service/tests/tcp_reply_nodes.rs
git commit -m "hammer-service(Feat): active-open emits final handshake ack"
```

---

## Task 5: 定时器到期真实处理（Connect 超时 / 重传上限判死 / TimeWait 回收）

替换 `TcpWorkerEvent::TimerExpired => Ok(())`。

**Files:**
- Modify: `crates/hammer-service/src/service.rs`（两处 `TimerExpired` 分支：3069、3236 附近）
- Test: service 库测试模块

- [ ] **Step 1: 写失败测试（Connect 超时回收 + TimeWait 到期回收）**

```rust
#[test]
fn runtime_service_connect_timer_expiry_reclaims_connection() {
    // 1. 注册一个 pending SYN_SENT 连接（promote_pending_syn_sent_for_test 的前置 setup）。
    // 2. handle_tcp_worker_event_for_test(TimerExpired { connection_id, timer_id, kind: Connect })。
    // 3. 断言：连接被移除（lookup 查不到）、pending app 操作收到错误 CQE（result_code != 0）。
}

#[test]
fn runtime_service_time_wait_timer_expiry_reclaims_connection() {
    // 1. 把连接置于 TimeWait（经关闭路径或直接构造）。
    // 2. handle_tcp_worker_event_for_test(TimerExpired { kind: TimeWait, .. })。
    // 3. 断言：连接被移除、五元组释放。
}
```

- [ ] **Step 2: 跑测试确认 RED**

Run: `cargo test -p hammer-service runtime_service_connect_timer_expiry_reclaims_connection runtime_service_time_wait_timer_expiry_reclaims_connection -- --exact`
Expected: FAIL（TimerExpired 当前是 no-op）。

- [ ] **Step 3: 实现 `TimerExpired` 分支**

```rust
TcpWorkerEvent::TimerExpired { connection_id, timer_id: _, kind } => {
    match kind {
        // 终结类：到期即关闭并回收
        TcpTimerKind::Connect
        | TcpTimerKind::Retransmit
        | TcpTimerKind::KeepAlive => {
            if let Some(reason) = kind.close_reason_on_expiry() {
                self.handle_tcp_worker_event(TcpWorkerEvent::Closed { connection_id, reason })
            } else {
                Ok(())
            }
        }
        // TimeWait 到期：静默回收（非错误关闭）
        TcpTimerKind::TimeWait => self.reclaim_tcp_connection(connection_id),
        // 非终结类：Phase 1 no-op（Persist→Phase2, DelayedAck→Phase3）
        TcpTimerKind::DelayedAck | TcpTimerKind::Persist => Ok(()),
    }
}
```

注意：`Retransmit` 到期在数据路径已由 `arm_tcp_retransmit_timer` 的回调驱动重发（不经此事件）；此处的 `Retransmit` 终结仅作为「重传次数达上限后控制面注入的判死」兜底。`close_reason_on_expiry`（core mod.rs:1041）已给出 Connect→ConnectTimeout、Retransmit→RetransmitTimeout、KeepAlive→KeepAliveTimeout、其余→None。

`reclaim_tcp_connection(connection_id)`：复用现有终态清理三件套——移除 registration、`publish_tcp_lookup` + `publish_tcp_app_ingress` + 发布连接快照、`cancel_tcp_connection_timers`（service.rs，基线审计列出的 `cancel_tcp_connection_timers` / `remember_closed_connection`）。若 `Closed` 分支已做这些，让 `reclaim_tcp_connection` 与 `Closed` 处理共用同一私有函数。

- [ ] **Step 4: 跑测试确认 GREEN，提交**

Run: `cargo test -p hammer-service runtime_service_connect_timer_expiry_reclaims_connection runtime_service_time_wait_timer_expiry_reclaims_connection -- --exact`
Expected: PASS。

```bash
git add crates/hammer-service/src/service.rs
git commit -m "hammer-service(Fix): handle tcp timer expiry and reclaim state"
```

---

## Task 6: 关闭终态联通 — Close SQE→FIN 与 pending CQE 错误投递

确保对端先关（CLOSE_WAIT 半关闭 + app 续发 + Close→FIN→LAST_ACK→Closed）与异常关闭（pending 操作收错误 CQE）端到端跑通。

**Files:**
- Modify: `crates/hammer-service/src/service.rs`
- Test: service 库测试模块

- [ ] **Step 1: 写失败测试（对端 FIN→CLOSE_WAIT，app 续发后 Close→FIN→LAST_ACK，收末 ACK→Closed）**

```rust
#[test]
fn runtime_service_passive_close_completes_full_shutdown() {
    // 1. 建立 Established 连接（echo server 侧）。
    // 2. 注入对端 FIN(seq=rcv_nxt)；断言 state==CloseWait，app 收到 shutdown（recv 完成带 FIN flag/EOF）。
    // 3. app 续发剩余数据（send）后提交 Close SQE；断言发出 FIN，state==LastAck。
    // 4. 注入对端对该 FIN 的 ACK；断言 state==Closed，产生 AppCqeData::Closed。
}

#[test]
fn runtime_service_reset_delivers_error_cqe_to_pending_ops() {
    // 1. 建立 Established 连接，挂起一个 recv。
    // 2. 注入窗口内 RST。
    // 3. 断言：pending recv 收到错误 CQE（result_code != 0），连接被回收。
}
```

- [ ] **Step 2: 跑测试确认 RED**

Run: `cargo test -p hammer-service runtime_service_passive_close_completes_full_shutdown runtime_service_reset_delivers_error_cqe_to_pending_ops -- --exact`
Expected: FAIL（关闭终态/错误投递未联通）。

- [ ] **Step 3: 联通关闭路径**

- 对端 FIN：established 已检测 FIN 并经 `observe_close`（RemoteFin）上报；确保 service 把 state 推进到 CLOSE_WAIT 并向 app 投递 shutdown（FIN flag 经 `AppCqeFlags::FIN`，ring.rs）。CLOSE_WAIT 期间允许 app 继续 send（不提前关发送方向）。
- Close SQE：app 提交 `AppSqe::CloseFlow`（ring.rs:198）→ service 排空 `pending_send_payloads` 后追加 FIN（`tcp_state_requires_local_fin` + `tcp_apply_deferred_local_shutdown` 已存在，确认其在 CLOSE_WAIT→LAST_ACK 与 ESTABLISHED→FIN_WAIT_1 都触发 FIN 入队），经 `drive_tcp_output` 的 `include_fin` 路径发出。
- 末 ACK：established ACK 校验中 `acknowledges_local_fin`（established.rs:266）已识别「对端 ACK 了我方 FIN」；service 据此把 LAST_ACK→Closed（或 FIN_WAIT_1→FIN_WAIT_2 等），Closed 时回收并投递 `AppCqeData::Closed`。
- RST/异常：在终态回收函数里，对该连接所有 pending app 操作投递带非零 `result_code` 的错误 CQE（与 `Closed` 正常完成区分）。

dispatch 表确认覆盖 CLOSE_WAIT/LAST_ACK/FIN_WAIT*/CLOSING 的 `(state, flags)`（基线审计第 8-12 条已 fill_row，确认 ACK/FIN/RST override 正确路由到 established）。

- [ ] **Step 4: 跑测试确认 GREEN，提交**

Run: `cargo test -p hammer-service runtime_service_passive_close_completes_full_shutdown runtime_service_reset_delivers_error_cqe_to_pending_ops -- --exact`
Expected: PASS。

```bash
git add crates/hammer-service/src/service.rs
git commit -m "hammer-service(Feat): wire tcp close lifecycle and error cqes"
```

---

## Task 7: 最终验证

**Files:** Verify only

- [ ] **Step 1: 格式化**

Run: `cargo fmt --all`
Expected: 无残留 diff。

- [ ] **Step 2: 焦点 TCP 测试全绿**

Run: `cargo test -p hammer-service --test tcp_reply_nodes --test tcp_input_nodes --test tcp_connection_state --test tcp_output --test tcp_congestion_node --test app_tcp_runtime --test app_tcp_connect_runtime`
Expected: PASS。

- [ ] **Step 3: service 库内 TCP 行为测试**

Run: `cargo test -p hammer-service runtime_service_`
Expected: PASS。

- [ ] **Step 4: runtime 侧 TCP 控制面/定时器**

Run: `cargo test -p hammer-runtime tcp`
Expected: PASS。

- [ ] **Step 5: 全工作区**

Run: `cargo test --workspace`
Expected: PASS。

- [ ] **Step 6: 若 fmt 有改动则提交**

```bash
git add -A
git commit -m "hammer-service(Debug): verify tcp phase 1 core state machine"
```

---

## Phase 1 验收

- 被动开：纯 SYN 建 SYN_RCVD 半开 + 发 SYN-ACK，不提前 accept；最终 ACK 校验通过才 Established + posts `Accepted`。
- 主动开：SYN-ACK 观察后升级 Established 并上线最终 ACK，不新增 connect CQE。
- 标准控制包（pure ACK / challenge ACK / SYN-ACK / 最终 ACK）真实上线，tuple 反转、seq/ack/window/校验和正确。
- 定时器到期真实处理：Connect/重传上限判死回收 + app 错误 CQE；TimeWait 2MSL 回收。
- 关闭：对端先关全链路（CLOSE_WAIT 半关闭 → Close→FIN → LAST_ACK → Closed）+ RST 异常下 pending 操作收错误 CQE。
- `cargo test --workspace` 通过。

下一阶段（Phase 2 可靠性增强：乱序缓冲 / SACK / fast retransmit / timestamps / persist / PMTU）单独成计划。

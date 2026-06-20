# TCP SACK/DSACK/乱序接收 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Any item in **审批项** must先展示给用户并获批，然后才能开始 Rust 实现。

**Goal:** 以当前仓库代码为准，补齐 TCP Established 路径上的入站 SACK 解析透传、出站 SACK/DSACK 选项写出、乱序 payload 保留与缺口闭合后的顺序交付，同时删掉公开 `TcpSentSegment`，让 ACK/SACK/RACK/TLP 都围绕 recovery 私有状态工作。

**Architecture:** 参考 VPP 语义，但不照搬 VPP API。TCP 连接继续拥有序号、ACK、SACK、DSACK、恢复和计时器决策；session/runtime 继续拥有 buffer 和 app completion；TCP output 继续只从 `TcpSegment` prepend header。除 app/session 边界外不复制 payload，不增加 TCP 专用 runtime/buffer helper，不增加 standalone recv queue。

**Tech Stack:** Rust 2024, `hammer-core::protocol::tcp`, `hammer-service::transport::tcp`, `hammer-adapter` dataplane buffer, `hammer-infra::vec`, 本地 `third_party/vpp` 参考代码。

## 执行记录位置

- 本轮 SDD 执行记录落在 `docs/superpowers/sdd/2026-06-19-tcp-sack-dsack-ooo/`。
- 当前沙箱不允许写 `.git/sdd`，所以先用 docs 路径保存 progress / brief / review artifact；如果后续需要切回 `.git/sdd`，再单独申请权限。

## Global Constraints

- 以当前代码为准，不回退到旧设计稿。
- session runtime 是唯一调度者；不引入 cc sibling/node。
- congestion control 继续通过 `TcpConnection<S, C>` 持有，只接收 typed packet/ack/loss facts。
- app/session 是唯一允许复制 payload 的边界；session/TCP/recovery/output 之后不允许中间 payload `Vec` 或私有 payload 副本。
- TCP output 只 prepend TCP header；session/runtime 不知道 TCP header 字段。
- timer expiry 必须只处理 runtime 传进来的那个准确 token/kind，不能扫描全部 `TcpConnectionTimerKind`。
- 不新增 standalone recv queue，不新增 range/view/carrier/helper 风格 API。
- `TcpSentSegment` 必须从公开面删除；recovery 内部记录必须留在 `recovery.rs` 私有范围。
- 如确实需要新的类型或对外 API，必须在实现前走本计划里的审批项。

## 当前代码事实

- `crates/hammer-core/src/protocol/tcp/options.rs`
  - 已有 `TcpSackBlock`、`ParsedTcpOptions::sack_blocks`、入站 SACK 解析。
  - `TCP_MAX_SACK_BLOCKS` 目前是私有常量。
- `crates/hammer-core/src/protocol/tcp/segment.rs`
  - `write_tcp_segment_header()` 现在只在 SYN 路径写 options；Established ACK 不会写 SACK/DSACK。
- `crates/hammer-service/src/transport/tcp/segment.rs`
  - `parse_tcp_packet()` 目前把 options 里的 capability 带出来了，但丢掉了 `sack_blocks`。
  - `TcpSegment` 现在只表达基本 header intent + `payload_len`，没有出站 SACK 信息。
- `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - `commit_payload_tx()` 仍然公开构造 `TcpSentSegment`。
  - `receive_ack()` 只做 cumulative ACK/window 更新。
  - `accept_payload()` 只接受严格 in-order payload。
- `crates/hammer-service/src/transport/tcp/session.rs`
  - `receive_data()` 目前只有两条路：in-order 时 `advance + truncate_chain + enqueue_rx`，否则 free payload buffer。
  - ACK path 还没有把入站 SACK blocks 喂给 `TcpRecoveryState`。
- `crates/hammer-service/src/session/app.rs`
  - `handle_submission_descriptor()` 现在对 `AppOpcode::Recv` 直接忽略，不会因为新的 recv SQE 到来而重新尝试交付已持有的 RX payload。
- `crates/hammer-service/src/transport/tcp/recovery.rs`
  - `TcpRecoveryState::on_sack_blocks()` 已经存在。
  - `TcpSentSegment` 仍然是公开类型，`tests/tcp_rack_tlp.rs` 直接手工构造它。

## VPP 参考点

- `third_party/vpp/src/vnet/tcp/tcp_input.c`
  - `tcp_session_enqueue_ooo()`：乱序 payload 以相对 `rcv_nxt` 的方式保留，同时更新 SACK list。
  - `tcp_update_sack_list()`：缺口闭合后收缩/删除已经被顺序交付的 SACK block。
- `third_party/vpp/src/vnet/tcp/tcp_output.c`
  - `tcp_make_established_options()`：Established ACK 上按当前连接状态写 SACK blocks。
  - DSACK 先于普通 SACK blocks 输出。
- `third_party/vpp/src/vlib/buffer_funcs.h`
  - buffer chain 共享依赖 `attach_clone`/refcount 语义；SACK 计划不新增偏离这套语义的 buffer API。

## 文件职责图

- `crates/hammer-core/src/protocol/tcp/options.rs`
  - 负责 SACK option 的 parse/write primitive。
- `crates/hammer-core/src/protocol/tcp/segment.rs`
  - 负责把 `TcpSegmentHeader` 和该次 segment 的可选 SACK/DSACK 事实编码成 TCP header bytes。
- `crates/hammer-service/src/transport/tcp/segment.rs`
  - 负责 packet parse 后把入站 SACK facts 暴露到 `TcpPacket`，并让 `TcpSegment` 持有出站 SACK facts。
- `crates/hammer-service/src/transport/tcp/recovery.rs`
  - 负责 recovery 私有 outstanding packet/accounting、RACK/TLP 和入站 SACK 处理。
- `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - 负责连接内 ACK/SACK/DSACK/接收缺口事实、`snd_una`/`rcv_nxt` 演进、ACK 输出意图。
- `crates/hammer-service/src/transport/tcp/session.rs`
  - 负责把 typed TCP 决策落到 buffer/session runtime：把乱序 payload 放进 session/runtime 私有 RX 持有状态、顺序交付、free duplicate、enqueue ACK 输出。

## 审批项

### 审批 A：复用现有 `TcpSackBlock`，扩展现有 header/segment 签名

**不新增新类型，不新增状态专用 helper；只允许在现有 `write_tcp_segment_header()` / `TcpSegment` 路径上承载可选的出站 SACK/DSACK 事实。**

涉及文件：

- `crates/hammer-core/src/protocol/tcp/segment.rs`
- `crates/hammer-service/src/transport/tcp/segment.rs`

拟修改结果：

```rust
// 只允许复用现有 header/segment 路径承载“这个具体 segment 是否带 SACK/DSACK”。
// 最终是扩 `TcpSegmentHeader`，还是调整 `write_tcp_segment_header()` /
// `TcpSegment::new()` 的参数形状，执行前给用户看最终 diff 再批准。
```

当前代码落点（2026-06-19）：

```rust
pub fn write_tcp_segment_header(output: &mut [u8], header: TcpSegmentHeader) -> CoreResult<usize>;

impl TcpSegment {
    pub fn new(
        local: SocketAddr,
        remote: SocketAddr,
        sequence: u32,
        acknowledgment: u32,
        advertised_window: u16,
        flags: TcpSegmentFlags,
        capabilities: TcpCapabilities,
        payload_len: usize,
    ) -> Self;

    pub fn write_header(&self, output: &mut [u8]) -> CoreResult<usize>;
}
```

理由：

- 现在入站 SACK 已经能 parse，但出站 Established ACK 没法从现有 `TcpSegment` 路径写出 SACK/DSACK。
- 这里直接复用现有 `TcpSackBlock`，不引入新 carrier/type，也不再拆 `*_established_*` 之类的状态专用 helper，仍然让 output 只认 `TcpSegment`。
- “这个具体 segment 是否要带 SACK/DSACK”本来就是可选事实，不应该强迫所有非 SACK 调用点传空 slice。
- 执行时如果需要改 public 签名，只能在上面这条现有路径上收敛；不批准新增新的 `write_ack_*` 生产入口，测试里的 helper 也只能是局部 helper。

### 审批 B：乱序 payload 存储放在 session/runtime 私有 RX 持有状态，TCP 只保存协议事实

涉及文件：

- `crates/hammer-service/src/session/app.rs`
- `crates/hammer-service/src/session/runtime.rs`
- `crates/hammer-service/src/transport/tcp/state_machine.rs`
- `crates/hammer-service/src/transport/tcp/session.rs`

拟约束结果：

```rust
// session/runtime 私有 RX 持有状态保存乱序 payload buffer 和相对 rcv_nxt 的顺序信息；
// TCP 连接只持有：
// - 当前接收缺口对应的 SACK facts（仅在协商了 SACK 时需要）
// - 待回 DSACK fact（仅在协商了 SACK 时需要）
// - rcv_nxt 推进所需的协议状态
```

理由：

- VPP 的 `tcp_session_enqueue_ooo()` 不是把 payload 挂进 TCP 私有 queue，而是按相对 `rcv_nxt` 的 offset 放进 session rx fifo，再由 TCP 更新 SACK list。
- 所以这里计划应该先锁定“payload 在 session/runtime 私有 RX 持有状态，TCP 只保留协议事实”，而不是先暗示“连接里会挂一份乱序 payload 列表”。
- 乱序 payload 存储本身不能依赖 SACK 协商结果；即使 peer 没开 SACK，future payload 也仍然要能暂存并在缺口闭合后顺序交付。
- 但 SACK/DSACK 相关状态可以按协商结果做成 `Option` 或惰性初始化，避免在无 SACK 连接上保留无意义状态。
- 如果实现时发现 session RX storage 现有能力不够，只能补通用的 session-side storage primitive，不能倒回 TCP 私有 payload queue。

### 审批 C：计划阶段不预设新的 sent-record 类型或最终 `record_sent` 签名

涉及文件：

- `crates/hammer-service/src/transport/tcp/recovery.rs`
- `crates/hammer-service/src/transport/tcp/mod.rs`

拟修改结果：

```rust
// 外层调用点不再公开构造 sent record。
// 最终是把 `record_sent()` 改成 primitive 参数，还是改成 recovery 内部更合适的私有入口，
// 只在执行前给用户看最终 diff 后再批准。
```

当前代码落点（2026-06-19）：

```rust
pub struct TcpSentSegment {
    pub packet_number: PacketNumber,
    pub sequence: u32,
    pub end_sequence: u32,
    pub bytes: u32,
    pub sent_at: Instant,
    pub retransmitted: bool,
    pub is_probe: bool,
}

impl TcpRecoveryState {
    pub fn record_sent(&mut self, segment: TcpSentSegment);
    pub fn next_tlp_probe(&mut self) -> Option<TcpSentSegment>;
}
```

理由：

- `TcpSentSegment` 现在只是“为了让外面塞 recovery”而暴露出来的 carrier。
- VPP 对应的是 scoreboard / byte tracker 这样的内部状态结构，不是一个在外层流转的 `SentPacket` carrier。
- 所以这里计划只锁定“外层不再公开构造 sent record”，不预设 recovery 内部最终一定要落成某个 `SentPacket` 风格的新类型名，也不预设最终 `record_sent()` 的公开形状。
- 如果实现时确实需要一个私有业务记录，必须先给出最终定义和理由再审批。
- `crates/hammer-service/tests/tcp_rack_tlp.rs` 当前也直接 import 了 `TcpSentSegment`；Task 2 的第一步就是把这层依赖迁回 `recovery.rs` 自测，再决定外部测试文件是否删除。

---

### Task 1: 打通入站 SACK facts 和出站 SACK header 路径

**Files:**
- Modify: `crates/hammer-core/src/protocol/tcp/options.rs`
- Modify: `crates/hammer-core/src/protocol/tcp/segment.rs`
- Modify: `crates/hammer-core/tests/protocol_tcp_segment.rs`
- Modify: `crates/hammer-service/src/transport/tcp/segment.rs`

**Interfaces:**
- Consumes: `ParsedTcpOptions::sack_blocks`, `TcpSackBlock`, `TcpSegment::write_header`
- Produces:
  - `TcpPacket { sack_blocks: std::vec::Vec<TcpSackBlock>, .. }`
  - 现有 header 写出路径能够在需要时带出 SACK/DSACK option
  - 现有 `TcpSegment` 路径能够承载“这次 ACK 是否带可选 SACK/DSACK facts”

- [ ] **Step 1: 写失败测试，固定当前 ACK 路径不会写 SACK options 的缺口**

```rust
// 下面只固定断言目标；最终到底通过 `TcpSegmentHeader` 承载，
// 还是通过 `write_tcp_segment_header()` / `TcpSegment::new()` 的参数形状承载，
// 以审批 A 批准的 diff 为准。
#[test]
fn core_tcp_write_ack_with_sack_blocks() {
    let mut output = [0u8; 64];
    // 测试里可以包一个局部 helper，复用审批 A 最终批准后的现有 ACK header 写出入口；
    // 这里的 helper 名只是测试占位，不代表新增生产 API。
    let written = write_ack_for_test(
        &mut output,
        &[TcpSackBlock {
            left_edge: 30,
            right_edge: 40,
        }],
    )
    .expect("write ack with sack");
    let parsed = tcp_options_from_bytes(&output[20..written]);
    assert_eq!(
        parsed.sack_blocks,
        vec![TcpSackBlock {
            left_edge: 30,
            right_edge: 40,
        }]
    );
}
```

Run:

```bash
cargo test -p hammer-core protocol_tcp_segment -- --nocapture
```

Expected: FAIL，因为当前 `write_tcp_segment_header()` 对非 SYN segment 不会写 SACK options。

- [ ] **Step 2: 让 `TcpPacket` 带出入站 SACK blocks**

```rust
#[derive(Debug, Clone)]
pub(crate) struct TcpPacket {
    pub(crate) local: SocketAddr,
    pub(crate) remote: SocketAddr,
    pub(crate) sequence: u32,
    pub(crate) acknowledgment: Option<u32>,
    pub(crate) advertised_window: u16,
    pub(crate) flags: TcpSegmentFlags,
    pub(crate) capabilities: TcpCapabilities,
    pub(crate) sack_blocks: std::vec::Vec<TcpSackBlock>,
    pub(crate) payload_offset: usize,
    pub(crate) payload_len: usize,
}
```

并在 `parse_tcp_packet()` 中把 `tcp_options_from_bytes(segment.options())` 的结果整体接住，而不是只取 capability。

- [ ] **Step 3: 复用现有 `TcpSackBlock` 扩展现有 header 写出路径**

```rust
// 继续使用现有 write_tcp_segment_header()：
// - SYN 路径保持现在的 option 写出语义
// - 非 SYN 路径在“这个具体 segment 带有出站 SACK/DSACK 事实”时，直接在原写出逻辑中编码 SACK option
// - 没有这类事实时，不写 SACK option
// - 不新增状态专用 helper，不新增新的 option writer 入口
```

`options.rs` 新增的能力只允许是对现有 TCP option 编码逻辑的补齐，不允许引入新的 view/carrier 类型，也不允许把 ACK/SACK 再分成单独 helper：

```rust
// 只在现有 write_tcp_segment_header() / options writer 内完成
```

- [ ] **Step 4: 让现有 `TcpSegment` 路径承载可选的出站 SACK/DSACK 事实，但不预设内部存储形状**

```rust
// 结果要求：
// - output node 仍然只消费 TcpSegment
// - 没有 SACK/DSACK 时不额外制造 carrier
// - 有 SACK/DSACK 时，TcpSegment 能把这次 segment 的可选事实带到 header prepend 路径
// - 内部最终存储形状执行前如需改动，先给用户看 diff 再批准
```

写入时只在这次 segment 真的带有出站 SACK/DSACK 事实时，才把该事实带到现有 header 写出路径。

- [ ] **Step 5: 验证**

Run:

```bash
cargo test -p hammer-core protocol_tcp_segment -- --nocapture
cargo test -p hammer-service transport::tcp::segment -- --nocapture
```

Expected:

- `hammer-core` 能写出 ACK 上的 SACK options。
- `parse_tcp_packet()` 不再丢弃入站 `sack_blocks`。

### Task 2: 删除公开 `TcpSentSegment`，把 ACK/SACK 处理收回 recovery 私有状态

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/tests/tcp_rack_tlp.rs`（若相关覆盖已完整迁入 `recovery.rs` 的 `#[cfg(test)]`，再删除）

**Interfaces:**
- Consumes: `TcpConnection<Established, C>::commit_payload_tx`, `TcpRecoveryState::on_sack_blocks`, `TcpConnection::apply_ack`
- Produces:
  - recovery sent-record 构造回收至 `recovery.rs` 私有范围
  - `TcpConnection<Established, C>::receive_ack(...)` 在一个 ACK 事件里只做一次 cumulative ACK 清理，并在有入站 SACK 时把 blocks 喂给 recovery

- [ ] **Step 1: 先写失败测试，锁住“不能再依赖公开 sent type”**

把 recovery 行为测试搬进 `recovery.rs` 的 `#[cfg(test)] mod tests`，用私有 helper 填充 outstanding packets：

```rust
fn record_sent_for_test(
    recovery: &mut TcpRecoveryState,
    packet_number: PacketNumber,
    sequence: u32,
    end_sequence: u32,
    bytes: u32,
    sent_at: Instant,
) {
    // 这里调用 recovery 内部最终批准后的私有 sent-record 入口；
    // 断言目标是“外部测试不再依赖公开 TcpSentSegment”，不是写死最终函数签名。
}

#[test]
fn rack_marks_older_unacked_segment_lost_after_later_sack() {
    let now = Instant::now();
    let mut recovery = TcpRecoveryState::new();
    let mut controller = RecordingController::new(1_460);

    record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
    record_sent_for_test(
        &mut recovery,
        2,
        2_000,
        3_000,
        1_000,
        now + Duration::from_millis(1),
    );

    recovery.on_sack_blocks(
        ack(1_000, now + Duration::from_millis(30), 40),
        &[TcpSackBlock {
            left_edge: 2_000,
            right_edge: 3_000,
        }],
        &mut controller,
    );

    recovery.on_rack_timeout(now + Duration::from_millis(56), &mut controller);
    assert_eq!(controller.lost[0].packet_number, 1);
}
```

Run:

```bash
cargo test -p hammer-service transport::tcp::recovery::tests -- --nocapture
```

Expected: FAIL，因为当前 recovery 外部测试入口仍依赖公开 `TcpSentSegment`。

- [ ] **Step 2: 把 sent-record 构造收回 `recovery.rs` 私有范围，但不预设新类型名或最终入口签名**

要求：

```rust
// recovery 内部必须能维护这些事实：
// - packet_number
// - sequence / end_sequence
// - bytes
// - sent_at
// - retransmitted
// - is_probe
//
// 但计划阶段不预设它一定要落成 `SentPacket` 这样的新类型名。
```

`TcpSentSegment` 删除，`mod.rs` 停止 re-export。recovery 内部如何组织这些事实、`record_sent()` 最终长什么样，执行前如需新增私有业务记录或改动对外签名，必须单独审批。

- [ ] **Step 3: 让 `commit_payload_tx()` 不再公开构造 `TcpSentSegment`**

```rust
// commit_payload_tx() 的结果要求：
// - 调用点不再手工构造 `TcpSentSegment`
// - recovery 仍然拿到 packet_number / sequence range / bytes / sent_at / probe/retransmit facts
// - 具体参数形状按审批 C 最终批准的 diff 落地
```

这里顺手把公开类型名从调用点完全删掉。

- [ ] **Step 4: ACK path 直接把入站 SACK blocks 喂给 recovery，但不再沿用当前 `apply_ack()` 的双重清理语义**

```rust
// 结果要求：
// - 当前 `apply_ack()` / `ack_sent_data()` 这一层要拆开，让 ACK 事件只做一次 cumulative ACK 清理
// - `receive_ack()` 在已有 cumulative ACK 推进时，更新 snd_una / snd_wnd / RTT sample / recovery / congestion
// - 如果入站带 SACK blocks，则同一个 ACK 事件里把 blocks 交给 recovery
// - 不能先走一次当前 `apply_ack()` 的 recovery.on_ack()，再额外对同一个 ACK 调 `on_sack_blocks()`
```

要求：

- `receive_ack()` 对每个 ACK 事件只能让 recovery 跑一次 cumulative ACK 清理。
- 有 SACK blocks 时走 `on_sack_blocks()`，没有时走 `on_ack()`，不能先 `apply_ack()` 再额外 `on_sack_blocks()`。
- 连接层需要能区分“累计 ACK 前进了多少字节”和“这次 ACK 是否还带了 SACK facts”，但计划阶段不写死辅助函数名。
- 纯 SACK 不得伪装成 cumulative ACK 推进；只有累计确认前进时才允许后续 TX cleanup 前进。

- [ ] **Step 5: 验证**

Run:

```bash
cargo test -p hammer-service transport::tcp::recovery::tests -- --nocapture
rg -n "TcpSentSegment" crates/hammer-service/src crates/hammer-service/tests
```

Expected:

- recovery 测试通过。
- `TcpSentSegment` 不再出现在公开 API 或外部测试入口。

### Task 3: 把 Established receive、SACK/DSACK 和 app recv 回流合成一个集成任务

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Create tests: `crates/hammer-service/tests/tcp_receive_sack.rs`

**Interfaces:**
- Consumes: `TcpPacket { sequence, payload_offset, payload_len, sack_blocks, flags }`
- Produces:
  - session/runtime 侧的 RX 存储能力，能够在 app 尚未消费时保留 in-order payload，并在需要时额外保留 future payload
  - 连接内私有的 DSACK / SACK 协议事实（仅在协商了 SACK 时维护）
  - 现有 ACK output 路径能读取当前要回给对端的可选 SACK/DSACK 事实
  - app 后续提交 `Recv` SQE 时，session/runtime 能重新尝试交付已持有 payload

- [ ] **Step 1: 先写失败测试，锁住 OOO/SACK/DSACK 行为**

```rust
#[test]
fn out_of_order_payload_is_retained_and_ack_advertises_sack() {
    let (runtime, mut queue, session_id, index, packet) =
        established_packet_with_payload(2_001, b"world");
    let connection = queue.take_connection::<Established>(session_id).expect("connection");

    let ack = connection
        .receive_data(&runtime, index, &mut queue, session_id, &packet)
        .expect("receive")
        .expect("ack");

    let mut bytes = [0u8; 64];
    let header_len = ack.write_header(&mut bytes).expect("write ack");
    let parsed = tcp_options_from_bytes(&bytes[20..header_len]);
    assert_eq!(
        parsed.sack_blocks,
        vec![TcpSackBlock {
            left_edge: 2_001,
            right_edge: 2_006,
        }]
    );
}

#[test]
fn duplicate_payload_sets_dsack_before_regular_sack() {}

#[test]
fn gap_fill_delivers_retained_payload_in_order() {}

#[test]
fn recv_submission_retries_delivery_of_retained_payload() {}
```

Run:

```bash
cargo test -p hammer-service --test tcp_receive_sack -- --nocapture
```

Expected: FAIL，因为当前 `receive_data()` 对非 in-order payload 直接 free，不会放进 session/runtime 私有 RX 持有状态，也不会发 SACK/DSACK。

- [ ] **Step 2: 给 session/runtime 现有 RX 生命周期补齐“保留 future payload 并在缺口闭合后顺序交付”的能力**

```rust
// 结果要求：
// - 不能再出现“没有 pending recv 就直接 free payload buffer”
// - in-order payload 和 future payload 都必须进入 session/runtime 负责的 RX 持有路径
// - future payload 需要带上相对当前 rcv_nxt 的顺序信息，便于 gap close 后继续走现有 app recv completion
// - 计划阶段不预设这块最终一定落在 `session/app.rs` 还是 `session/runtime.rs` 的哪一个私有记录里
```

要求：

```rust
// 这是 session/runtime 的 RX 存储能力，不是 TcpConnection 私有 payload queue
// 它不依赖 negotiated SACK；无 SACK 连接也必须能保存 future payload
```

- [ ] **Step 3: 用现有 buffer primitive 处理 overlap / duplicate / future payload，而不是发明 range API**

规则：

```rust
if packet.sequence == self.rcv_nxt {
    // 走现有 in-order 路径
} else if TcpSeq::new(packet.sequence).after(TcpSeq::new(self.rcv_nxt)) {
    // advance 到 payload_offset，truncate 到 payload_len
    // 交给 session/runtime 私有 RX 持有状态，按 packet.sequence - self.rcv_nxt 作为相对顺序保留
    // 如果 negotiated SACK 打开，TCP 再同步更新当前 SACK facts
} else {
    // duplicate 或 overlap
    // 如果 negotiated SACK 打开，计算重复区间并写入待回 DSACK fact
    // 释放重复输入 buffer
}
```

这里不允许引入 `range`、`view`、`BufferHeaderSnapshot` 一类新 API；只用现有 `advance()` / `truncate_chain()` / `free_index()` / chain metadata，以及 session/runtime 侧必要的私有持有记录。

- [ ] **Step 4: 只在协商了 SACK 时维护连接内 SACK/DSACK facts**

连接内只保留现有 `TcpSackBlock` 一类协议事实，不保留乱序 payload 所有权：

```rust
// - DSACK 优先于普通 SACK block
// - 其余 block 来自 session/runtime 私有 RX 持有状态当前可见的 hole/block
// - 已经被 rcv_nxt 覆盖的 block 在 gap close 后删除
// - 这里不预设最终 helper 名或最终内部存储形状
```

要求：

```rust
if !self.negotiated_options().sack {
    // 不维护待回 DSACK / SACK facts
    // ACK output 不写 SACK option
}
```

实现时可以拆成多个私有函数，但不允许新增对外 helper surface。

- [ ] **Step 5: 缺口闭合和新的 `Recv` SQE 都继续走现有 app recv completion 语义，而不是发明 recv queue**

规则：

```rust
// 当 future payload 对应的缺口已经闭合时：
// - 相关 payload 按序转入现有 `enqueue_rx()` / `try_complete_recv_buffer()` 语义
// - 推进 rcv_nxt
// - 从 session/runtime 私有 RX 持有状态里移除已经可交付的部分
// - TCP 收缩/更新当前 SACK facts
//
// 当 `SessionAppRuntime::handle_submission_descriptor()` 收到 `AppOpcode::Recv` 时：
// - 定位对应 session_id
// - 把该 session 标记 ready
// - 让后续 dispatch 重新尝试把 session/runtime 私有 RX 持有状态里的可交付 payload
//   通过现有 complete_recv 语义送给 app
```

排序和持有都在 session/runtime 私有 RX 状态；即使内部先用简单线性结构，也只是 session/runtime 层实现细节，不把它升格成 TCP recv queue/module。

- [ ] **Step 6: 验证**

Run:

```bash
cargo test -p hammer-service --test tcp_receive_sack -- --nocapture
```

Expected:

- 乱序包被放进 session/runtime 私有 RX 持有状态，而不是直接 free。
- gap fill 后能顺序交付。
- 开启 SACK 时，duplicate/overlap 会产生 DSACK，且 DSACK 在 options 中排在普通 SACK 前面。
- 未开启 SACK 时，仍然保留 future payload，但不会维护 SACK/DSACK option 状态。

### Task 4: 最终收口与回归验证

**Files:**
- Modify as needed: all files changed in Tasks 1-3

**Interfaces:**
- Consumes: tasks 1-3 的所有改动
- Produces: 无公开坏面回流，SACK/DSACK/OOO/recovery 行为稳定，且计划文本不再假装 TX 存储重整已经在本任务内完成

- [ ] **Step 1: 搜索并清理坏面**

Run:

```bash
rg -n "TcpSentSegment|for timer in \\[|TcpConnectionTimerKind::all|copy_current_chain|with_current_chain_range|attach_clone|recv queue" \
  crates/hammer-service crates/hammer-core
```

Expected:

- 没有公开 `TcpSentSegment`
- 没有扫描全部 timer kind 的代码
- 没有这次 SACK 实现顺手引入的 range/view/carrier 坏面

- [ ] **Step 2: 跑 focused tests**

Run:

```bash
cargo test -p hammer-core protocol_tcp_segment -- --nocapture
cargo test -p hammer-service transport::tcp::recovery::tests -- --nocapture
cargo test -p hammer-service transport::tcp::session::tests -- --nocapture
cargo test -p hammer-service --test tcp_receive_sack -- --nocapture
```

Expected:

- `hammer-core` 的 TCP option/header 测试通过。
- recovery / session / receive SACK 回归通过。

- [ ] **Step 3: 跑 workspace 级相关回归**

Run:

```bash
cargo test -p hammer-service tcp -- --nocapture
```

Expected:

- 现有 TCP 测试没有因 SACK/DSACK/OOO 改动回退。

## 自检结果

- Module 5 的 SACK/DSACK/OOO 接口面对应 Task 1、Task 3；Task 4 只负责最终收口与回归验证。
- `TcpSentSegment` 清理对应 Task 2。
- 没有把 payload copy、standalone recv queue、TCP-specific runtime helper 混进这份计划。
- 本计划已经收紧为 VPP 方向：乱序 payload 存储在 session/runtime 私有 RX 持有状态，TCP 只保留协议事实；如果实现时确实需要新增类型或 storage API，必须先拿最终定义和理由单独审批。

# TCP Session TX Recovery Timestamps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 TCP 发送主路径补到“session 持有 TX 字节、recovery 持有私有 sent sample、timestamps/PAWS 真正参与协议语义”这一版可用基线。

**Architecture:** app/session 仍然是唯一允许复制 payload 的边界，但复制必须在 `SessionAppRuntime::drain_submissions()` 之后立刻落到 session-owned buffer chain；TCP 之后只处理 sequence/ack/recovery/timestamp 事实。recovery 不暴露新的 sent-segment 公共类型，只扩展现有私有 `TcpSentSample`；timestamps 只扩展当前 `TcpPacket`/`TcpSegment` 与 `TcpConnection` 私有状态，不把 TCP 选项抬进 session/runtime。

**Tech Stack:** Rust 2024, `hammer-service::{session,transport::tcp}`, `hammer-runtime::app`, `hammer-adapter::buffer`, `hammer-core::protocol::tcp`, `hammer-infra::{fifo,pool,map,rbtree,vec}`, `third_party/vpp`.

---

## Scope

这份计划只处理三件事：

1. `SessionAppTxProgress` 不再持有 `AppSendData`，而是持有 session-owned TX queue entry。
2. `TcpRecoveryState` 的私有 sent sample 记录真正挂上 session TX payload facts，RACK/TLP 不再依赖空 `payload: None`。
3. timestamps 选项从“只会协商/写 0”补到“会 parse、会回显、会参与 PAWS/RTT 采样跳过”。

这份计划**不**处理 delayed ACK、persist、keepalive、TIME_WAIT；那几项分别落到独立计划，避免把 timer 语义和 TX ownership 一次揉在一起。

## File Structure

**Modify**

- `crates/hammer-service/src/session/app.rs`
  - 把 app send copy 边界前移到 session；`SessionAppTxProgress` 改成持有 session-owned TX queue entry，而不是 `AppSendData`。
- `crates/hammer-service/src/session/runtime.rs`
  - 继续复用现有 session TX queue；让 flush path 直接从 session-owned TX queue 组包，不再从 app ring 临时拷贝。
- `crates/hammer-service/src/transport/tcp/connection.rs`
  - 补 timestamps 私有状态、PAWS 检查、基于 session TX payload facts 的 `commit_payload_tx` / recovery 连接。
- `crates/hammer-service/src/transport/tcp/recovery.rs`
  - 扩展私有 `TcpSentSample` 字段，不新增公开 sent-segment 类型。
- `crates/hammer-service/src/transport/tcp/segment.rs`
  - `TcpPacket` 解析 timestamp；`TcpSegment` 输出 timestamp。
- `crates/hammer-core/src/protocol/tcp/options.rs`
  - 继续复用现有 `TcpTimestampOption`，不新增第二套 timestamp 结构。
- `crates/hammer-core/src/protocol/tcp/segment.rs`
  - `write_tcp_segment_header` 改为接 optional timestamp，而不是固定写零。

**Test**

- `crates/hammer-service/tests/session_runtime.rs`
- `crates/hammer-service/tests/tcp_output.rs`
- `crates/hammer-service/tests/tcp_connection_state.rs`
- `crates/hammer-service/src/transport/tcp/recovery.rs` 内联单测

## 最终数据结构

### 1. Session app TX 进度

不新增新模块，也不新增业务 wrapper，只收紧现有私有结构：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionAppTxProgress {
    head: BufferIndex,
    sent_offset: usize,
    total_len: usize,
}
```

语义：

- `head` 指向 session-owned TX queue entry 对应的数据头。
- `sent_offset` 表示该 app submission 已经被 packetize 进 TCP 的字节数，不是已经被 ACK 的字节数。
- `total_len` 是这一笔 app submission 的总字节数。

### 2. Recovery 私有 sent sample

不新增 `TcpSentSegment` 之类公开类型，只扩展现有私有 `TcpSentSample`：

```rust
#[derive(Clone, Copy, Debug)]
struct TcpSentSample {
    packet_number: PacketNumber,
    sequence: u32,
    end_sequence: u32,
    bytes: u32,
    payload: Option<BufferIndex>,
    payload_offset: u32,
    payload_len: u32,
    sent_at: Instant,
    retransmitted: bool,
    prev: Option<PoolIndex>,
    next: Option<PoolIndex>,
}
```

语义：

- `payload` 是本次发包对应的 session TX 数据引用；纯控制包可为 `None`。
- `payload_offset/payload_len` 是 probe/retransmit 时在 session TX 字节流上的 payload range facts。
- `retransmitted` 只用于 RTT sample 跳过，不暴露到 recovery 模块外。

### 3. Timestamp 私有协议状态

新增一个**只存在于 `connection.rs` 内部**的私有状态：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct TcpTimestampState {
    recent_remote: Option<u32>,
    echo_remote: Option<u32>,
    last_local: u32,
}
```

语义：

- `recent_remote`：最近一次被接受的数据段 `tsval`，用于 PAWS。
- `echo_remote`：下一次 ACK/TLP/RACK/FIN 应回显的 `tsecr`。
- `last_local`：最近一次发出的本地 `tsval`，单调不回退。

## Approval Gate

### 审批 A：`TcpPacket` / `TcpSegment` 接通 optional timestamp

涉及文件：

- `crates/hammer-service/src/transport/tcp/segment.rs`
- `crates/hammer-core/src/protocol/tcp/segment.rs`

拟改结果：

```rust
pub(crate) struct TcpPacket {
    // existing fields...
    pub(crate) timestamp: Option<TcpTimestampOption>,
}

pub struct TcpSegment {
    // existing fields...
    timestamp: Option<TcpTimestampOption>,
}
```

原因：

- 现有代码已经 parse 出 `TcpTimestampOption`，但 TCP 协议层拿不到。
- 不接通这个 optional 字段，就只能继续把 timestamps 写成全零。

### 审批 B：`SessionAppTxProgress` 改成持有 session-owned chain

涉及文件：

- `crates/hammer-service/src/session/app.rs`

拟改结果：

```rust
struct SessionAppTxProgress {
    head: BufferIndex,
    sent_offset: usize,
    total_len: usize,
}
```

原因：

- 现在它持有 `AppSendData`，导致 session/TCP 在发包时还得回 app ring 临时拷贝。
- persist、retransmit、TLP 想做干净，必须先把 app/session copy 边界前移。

### 审批 C：扩展 recovery 私有 `TcpSentSample`

涉及文件：

- `crates/hammer-service/src/transport/tcp/recovery.rs`

拟改结果：

- 给现有私有 `TcpSentSample` 增加 `payload_offset`、`payload_len`、`retransmitted`。
- 不新增任何公开 sent-segment 类型或公开构造函数。

原因：

- 当前 `record_sent(..., payload: None, ...)` 让 RACK/TLP 的 session TX payload 语义是半截的。
- 这次只是在 recovery 私有层补齐 facts，不扩散新 API。

## Task 1: 把 app/session copy 边界前移到 session-owned TX queue

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-service/tests/session_runtime.rs`

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/session_runtime.rs` 增加：

```rust
#[test]
fn session_app_send_is_copied_into_session_owned_tx_chain_before_transport() {
    let (mut driver, ring, session_id) = test_session_driver();
    let send = ring_send(&ring, b"hello world");

    driver.app_mut().push_pending_send(session_id, send);
    driver.poll_app().expect("poll app");

    assert!(driver.app().pending_send_len(session_id).expect("pending len").is_none());
    assert!(driver.has_session_tx(session_id));
}

#[test]
fn session_tx_flush_uses_session_tx_without_recopying_from_app_ring() {
    let (runtime, mut driver, ring, session_id) = test_session_flush_driver();
    let send = ring_send(&ring, b"abcdef");

    driver.app_mut().push_pending_send(session_id, send);
    driver.poll_app().expect("poll app");
    dispatch_session_queue_once(&runtime, &mut driver).expect("dispatch");

    assert!(driver.has_session_tx(session_id));
    assert!(driver.app().pending_send_len(session_id).expect("pending len").is_none());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test session_runtime session_app_send_is_copied_into_session_owned_tx_chain_before_transport -- --exact`

Expected: FAIL，因为当前 `poll_app()` 之后 `pending_send_len()` 仍然依赖 `AppSendData`，并且 TX ownership 还没有彻底收敛到 session queue。

- [ ] **Step 3: 实现最小改造**

在 `crates/hammer-service/src/session/app.rs` 改成以下思路：

```rust
impl SessionAppRuntime {
    pub(crate) fn push_pending_send(
        &mut self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        send: AppSendData,
    ) -> CoreResult<()> {
        let total_len = send.len()?;
        let head = copy_send_into_session_chain(buffers, &send)?;
        send.release();
        let progress = SessionAppTxProgress {
            head,
            sent_offset: 0,
            total_len,
        };
        // existing queue insertion
        Ok(())
    }
}

fn copy_send_into_session_chain(
    buffers: &DataPlaneBuffers,
    send: &AppSendData,
) -> CoreResult<BufferIndex> {
    let mut remaining = send.len()?;
    let mut copied = 0usize;
    let head = buffers.alloc_index()?;
    let mut current = head;
    while remaining != 0 {
        let writable = buffers.get_buffer_mut(current)?.writable_tail_mut().len();
        let chunk = writable.min(remaining);
        let buffer = &mut buffers.get_buffer_mut(current)?;
        let written = send.copy_to(copied, &mut buffer.writable_tail_mut()[..chunk]).map_err(CoreError::from)?;
        buffer.commit_writable_tail(written)?;
        copied += written;
        remaining -= written;
        if remaining != 0 {
            let next = buffers.alloc_index()?;
            buffers.append_existing_chain(current, next)?;
            current = next;
        }
    }
    Ok(head)
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service --test session_runtime session_tx_flush_uses_session_tx_without_recopying_from_app_ring -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/app.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "session(Refactor): move app send ownership into session tx chains"
```

## Task 2: 让 recovery sample 真正挂上 session TX payload facts

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/src/transport/tcp/recovery.rs`

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/src/transport/tcp/recovery.rs` 现有单测模块里增加：

```rust
#[test]
fn recovery_tlp_probe_keeps_original_session_tx_payload_pointer() {
    let mut recovery = TcpRecoveryState::new();
    let payload = BufferIndex::new(7).expect("payload");
    let now = Instant::now();

    recovery.record_sent(1, 1_000, 2_000, 1_000, Some(payload), 0, 1_000, now);

    let (_, _, _, retained, offset, len) = recovery.take_tlp_probe().expect("probe");
    assert_eq!(retained, Some(payload));
    assert_eq!(offset, 0);
    assert_eq!(len, 1_000);
}

#[test]
fn recovery_ack_skips_rtt_sample_after_retransmitted_sample() {
    let mut recovery = TcpRecoveryState::new();
    let now = Instant::now();

    recovery.record_sent(1, 1_000, 2_000, 1_000, None, 0, 1_000, now);
    recovery.mark_retransmitted(1_000);

    let mut controller = test_controller();
    recovery.on_ack(test_ack(2_000, now + Duration::from_millis(5)), &mut controller);

    assert!(controller.rtt_samples().is_empty());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service recovery_tlp_probe_keeps_original_session_tx_payload_pointer -- --exact`

Expected: FAIL，因为当前 `record_sent` / `take_tlp_probe` 还没有 offset/len/retransmitted 语义。

- [ ] **Step 3: 实现最小改造**

在 `crates/hammer-service/src/transport/tcp/recovery.rs` 收紧接口：

```rust
pub fn record_sent(
    &mut self,
    packet_number: PacketNumber,
    sequence: u32,
    end_sequence: u32,
    bytes: u32,
    payload: Option<BufferIndex>,
    payload_offset: u32,
    payload_len: u32,
    sent_at: Instant,
)

pub fn mark_retransmitted(&mut self, sequence: u32)

pub fn take_tlp_probe(&mut self) -> Option<(PacketNumber, u32, u32, Option<BufferIndex>, u32, u32)>
```

并在 `connection.rs::commit_payload_tx()` 里，把 session TX 对应的数据引用传给 recovery：

```rust
self.recovery.record_sent(
    packet_number,
    sequence,
    end_sequence,
    payload_len,
    Some(payload),
    0,
    payload_len,
    now,
);
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p hammer-service recovery_ack_skips_rtt_sample_after_retransmitted_sample -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/recovery.rs crates/hammer-service/src/transport/tcp/connection.rs
git commit -m "tcp(Fix): retain payload facts inside recovery samples"
```

## Task 3: 接通 timestamps 输出、回显、PAWS

**Files:**
- Modify: `crates/hammer-core/src/protocol/tcp/segment.rs`
- Modify: `crates/hammer-service/src/transport/tcp/segment.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/tests/tcp_output.rs`
- Test: `crates/hammer-service/tests/tcp_connection_state.rs`

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_output.rs` 增加：

```rust
#[test]
fn tcp_output_writes_nonzero_timestamp_when_negotiated() {
    let segment = timestamp_segment_for_test();
    let mut header = [0u8; 64];

    let written = segment.write_header(&mut header).expect("write header");
    let slice = etherparse::TcpSlice::from_slice(&header[..written]).expect("parse tcp");
    let options = hammer_core::protocol::tcp::tcp_options_from_bytes(slice.options());

    assert!(options.timestamp.is_some());
    assert_ne!(options.timestamp.expect("timestamp").tsval, 0);
}
```

在 `crates/hammer-service/tests/tcp_connection_state.rs` 增加：

```rust
#[test]
fn tcp_timestamp_paws_drops_old_data_segment() {
    let mut connection = timestamp_established_connection();
    let packet = stale_timestamp_packet_for_test();

    let accepted = connection.accept_payload(&packet);

    assert!(accepted.is_none());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-service --test tcp_output tcp_output_writes_nonzero_timestamp_when_negotiated -- --exact`

Expected: FAIL，因为当前 timestamp option 写出的 `tsval/tsecr` 都是 0。

- [ ] **Step 3: 实现最小改造**

在 `crates/hammer-service/src/transport/tcp/segment.rs` 增加 timestamp 贯通：

```rust
pub(crate) struct TcpPacket {
    // existing fields...
    pub(crate) timestamp: Option<TcpTimestampOption>,
}

pub struct TcpSegment {
    // existing fields...
    timestamp: Option<TcpTimestampOption>,
}
```

在 `crates/hammer-core/src/protocol/tcp/segment.rs` 改 `write_tcp_segment_header()`：

```rust
pub struct TcpSegmentHeader<'a> {
    // existing fields...
    pub timestamp: Option<TcpTimestampOption>,
}

if header.capabilities.timestamps {
    let ts = header.timestamp.unwrap_or(TcpTimestampOption { tsval: 0, tsecr: 0 });
    options.extend(ts.tsval.to_be_bytes());
    options.extend(ts.tsecr.to_be_bytes());
}
```

在 `crates/hammer-service/src/transport/tcp/connection.rs` 增加私有状态与逻辑：

```rust
fn next_local_timestamp(&mut self) -> TcpTimestampOption {
    self.timestamps.last_local = self.timestamps.last_local.wrapping_add(1);
    TcpTimestampOption {
        tsval: self.timestamps.last_local,
        tsecr: self.timestamps.echo_remote.unwrap_or(0),
    }
}

fn observe_inbound_timestamp(&mut self, packet: &TcpPacket) -> bool {
    let Some(timestamp) = packet.timestamp else {
        return true;
    };
    if let Some(recent) = self.timestamps.recent_remote
        && timestamp.tsval.wrapping_sub(recent) > (1 << 31)
    {
        return false;
    }
    self.timestamps.recent_remote = Some(timestamp.tsval);
    self.timestamps.echo_remote = Some(timestamp.tsval);
    true
}
```

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test tcp_output tcp_output_writes_nonzero_timestamp_when_negotiated -- --exact
cargo test -p hammer-service --test tcp_connection_state tcp_timestamp_paws_drops_old_data_segment -- --exact
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-core/src/protocol/tcp/segment.rs crates/hammer-service/src/transport/tcp/segment.rs crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/tests/tcp_output.rs crates/hammer-service/tests/tcp_connection_state.rs
git commit -m "tcp(Feat): wire timestamps into tx rx and paws"
```

## Self-Review

- **Spec coverage:** 覆盖了 session-owned TX、recovery session TX payload facts、timestamps/PAWS 三个目标，没有把 delayed ACK/persist/keepalive/TIME_WAIT 混进来。
- **Placeholder scan:** 本文没有 `TODO`/`TBD`/“后续实现”式占位。
- **Type consistency:** 全文只新增 `TcpTimestampState` 一个连接私有类型；`TcpSentSample` 只扩字段，不引入新的 sent-segment 公共类型。

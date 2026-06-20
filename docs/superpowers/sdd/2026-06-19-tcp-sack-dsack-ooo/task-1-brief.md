# Task 1 Brief

- Source plan: `docs/superpowers/plans/2026-06-19-tcp-sack-dsack-ooo-plan.md`
- Branch: `codex/hammer-app-ring-zero-copy`
- Base commit before Task 1: `9663be4be857095bec0c2d4979ee9939906024c3`

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

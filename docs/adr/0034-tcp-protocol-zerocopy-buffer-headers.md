# ADR-0034: TCP protocol zerocopy Buffer header and options access

Status: accepted

Date: 2026-09-21

本文记录TCP protocol层base header、options和checksum codec的实施结果。RX直接借用最终Data-Plane
Buffer中的header/options，TX直接写最终Buffer中的header/options，不产生owned packet header、
临时encoded header或copy-back。

本文不设计Session/FIFO交互、connection state transition、listen、ACK/recovery、congestion control、
retransmit payload ownership或worker scheduling。protocol解析出的短生命周期facts是否以及如何进入
connection state，由后续transport设计决定。

## 1. 范围与非目标

### 1.1 本文决定

1. 删除名称和ownership均不合要求的`TcpWireHeader`，由zerocopy `TcpHeader`直接映射Buffer bytes；
2. `tcp_header`在bounds/header-length validation后返回`&TcpHeader`，不返回owned header；
3. `TcpSegmentHeader<'a>`继续作为scalar TX intent，但只向最终Buffer中的`&mut TcpHeader`和紧随其后的
   options range写入；
4. 删除`ParsedTcpOptions`及其SACK `Vec`、owned Fast Open cookie；options由借用input slice的
   无分配iterator逐项产生；
5. checksum遍历Buffer chain，checksum field通过`TcpHeader` setter清零和写回；
6. reset packet的TCP base header复用同一个codec，不保留独立raw byte writer；
7. `NetworkOpaque`和`TcpSecondaryOpaque`只能通过`hammer_core::buffer_opaque!`取得，layout保持不变。

### 1.2 非目标

- 不修改`TcpPacket`、`TcpConnection`、listener、lookup、recovery、timer或congestion-control语义；
- 不决定SACK、timestamp、MSS、window scale或Fast Open fact如何持久化到connection state；
- 不修改Session/FIFO、TX byte retention、payload packetization或app/session copy boundary；
- 不新增TCP-specific Buffer/runtime API、headroom policy、chain owner或payload selector；
- 不允许跨segment base header/options borrow，也不linearize；
- 不改变IP checksum validation、FIB/DPO、node scheduling或worker ownership；
- 本次实施不新增或修改测试；按用户要求不以编译、测试或CI为交付门槛。

### 1.3 “不 copy”的定义

禁止packet/header storage copy：`read_unaligned() -> TcpHeader`、header clone、stack header、temporary
encoded array、`Vec` options、cookie byte copy、options copy-back和checksum byte-slice copy-back。

端口、sequence、acknowledgment、flags、window、option numeric value、checksum accumulator、cursor和
next/error等短生命周期scalar不是packet copy。`TcpSegmentHeader<'a>`是已有TX protocol intent，保存
scalar与对owner state的borrow，不保存packet bytes，因此可保留。

## 2. Hammer基线

| ID | Path与symbol | 当前事实 | 迁移结论 |
| --- | --- | --- | --- |
| H1 | `tcp/src/protocol/segment.rs`, `TcpWireHeader` | packed `Clone, Copy` owned header；名称违反仓库规则 | 替换为non-Copy zerocopy `TcpHeader` |
| H2 | `segment.rs`, `tcp_header`/`read_tcp_wire_header` | `read_unaligned`复制base header并返回value | bounds-first返回`&TcpHeader` |
| H3 | `segment.rs`, `tcp_wire_header_mut` | raw pointer cast返回mutable header | 删除；`mut_from_prefix`借用最终range |
| H4 | `protocol/options.rs`, `ParsedTcpOptions` | 聚合capabilities、SACK `Vec`、timestamp和owned cookie | 删除aggregate；borrowed option iterator |
| H5 | `protocol/mod.rs`, `TcpFastOpenCookie` | fixed array复制cookie，随后可存入connection state | protocol parser不再构造；持久化属后续transport决策 |
| H6 | `segment.rs`, `TcpSegmentHeader::write` | 向output写base header及options，base header依赖raw cast | 保留TX intent，直接写最终header/options range |
| H7 | `output.rs`, `set_tcp_checksum` | checksum已遍历chain，但用raw byte offset fill/copy-back | 保留chain算法，通过`TcpHeader` setter写 |
| H8 | `reset.rs`, `tcp_reset_write_tcp_header` | 独立byte-indexed TCP encoder | 改用相同`TcpHeader` codec；reset policy不变 |
| H9 | `segment.rs::tcp_packet`, `input.rs`, `output.rs` | direct callers消费owned header/options | 仅列入codec API机械迁移，transport语义不在本ADR |
| H10 | `lib.rs`, `TcpSecondaryOpaque` | route/egress union已由TCP owner定义并通过opaque macro访问 | layout和owner保持 |

## 3. Vendored VPP证据

| ID | Vendored path与symbol | 已核实行为 | 本文约束 |
| --- | --- | --- | --- |
| V1 | `vnet/tcp/tcp_packet.h`, `tcp_header_t` | base header是packet中的packed 20-byte layout | `TcpHeader`直接映射Buffer bytes |
| V2 | `vnet/tcp/tcp_inlines.h`, `tcp_buffer_hdr` | 根据Buffer metadata直接取得TCP header pointer | Hammer使用opaque cursor + checked borrow |
| V3 | `vnet/tcp/tcp_inlines.h`, `vlib_buffer_push_tcp_net_order` | push最终storage后原地写header fields | TX直接mutably borrow最终Buffer |
| V4 | `vnet/tcp/tcp_packet.h`, `tcp_options_parse` | options紧随base header并按kind/length扫描 | Hammer iterator直接扫描同一borrowed options slice |
| V5 | `vnet/tcp/tcp_packet.h`, `tcp_options_write` | options直接写入header后的packet range | Hammer TX直接写最终options range |
| V6 | `vnet/tcp/tcp_output.c`, `tcp_make_ack_i`/`tcp_make_syn`/`tcp_make_synack` | output在Buffer中materialize header/options并写checksum | Hammer保留single final storage |
| V7 | `vnet/tcp/tcp_output.c`, reset/output paths | checksum覆盖TCP segment并直接写header field | Hammer复用chain-aware `InternetChecksum` |

VPP的`tcp_options_parse`会把部分facts写入`tcp_options_t`，SACK甚至可使用vector。Hammer只对齐
packet access、header/options placement和ownership，不复制其可分配的解析结果模型。

## 4. 最终设计

### 4.1 TCP base header

```rust
#[derive(zerocopy::FromBytes, zerocopy::IntoBytes,
         zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
pub struct TcpHeader {
    source_port: [u8; 2],
    destination_port: [u8; 2],
    sequence_number: [u8; 4],
    acknowledgment_number: [u8; 4],
    data_offset_reserved_flags: [u8; 2],
    advertised_window: [u8; 2],
    checksum: [u8; 2],
    urgent_pointer: [u8; 2],
}
```

size必须为20、alignment为1、all-bit-valid；getter接收`&self`，setter接收`&mut self`。不derive
`Copy`/`Clone`，不提供owned constructor、raw pointer、field offset或generic packet header helper。

`tcp_header(packet: &[u8]) -> Result<&TcpHeader, TcpError>`先借用20-byte base header，再验证data
offset至少20、4-byte aligned且不超过传入slice。options range是同一slice的`20..header_len`。
mutable encode不需要public通用helper；owner在已验证final range上直接使用`mut_from_prefix`。

### 4.2 Options decode

`ParsedTcpOptions`删除。protocol owner新增一个具体、无分配的`TcpOptionIter<'a>`状态机；它拥有
`&'a [u8]`和当前index，`Iterator::Item`为`Result<TcpOption<'a>, TcpError>`。这不是packet owner或
借用包装器，而是变量长度protocol grammar的迭代状态。

`TcpOption<'a>`按domain fact建模：

```text
End
NoOperation
MaximumSegmentSize(u16)
WindowScale(u8)
SackPermitted
SackBlock { left_edge: TcpSeq, right_edge: TcpSeq }
Timestamp(TcpTimestampOption)
FastOpenCookie(&'a [u8])
AccurateEcn
Unknown { number: u8, bytes: &'a [u8] }
```

- EOL/NOP只读取kind byte；其余option先验证length byte和剩余range；
- MSS/window/timestamp/SACK固定字段用small zerocopy borrowed layouts或直接scalar decode；
- 一个SACK option由iterator连续产出每个`SackBlock`，不分配`Vec`；最多四个block的现有协议上限保留；
- Fast Open只返回同一Buffer options range中的`&[u8]`，不构造`TcpFastOpenCookie`；
- malformed length返回typed protocol error，不静默产生部分aggregate；
- `tcp_capabilities_from_options`如保留，只能单次迭代并聚合scalar flags，不得间接创建owned options。

caller若要把cookie、SACK或timestamp写入长期connection state，必须在后续transport ADR中决定
ownership、上限和失败原子性；本ADR既不授权该copy，也不改变connection API。

### 4.3 TX header与options encode

`TcpSegmentHeader<'a>`先计算base header + options的最终length并验证20..60 bytes、4-byte alignment、
destination range和所有option lengths。验证完成后：

1. 对最终base-header range取得`&mut TcpHeader`，直接写ports、seq/ack、data offset、flags、window、
   urgent pointer并置checksum为0；
2. 对紧随其后的最终options slice顺序写入kind、length和scalar bytes；padding直接写EOL；
3. 不创建完整header/options stack array，不回写第二份storage；
4. borrow结束后才允许push IP header、改变current window或enqueue。

TX intent中的Fast Open cookie或SACK source已经由其transport owner持有；从该owner slice写入最终
packet是协议要求的materialization，不是codec staging copy。本文禁止的是先复制到另一个临时options
aggregate再复制到Buffer。

### 4.4 Checksum与reset

`set_tcp_checksum`继续使用`InternetChecksum`并遍历`runtime.chain(index)`。不同之处是通过短
`&mut TcpHeader`把checksum置0和写入final value，不通过`TCP_CHECKSUM_OFFSET`、`fill`或
`copy_from_slice`修改raw bytes。IP pseudo-header仍由IP address scalar构成。

RX TCP checksum继续由IP local在进入TCP input前chain-aware验证，TCP protocol不重复扫描payload。

reset generation继续按现有策略创建新packet；这是协议响应materialization，不是中间codec copy。
其TCP 20-byte range必须使用同一个`TcpHeader` encode path，删除`tcp_reset_write_tcp_header`和TCP
raw byte field writer。IPv4/IPv6 header encode、reset decision和connection/listener behavior均不在
本文改变范围。

### 4.5 Opaque与layer isolation

- TCP可调用：`Buffer::{current,current_mut,push_uninit}`、chain iterator、`buffer_opaque!`、IP
  concrete header getters以及generic checksum；
- TCP protocol不得调用：Session/FIFO、connection scheduling、recovery payload selection、Buffer
  linearization、raw opaque bytes或TCP-specific runtime helper；
- RX offset只来自`NetworkOpaque::packet_cursor()`；route/egress facts只来自`TcpSecondaryOpaque`；
- 不改变`TcpSecondaryOpaque` union layout，不加入header/options pointer或offset cache；
- header/options borrow不得跨Buffer window mutation、chain relink、enqueue、handoff或free存活。

## 5. 三向语义差异

| 维度 | 当前Hammer | 本ADR目标 | Vendored VPP | 决策 |
| --- | --- | --- | --- | --- |
| base decode | owned `TcpWireHeader` | `&TcpHeader` | typed Buffer pointer | 对齐direct access并修正命名 |
| base encode | raw pointer cast | final Buffer `&mut TcpHeader` | push后原地写 | 对齐single storage |
| options decode | owned aggregate + SACK `Vec` + cookie copy | borrowed iterator/events | direct scan into options state | 对齐packet access；拒绝VPP allocation |
| options encode | direct slice writes mixed with raw header | direct final header/options borrows | direct packet range writes | 对齐 |
| checksum | chain-aware，raw offset回写 | chain-aware，typed field write | chain-aware typed field | 保留算法，收敛access |
| reset | independent raw TCP writer | shared `TcpHeader` codec | typed packet header | 删除重复codec |
| connection/session | parser结果进入transport state | 不在范围 | TCP owns connection state | 后续ADR决定 |

## 6. 决策记录

| ID | 决策 | Alignment | 理由 |
| --- | --- | --- | --- |
| D1 | `TcpWireHeader`替换为Buffer-backed `TcpHeader` | Aligned + naming correction | V1、H1 |
| D2 | RX base header只返回borrow | Aligned | V2、H2 |
| D3 | TX直接写最终base header和options range | Aligned | V3、V5、H3、H6 |
| D4 | options用borrowed iterator，不使用parsed aggregate/`Vec` | Rust strengthening | V4、H4、H5 |
| D5 | checksum保持chain-aware并改为typed field mutation | Aligned | V7、H7 |
| D6 | reset复用同一TCP codec | Aligned | H8及single-codec invariant |
| D7 | connection、recovery、Session/FIFO行为全部排除 | Scope preservation | 用户边界、H9 |
| D8 | opaque layouts保持且只能经宏访问 | Hammer ownership preservation | H10、ADR-0031 |

## 7. 变更清单与API审批

### Add

- protocol-owned `TcpOptionIter<'a>`和`TcpOption<'a>`；
- 固定option layout仅在确有zerocopy field access需求时作为private protocol types加入。

### Modify

- `protocol/segment.rs`：`TcpHeader` zerocopy layout、borrowed decode、final Buffer encode；
- `protocol/options.rs`：无分配iterator与borrowed option events；
- `protocol/mod.rs`：exports收敛到新protocol API；
- direct codec callers：仅迁移borrow lifetime和scalar读取；
- `output.rs`：typed checksum field access；
- `reset.rs`：复用`TcpHeader` encode。

### Remove

- `TcpWireHeader`、`read_tcp_wire_header`、`tcp_wire_header_mut`、raw casts；
- `ParsedTcpOptions`及protocol parser中的SACK `Vec`和Fast Open cookie copy；
- `tcp_reset_write_tcp_header`和TCP checksum raw offset copy-back。

### Preserve

- `TcpSegmentHeader<'a>` TX intent；
- `TcpPacket`、`TcpFastOpenCookie`及全部connection/listen/recovery state，直至后续transport ADR决定；
- Session/FIFO、payload ownership、Graph Nodes、errors、timers、congestion control；
- `NetworkOpaque`、`TcpSecondaryOpaque`和generic Buffer/runtime APIs。

| API项目 | 状态 | 理由 |
| --- | --- | --- |
| `TcpWireHeader` -> `TcpHeader` | accepted breaking protocol API | 修正ownership和禁用名称；direct callers已机械迁移 |
| `tcp_header`返回borrow | accepted breaking protocol API | owned decode与zero-copy目标冲突 |
| `TcpOptionIter<'a>` / `TcpOption<'a>` | accepted new protocol API | issue #346批准；现有aggregate无法无分配表达SACK/cookie borrow |
| private fixed-option layouts | not added | scalar decode已满足需求 |
| connection/Session API变化 | rejected from scope | 用户明确只设计protocol |
| TCP-specific Buffer/runtime helper | rejected | existing Buffer/zerocopy API足够 |

Open questions: none。connection与Session问题明确留给后续ADR。

issue #346 已批准并实施上述protocol-only API变化。

**Design verdict: Aligned.** 设计对齐VPP的direct Buffer header/options placement，以Rust
zerocopy borrow表达生命周期，同时拒绝options分配模型，并保持Session、connection和recovery边界
完全不变。

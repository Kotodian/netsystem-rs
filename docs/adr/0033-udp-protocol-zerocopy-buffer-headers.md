# ADR-0033: UDP protocol zerocopy Buffer header access

Status: accepted

Date: 2026-09-21

本文记录 UDP protocol header 的 decode、encode 与 checksum access 实施结果。UDP header
始终位于最终 Data-Plane Buffer 中：RX 直接借用该 header，TX 直接写入该 header，不创建
owned header、临时 encoded bytes 或 copy-back。

本文不设计 Session、FIFO、datagram ownership、worker scheduling 或 app/session copy boundary。
因此“zerocopy”不表示整个 UDP plugin 不复制 payload，只表示进入 Data-Plane Buffer 后，UDP
protocol codec 不再复制 packet/header bytes。

## 1. 范围与非目标

### 1.1 本文决定

1. `UdpHeader` 改为 `zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable}` layout；
2. decode 从 `NetworkOpaque::packet_cursor()` 指定的最终 Buffer range 返回 `&UdpHeader`；
3. encode 在 `Buffer::push_uninit(UDP_HEADER_LEN)` 返回的最终 storage 上取得
   `&mut UdpHeader` 并原地写字段；
4. 删除 `UdpHeader::new`、owned `read_udp_header`、unaligned pointer read/write 与 checksum
   byte-slice copy-back；
5. checksum 直接遍历 Buffer chain，checksum 字段通过 borrowed header setter 清零和写回；
6. opaque 只能通过 `hammer_core::buffer_opaque!` 访问；不改变 `UdpEgressOpaque` 或
   `NetworkOpaque` layout。

### 1.2 非目标

- 不改变 UDP connection、port registration、dispatch、unknown-port ICMP policy或node arcs；
- 不改变 Session/FIFO、TX datagram dequeue、payload retention 或 payload 到 Buffer 的搬运；
- 不新增 UDP-specific Buffer/runtime API、header pointer cache或任意offset accessor；
- 不支持跨Buffer segment的UDP base-header borrow，也不linearize packet；
- 不改变IP header codec、FIB/DPO、interface、worker ownership或同步；
- 不设计TCP或ICMP行为；
- 本次实施不新增或修改测试；按用户要求不以编译、测试或CI为交付门槛。

### 1.3 “不 copy”的定义

禁止的是 packet/header storage copy：`read_unaligned() -> UdpHeader`、header clone、stack header、
encoded array、`Vec`、`to_vec`、为写checksum执行的`copy_from_slice`以及临时storage回写。

端口、length、checksum accumulator、IP address value、next/error和cursor等短生命周期scalar不拥有
packet bytes，不属于header copy。`UdpEgressOpaque`中为跨Node保存的endpoint value也是既有路由事实，
不是packet/header codec副本。

## 2. Hammer基线

| ID | Path与symbol | 当前事实 | 迁移结论 |
| --- | --- | --- | --- |
| H1 | `transport/udp/src/protocol.rs`, `UdpHeader` | 原`wire.rs`使用`repr(C, packed)` + `Clone, Copy`，getter按值接收 | 已改为8-byte zerocopy layout，getter借用 |
| H2 | `protocol.rs`, `write_udp_header` | 原实现构造owned header后`write_unaligned`到output slice | 已在最终Buffer header range取得mutable borrow后原地写 |
| H3 | `input.rs`, `read_udp_header` | `read_unaligned`返回owned header | 删除；bounds-first `ref_from_prefix` |
| H4 | `input.rs`, `udp_checksum_is_valid` | 重新扫描当前连续slice并按raw IP offset取地址 | IP local已验证完整chain；UDP只保留family-specific zero-checksum policy |
| H5 | `output.rs`, `udp_output_push_ipv4/ipv6` | checksum只包含first Buffer的`current()`，然后按`[6..8]` copy-back | 用`InternetChecksum`遍历完整chain并通过header setter写回 |
| H6 | `worker.rs`, UDP TX path | 最终Buffer已`push_uninit(8)`后调用codec；其前面存在Session payload copy | 只机械迁移header codec call；Session行为排除 |
| H7 | `output.rs`, `UdpEgressOpaque` | secondary opaque保存egress endpoints并通过opaque macro访问 | layout和访问方式保持 |
| H8 | `transport/udp/Cargo.toml` | 已直接依赖workspace `zerocopy` | 不新增依赖 |

## 3. Vendored VPP证据

| ID | Vendored path与symbol | 已核实行为 | 本文约束 |
| --- | --- | --- | --- |
| V1 | `vnet/udp/udp_packet.h`, `udp_header_t` | UDP base header是packet中的8-byte layout | `UdpHeader`直接映射Buffer bytes |
| V2 | `vnet/udp/udp_inlines.h`, `vlib_buffer_push_udp` | push最终header空间并原地写ports、length、checksum | Hammer TX直接mutably borrow `push_uninit`结果 |
| V3 | `vnet/udp/udp_local.c`, `udp46_local_inline` | input直接取得`udp_header_t *`并读取字段 | Hammer以checked zerocopy borrow表达相同ownership |
| V4 | `vnet/udp/udp.c`, `udp_compute_checksum` | checksum沿Buffer chain计算 | Hammer不能只校验或计算first segment |
| V5 | `vnet/ip/ip4_forward.c`, `ip4_tcp_udp_compute_checksum` | pseudo-header与chain packet bytes共同参与checksum | 复用generic `InternetChecksum`，不linearize |

VPP使用C pointer；Hammer只对齐header位于Buffer及chain-aware checksum的语义，不引入raw pointer
API，也不复制VPP的具体函数边界。

## 4. 最终设计

### 4.1 Header layout

`UdpHeader`保留UDP plugin owner和crate-private visibility：

```rust
#[derive(zerocopy::FromBytes, zerocopy::IntoBytes,
         zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
pub(crate) struct UdpHeader {
    source_port: [u8; 2],
    destination_port: [u8; 2],
    length: [u8; 2],
    checksum: [u8; 2],
}
```

size必须为8、alignment必须为1且all-bit-valid。getter接收`&self`；encode使用窄setter写port、
length、checksum。不得derive `Copy`/`Clone`，不得提供owned constructor、`to_bytes`、generic
header reader或raw pointer accessor。

### 4.2 RX decode与checksum边界

UDP input先通过opaque macro读取并复制`packet_cursor()`等必要scalar，然后结束opaque borrow。它对
`buffer.current()`执行checked slicing，在transport offset调用`UdpHeader::ref_from_prefix`，验证：

- base header连续且完整；
- UDP length至少为8且不超过cursor packet length；
- cursor、header length和payload offset均checked conversion；
- IPv6 checksum不得为0；IPv4 checksum为0仍表示未启用checksum。

IP local已在UDP input之前对非零UDP checksum执行chain-aware validation。UDP input不得再次扫描
payload或要求datagram连续；它只执行上述UDP-owned zero-checksum policy。source/destination地址从
borrowed `Ipv4Header`/`Ipv6Header`取得，不使用raw byte offsets。

所有header borrow结束后，input才通过opaque macro更新cursor的transport header/payload facts。
malformed packet走既有typed node error/next，不修改opaque或Buffer bytes。

### 4.3 TX encode与checksum

TX caller先验证`payload_len + 8`可表示为`u16`，再在最终Buffer执行`push_uninit(8)`。codec对该
返回slice调用`UdpHeader::mut_from_prefix`，直接写ports、length并把checksum置0。不得先构造
`UdpHeader` value再写入。

output checksum使用`hammer_infra::checksum::InternetChecksum`：先写入IPv4/IPv6 pseudo-header
scalar bytes，再依次遍历`runtime.chain(index)`的全部current ranges。计算前和计算后均通过
`&mut UdpHeader`访问checksum字段。计算结果为0时写`0xffff`，避免合法checksum被解释为IPv4的
“checksum disabled”；IPv6同样写`0xffff`。

checksum结束后才push IP header并发布`NetworkOpaque` cursor。任何首次header mutation前可发现的
length/headroom错误必须先返回，避免暴露半编码header。

### 4.4 Opaque与layer isolation

- UDP可调用：`Buffer::{current,current_mut,push_uninit}`、chain iterator、`buffer_opaque!`、IP
  concrete header getters以及generic internet checksum；
- UDP不得调用：Session/FIFO内部操作、payload staging allocator、Buffer linearization、raw opaque
  bytes、raw header pointer或UDP-specific runtime helper；
- `UdpEgressOpaque`只保存既有egress endpoint facts；不得加入header pointer、checksum state或cursor；
- `NetworkOpaque::packet_cursor()`继续是RX transport offset的唯一跨Node来源；
- header borrow不得跨advance/push/trim、chain relink、enqueue、handoff或free存活。

## 5. 三向语义差异

| 维度 | 当前Hammer | 本ADR目标 | Vendored VPP | 决策 |
| --- | --- | --- | --- | --- |
| RX header | owned unaligned read | `&UdpHeader` | typed Buffer pointer | 对齐direct Buffer access |
| TX header | owned construct + unaligned write | final Buffer `&mut UdpHeader` | push后原地写 | 对齐single storage |
| RX checksum | UDP再次扫描连续datagram | IP local验证chain；UDP验证zero policy | local/IP checksum owner协作 | 对齐owner并避免二次扫描 |
| TX checksum | first segment only | 遍历完整Buffer chain | chain-aware | 修复语义差异 |
| checksum write | byte-slice copy-back | header setter | typed field write | 对齐 |
| opaque | existing endpoint overlay | layout保持，宏访问 | buffer opaque metadata | 保持Hammer owner |
| Session payload | copy到Buffer | 不在范围 | session-owned path | 不作结论 |

## 6. 决策记录

| ID | 决策 | Alignment | 理由 |
| --- | --- | --- | --- |
| D1 | `UdpHeader`是Buffer中的crate-private zerocopy layout | Aligned | V1、H1 |
| D2 | RX只返回borrow，不产生owned header | Aligned | V3、H3 |
| D3 | TX直接写`push_uninit`返回的最终storage | Aligned | V2、H2 |
| D4 | RX非零checksum由IP local验证，UDP只执行zero policy | Aligned ownership | H4及IP/UDP node boundary |
| D5 | TX checksum遍历完整chain且0编码为`0xffff` | Aligned/protocol correction | V4、V5、H5 |
| D6 | opaque layouts与Session/FIFO保持不变 | Scope preservation | H6、H7 |

## 7. 变更清单与API审批

### Modify

- `udp/src/protocol.rs`：zerocopy `UdpHeader`、borrowed getters与窄setters；原`wire`模块删除；
- `udp/src/input.rs`：删除owned reader和重复checksum扫描，使用cursor + header borrows；
- `udp/src/output.rs`：chain-aware checksum并通过header setter写字段；
- `udp/src/worker.rs`：仅机械迁移最终header encode调用。

### Remove

- `UdpHeader::new`、owned `read_udp_header`、`read_unaligned`/`write_unaligned`；
- UDP header/checksum path中的raw byte offset和copy-back。

### Preserve

- Session/FIFO与datagram APIs、connection/worker state、dispatch、errors、node arcs；
- `UdpEgressOpaque`、`NetworkOpaque`和Buffer APIs；
- IP local checksum owner与IP output header behavior。

| API项目 | 状态 | 理由 |
| --- | --- | --- |
| 修改existing private `UdpHeader` layout/methods | accepted modification | owner不变，不增加外部API |
| 删除private owned codec helpers | accepted removal | 所有caller迁移到direct borrow |
| 新UDP opaque或Buffer/runtime helper | rejected | existing cursor/Buffer API足够 |
| Session/FIFO API变化 | rejected from scope | 用户明确只设计protocol |

Open questions: none。

issue #346 已批准并实施上述protocol-only API与模块迁移。

**Design verdict: Aligned.** 设计对齐VPP的direct Buffer header access和chain-aware checksum，且
明确不扩展到Session、FIFO或payload ownership。

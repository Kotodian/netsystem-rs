# ADR-0032: ICMP protocol zerocopy Buffer header access

Status: accepted

Date: 2026-09-21

目标是把 `hammer-plugins/net/icmp` 的 protocol decode/encode 全部收敛到
`zerocopy` 与 Data-Plane Buffer：input 直接借用 Buffer 中的 ICMP/IP header，echo reply
直接原地修改同一个 Buffer。不得复制 packet/header 到 owned value、临时 byte array、`Vec` 或
第二段 storage，也不得重新引入 ADR-0031 已删除的 `ParsedIpPacket`、raw header helper 或兼容层。

本文细化 ADR-0005、ADR-0006、ADR-0007 和 ADR-0031。ICMP plugin 继续拥有本地 ICMP type
dispatch 与 echo reply；IP plugin 继续拥有 IP header layout、IP local checksum validation 与
ICMP error generation。

## 1. 决策范围

### 1.1 本文决定

1. `IcmpHeader` 改成 `zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable}` layout；decode
   只产生 `&IcmpHeader`，encode 只产生指向最终 Buffer storage 的 `&mut IcmpHeader`；
2. `icmp4-input`、`icmp6-input` 通过 `NetworkOpaque::packet_cursor()` 定位 header，并直接借用
   family-specific IP header 与 ICMP header；
3. `build_echo_reply` 接收 `&mut hammer_core::buffer::Buffer`，不再接收 packet slice 与
   `ParsedIpPacket`；
4. echo reply 在收到的 Buffer 上原地修改 ICMP type/checksum、IP addresses、TTL/Hop Limit 和
   IPv4 identification/checksum，保留 payload、Buffer chain、BufferIndex 与 current window；
5. opaque 只能通过 `hammer_core::buffer_opaque!` 取得；不增加 ICMP opaque、header pointer、
   byte-offset accessor 或 parser carrier；
6. ICMP plugin 直接依赖 `zerocopy`，不经 IP plugin 转发该依赖；
7. 删除 ICMP plugin 对 `ParsedIpPacket`、`ip_header` 与 `protocol::wire::read_header` 的全部依赖，
   不保留 alias、deprecated wrapper 或兼容路径。

### 1.2 非目标

- 不改变 ICMP type registration、dispatch table、Graph Node、next arc、trace 或 node-error taxonomy；
- 不改变 IP local 对完整 ICMP checksum 的 chain-aware validation；ICMP input 继续信任该结果；
- 不改变 IP-owned `ip4-icmp-error` / `ip6-icmp-error`。错误响应按协议复制 offending packet quote，
  不是 ICMP plugin echo codec 的 staging copy；
- 不改变 ICMP echo identifier、sequence 或 payload，它们保留在原 Buffer bytes 中；
- 不增加跨 segment header borrow、linearization、scratch Buffer 或 Buffer-chain copy；
- 不把 IP local 的 private ICMP minimum-header layout移到ICMP plugin。依赖方向仍是
  `icmp -> ip`，IP plugin不得反向依赖ICMP；
- 不修改 TCP、UDP、PMTU policy、FIB/DPO、interface 或 worker synchronization；

### 1.3 “不 copy”的精确定义

本设计禁止 packet/header storage copy：禁止 `read_unaligned` 产出 owned header、header clone、
`copy_from_slice`/`rotate_left` 处理 protocol header、stack header array、`Vec`、`to_vec`、临时
encoded bytes 和 copy-back。

以下不是 packet copy：读取 type/code/checksum/length/address 为短生命周期 scalar、复制
`BufferPacketCursor`、复制 ICMP dispatch table 的固定 scalar entry、checksum accumulator 运算、
trace scalar，以及在同一 borrowed header 内交换 source/destination 字段。它们不拥有 packet
bytes，也不跨 Node 保存。这个边界与 ADR-0031 的 scalar decode 规则一致。

## 2. 当前 Hammer 基线

| ID | Path 与 symbol | 已核实事实 | 迁移结论 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-plugins/net/icmp/src/protocol.rs:5-28`, `IcmpHeader` | `repr(C, packed)` + `Clone, Copy`，getter按值接收 | 改成zerocopy layout，getter借用，增加owner-local mutable setters |
| H2 | `protocol.rs:39-96`, `build_echo_reply` | 接收`&mut [u8]`和owned `ParsedIpPacket`；`read_header`复制ICMP header；byte indexing、`rotate_left`和`copy_from_slice`改写headers | 改为直接接收`&mut Buffer`并借用concrete headers |
| H3 | `icmp.rs:520-605`, `next_slot_for_index` | 重新调用`ip_header`构造parsed aggregate，再用`read_header`读取owned ICMP header | 使用opaque cursor/facts + short zerocopy borrows |
| H4 | `icmp.rs:609-705`, `next_for_echo_request_index` | parsed aggregate跨immutable/mutable Buffer borrow保留，并在reply后重建cursor | validation scalars局部保存；原cursor继续有效，只更新echo transport length/payload offset |
| H5 | `hammer-service/src/opaque.rs:83-186,340-400`, `NetworkIpOpaque`, `NetworkOpaque` | 已保存packet length、IP version/protocol与L3/L4 offsets，并能重建`BufferPacketCursor` | 复用existing primary opaque，不增加ICMP metadata |
| H6 | `hammer-plugins/net/ip/src/protocol/ip.rs:178-324`, `Ipv4Header`, `Ipv6Header` | 已是zerocopy layout，具备read getters和部分mutable setters，但没有地址交换及IPv4 identification setter | IP owner增加最小窄方法，ICMP不绕过private fields |
| H7 | `hammer-plugins/net/ip/src/local.rs:21-31,828-837` | IP local已有private zerocopy ICMP minimum-header layout并负责完整chain checksum | 保持private；不制造跨DSO shared ICMP layout或反向依赖 |
| H8 | `hammer-plugins/net/icmp/Cargo.toml` | ICMP crate尚未直接依赖`zerocopy` | 增加workspace dependency |
| H9 | scoped search `rg "ParsedIpPacket|pub fn ip_header|mod wire|protocol::wire" crates/hammer-plugins/net/{ip,icmp}/src` | ADR-0031已删除IP生产定义；只剩ICMP旧引用及IP test-only旧引用 | ICMP不能恢复deleted API，必须直接迁移 |

`IcmpHeader`、`IcmpBuildError` 与 `build_echo_reply` 仅在ICMP crate内部使用。本文不新增external
plugin ABI、runtime registry entry或service capability。

## 3. Vendored VPP 证据

| ID | Vendored path 与 symbol | 已核实行为 | 本文约束 |
| --- | --- | --- | --- |
| V1 | `third_party/vpp/src/vnet/ip/icmp46_packet.h:158-164`, `icmp46_header_t` | ICMP base header是packet bytes中的4-byte packed layout | Rust `IcmpHeader`保持4 bytes并直接借用Buffer |
| V2 | `vnet/ip/icmp4.c:112-173`, `ip4_icmp_input` | 从`vlib_buffer_get_current`取得IP header，`ip4_next_header`直接定位ICMP header并按type dispatch | ICMPv4 input不构造parsed/owned header |
| V3 | `vnet/ip/icmp6.c:125-204`, `ip6_icmp_input` | 直接读取IP/ICMP headers，验证type/code/hop-limit/payload length；checksum已由IP local验证 | Hammer保持同一owner与validation boundary |
| V4 | `plugins/ping/ping.c:295-470`, `ip4_icmp_echo_request` | 在收到的Buffer上原地改type/checksum、交换addresses、更新TTL/fragment id/IP checksum并标记locally-originated | IPv4 echo不分配或复制packet/header |
| V5 | `plugins/ping/ping.c:509-678`, `ip6_icmp_echo_request` | 在收到的Buffer上原地改type/checksum、交换addresses、更新Hop Limit与link-local FIB选择 | IPv6 echo沿用同一Buffer和chain |
| V6 | `vnet/ip/ip_packet.h:155-172`, `ip_csum_update_inline` | field change通过scalar incremental checksum更新 | ICMP checksum继续按type scalar增量更新，不扫描或复制payload |
| V7 | `vnet/ip/icmp4.c:216-375`; `icmp6.c:257-409` | ICMP error generation另行分配Buffer并复制受限quote | 明确排除IP-owned error generation，不能把它误称为echo codec copy |
| V8 | `third_party/vpp/test/test_ip4.py:546-553`; `test_ip6.py:3561-3674` | upstream覆盖IPv4/IPv6 echo行为、global/link-local路径与不应回复场景 | Hammer test matrix保留同等可观察行为 |

VPP使用C typed pointers；Hammer只对齐direct Buffer ownership与in-place mutation语义，不引入
raw pointer API。Rust在bounds validation后使用`zerocopy` borrow表达同一生命周期。

## 4. 最终 header 与 Buffer 设计

### 4.1 ICMP header layout

`protocol.rs`保留一个ICMP-owned base header：

```rust
#[derive(zerocopy::FromBytes, zerocopy::IntoBytes,
         zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
pub(crate) struct IcmpHeader {
    icmp_type: u8,
    code: u8,
    checksum: [u8; 2],
}
```

layout必须保持size 4、alignment 1、all-bit-valid。getter接收`&self`；type/checksum mutation由
`&mut self`的窄方法完成。不得derive `Copy`/`Clone`，不得提供`to_bytes`、owned decode或generic
header reader。

不新增`IcmpEchoHeader`。echo identifier/sequence并未被节点读取或修改；caller只验证从ICMP offset
开始至少8 bytes连续，然后把后4 bytes和payload留在原Buffer。为未使用字段增加borrow wrapper
既不减少copy，也不表达新的domain owner。

### 4.2 Decode: ICMP input

每个input Node按以下顺序处理：

1. `buffer_opaque!(buffer => NetworkOpaque)`取得primary overlay，复制cursor、IP version/protocol等
   scalar facts，然后结束opaque borrow；
2. 用cursor对`buffer.current()`做checked slicing；不使用raw pointer或unchecked byte offset；
3. IPv4直接`Ipv4Header::ref_from_prefix`，IPv6直接`Ipv6Header::ref_from_prefix`；IPv6 extension
   后的最终protocol仍以`NetworkIpOpaque.ip_protocol()`为准；
4. 在transport offset执行`IcmpHeader::ref_from_prefix`，读取type/code；
5. 用cursor packet length计算ICMP message length；IPv6 hop-limit从`&Ipv6Header`读取；
6. header borrows结束后才设置node error、trace或enqueue next。

不得构造`ParsedIpPacket`、`ParsedIcmpPacket`、header enum/newtype、cached pointer或offset wrapper。
Graph Node已知道family，直接选择concrete header type。

### 4.3 Encode: echo reply

`build_echo_reply`的目标接口为：

```rust
pub(crate) fn build_echo_reply(
    buffer: &mut hammer_core::buffer::Buffer,
    version: IpVersion,
    fragment_id: u16,
) -> Result<(), IcmpBuildError>;
```

该函数直接从Buffer opaque取得cursor与protocol facts，再从`buffer.current_mut()`取得最终header
storage。它不接收packet slice、parsed aggregate、header owner或第二个destination。

mutation必须failure-atomic：在首次写入前完成packet/chain length、family、protocol、IP header、
ICMP base header、8-byte echo minimum length和request type的全部验证。验证完成后Buffer window不再
改变，因此后续`mut_from_prefix`失败是owner invariant bug，不再返回可恢复的partial-write error。

原地写入顺序：

1. 由borrowed `IcmpHeader`的旧checksum和request/reply type计算incremental checksum scalar；
2. 短`&mut IcmpHeader`直接写reply type与checksum，code/identifier/sequence/payload不变；
3. IPv4短`&mut Ipv4Header`交换addresses、设置TTL和identification、清零checksum；borrow结束后
   对同一Buffer中的完整IPv4 header range计算checksum，再次短borrow写checksum；
4. IPv6短`&mut Ipv6Header`交换addresses并设置Hop Limit；地址交换不改变ICMPv6 pseudo-header sum；
5. header borrows结束后，Node通过opaque macro更新locally-originated/checksum flags、必要的
   interface/FIB facts和现有cursor的ICMP echo header/payload offsets。

VPP对IPv4 header也使用incremental checksum；Hammer保留对同一Buffer header range做完整
`internet_checksum`的现有语义。它不分配、不复制header/payload，并能覆盖IPv4 options。这是有意
representation差异，结果与VPP等价。

### 4.4 IP owner的窄方法

ICMP不能访问IP header private fields，也不能重建raw byte mutation。IP owner应在existing types上
增加：

```text
Ipv4Header::set_identification(u16)
Ipv4Header::swap_addresses()
Ipv6Header::swap_addresses()
```

`swap_addresses`只在borrowed header内使用`core::mem::swap`交换fixed address fields，不返回owned
header或packet bytes。不得增加generic `set_bytes`、field-offset API、`as_mut_ptr`或ICMP-specific
IP helper。

## 5. Opaque、chain 与layer isolation

### 5.1 Opaque contract

- ICMP只通过`buffer_opaque!`访问`NetworkOpaque`与`IpSecondaryOpaque`；
- `NetworkOpaque::ip()`仍是version/protocol/packet length/cursor/FIB facts的唯一跨Node owner；
- echo reply不切换primary union member，不增加`IcmpOpaque`；
- `IpSecondaryOpaque`只用于清除既有ICMP error metadata，layout不变；
- header borrow存活期间不得borrow同一Buffer opaque；先复制必要scalar，再结束borrow。

### 5.2 Buffer-chain contract

IP local在进入ICMP Node前已对完整chain执行ICMP checksum validation。ICMP input/echo要求L3 header
与8-byte echo prefix位于first Buffer current window；split header返回existing `BadLength`，不得
linearize。tail Buffers、chain links、total-chain length、current offsets和refcounts保持不变。

echo reply不分配Buffer、不调用copy/clone/append API、不遍历或重写payload。Node继续转发原
`BufferIndex`。expected packet failure选择existing drop/punt next，由terminal Drop Node结束ownership。

### 5.3 Layer isolation

- ICMP protocol layer可以调用：`Buffer::{current,current_mut}`、`buffer_opaque!`、IP-owned concrete
  header methods、checksum scalar functions；
- 不得调用：Buffer allocation/copy/chain append、raw pointer construction、IP FIB mutation、ICMP
  error generation、runtime-wide packet helper；
- IP plugin继续负责IP local validation与error generation；ICMP plugin只依赖IP公开concrete header
  API和protocol registration；
- service/runtime/core不出现ICMP-specific type、trait、callback或capability。

## 6. 三向语义差异

| 维度 | 当前 Hammer | 本 ADR 目标 | Vendored VPP | 决策 |
| --- | --- | --- | --- | --- |
| ICMP decode | owned `read_header` + parsed IP aggregate | direct `&IcmpHeader` + concrete IP borrow | direct typed pointers | 对齐direct Buffer access |
| Echo encode | byte indexing、rotate/copy与copy-back混用 | final Buffer `&mut Header`原地写 | in-place typed pointer mutation | 对齐single storage |
| Function boundary | `&mut [u8]` + `&ParsedIpPacket` | `&mut Buffer` + family/id scalars | node owns `vlib_buffer_t *` | direct Buffer ownership |
| Cross-node facts | parsed aggregate重新构造 | existing primary opaque cursor/facts | vnet buffer opaque + packet header | 删除双重解析 |
| Payload/chain | payload未改，chain保留 | 明确保持且不遍历payload | echo原地保留packet | 对齐 |
| IPv4 checksum | full recompute | full recompute over borrowed Buffer range | incremental update | 有意差异，无copy且覆盖options |
| ICMP error packet | IP plugin另行复制quote | 不在本ADR范围 | separate error node复制quote | owner边界保持 |
| IP local layout | private duplicate minimum header | 保持private | IP local validates before ICMP input | 防止反向依赖 |
| Error behavior | existing typed node errors | 保持variant/next不变，mutation前失败 | packet error + punt/drop | 对齐packet failure class |

## 7. 决策记录

| ID | 最终决策 | Alignment | 理由 |
| --- | --- | --- | --- |
| D1 | ICMP base header是ICMP-owned private zerocopy layout | Aligned | V1、H1 |
| D2 | input从opaque cursor定位并直接borrow IP/ICMP headers | Aligned | V2、V3、H3、H5 |
| D3 | echo builder接收`&mut Buffer`并原地修改同一packet | Aligned | V4、V5、H2 |
| D4 | validation在首次mutation前完成，错误不改变bytes或opaque | Rust strengthening | Rust borrow/error contract；不改变VPP packet behavior |
| D5 | payload与chain完全保留，不linearize、不分配、不copy | Aligned | V4、V5、ADR-0007 |
| D6 | IPv4 checksum完整重算但直接读取同一Buffer | Intentional divergence | 保留existing behavior并覆盖options，无staging copy |
| D7 | IP owner补三个窄header mutation method | Rust ownership adaptation | H6；避免raw byte mutation与跨plugin field exposure |
| D8 | IP-owned ICMP error generation与其quote copy排除 | Aligned ownership boundary | V7、ADR-0005 |

## 8. 变更清单

### Add

- `hammer-plugin-icmp/Cargo.toml`：`zerocopy = { workspace = true }`；
- `Ipv4Header::set_identification`、`Ipv4Header::swap_addresses`、
  `Ipv6Header::swap_addresses`。

### Modify

- `icmp/src/protocol.rs`：`IcmpHeader` zerocopy derives与borrow methods；`build_echo_reply`改为直接
  `&mut Buffer`、two-phase validation与in-place mutation；
- `icmp/src/icmp.rs`：input/echo删除parsed/raw helper调用，使用opaque cursor + concrete borrows；
- `ip/src/protocol/ip.rs`：只增加D7列出的owner methods。

### Remove

- ICMP plugin中的`ParsedIpPacket`、`ip_header`、`protocol::wire::read_header` imports/callers；
- protocol production path中的header `copy_from_slice`、`rotate_left`、byte-index field writes；

### Preserve

- `IcmpBuildError`、`IcmpInputError`、`IcmpNodeError` variants与translation；
- ICMP Main、type table、registration、Graph Node与next slots；
- `NetworkOpaque` / `IpSecondaryOpaque` layouts；
- echo code、identifier、sequence、payload、Buffer chain与BufferIndex；
- IP local checksum validation与IP-owned ICMP error nodes。

## 9. API审批状态

| 项目 | 状态 | 理由 |
| --- | --- | --- |
| `IcmpHeader`改成private zerocopy layout | accepted modification | existing owner type，不新增外部API |
| `build_echo_reply(&mut Buffer, ...)` | accepted breaking crate-private API | direct Buffer是所需ownership boundary；无external caller |
| 三个IP header窄mutation methods | accepted public methods | dependent ICMP plugin不能绕过IP-owned private fields；issue #344授权实施 |
| `IcmpEchoHeader` | rejected | identifier/sequence不被读取或修改，无需wrapper |
| parsed/borrow/header wrapper | rejected | 重建ADR-0031已删除的中间owner |
| ICMP opaque | rejected | existing NetworkOpaque已拥有全部跨Node facts |
| generic raw header helper | rejected | 破坏bounds-first typed borrow contract |

issue #344 已明确批准实施上述API。

## 10. 实施门槛与verdict

本轮实现完成后的pre-commit gate为：

```bash
cargo check -p hammer-plugin-icmp
```

本轮不增加或运行测试。

Open questions: none。VPP与Hammer owner证据已决定header ownership、dispatch boundary、echo
mutation、chain与ICMP error exclusion；剩余工作是取得D7 API批准后实施。

**Design verdict: Aligned.** 该设计把ICMP input/echo迁移到VPP-style direct Buffer header access，
以Rust zerocopy borrows表达生命周期，不引入中间packet owner、兼容parser、opaque扩张或payload
copy。它不会声称IP-owned ICMP error quote generation是zero-copy。

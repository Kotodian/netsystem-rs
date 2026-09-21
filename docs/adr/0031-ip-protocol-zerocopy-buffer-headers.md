# ADR-0031: IP protocol zerocopy header borrow 与 Buffer opaque offset

Status: accepted

Date: 2026-09-21

本文记录已实施的设计边界；本轮只覆盖 IP plugin 的生产代码与本 ADR，不新增或运行测试。

目标是把 IP plugin 的 protocol encode/decode 都收敛到 `zerocopy`：decode 直接借用
Data-Plane Buffer 中的 header，encode 直接借用最终 destination Buffer 中的 header，整个
codec 不产生 owned header、临时 encoded bytes 或 copy-back。删除 `ParsedIpPacket`、
`ParsedIpFragment` 及其 parser 层；跨 Graph Node 使用的 offset 写入 Buffer opaque，并且所有
opaque 访问必须经过 `hammer_core::buffer_opaque!`。

本文细化 ADR-0005、ADR-0006、ADR-0007、ADR-0026 和 ADR-0030，不改变这些 ADR 已确定的
Buffer 生命周期、FIB/DPO、Feature Arc、interface 或 reassembly ownership。

## 1. 决策范围

### 1.1 本文决定

1. `Ipv4Header`、`Ipv6Header`、`Ipv6FragmentHeader` 以及 IP local 使用的固定 protocol
   header 通过 `zerocopy` 直接借用 Buffer bytes；
2. decode 只产生 `&Header`，encode 只产生指向最终 Buffer storage 的 `&mut Header`；
3. 删除 `ParsedIpPacket`、`ParsedIpFragment`、`parse_ip_header`、fragment parser、
   `ip_header` 和 `protocol/wire.rs`，不保留 alias、deprecated wrapper 或兼容路径；
4. 普通 L3/L4 cursor facts 继续由 primary `NetworkOpaque::ip()` 持有；packet 进入 reassembly
   后，fragment offsets/range 写入同一 primary opaque 的 `NetworkOpaque::reassembly()` member；
5. `IpSecondaryOpaque` 保持现有 lookup + ICMP error 布局，不保存 fragment metadata；
6. opaque 的取得方式只能是 `hammer_core::buffer_opaque!`，不得直接访问 raw opaque bytes、
   pointer cast、`transmute` 或增加任意 byte-offset accessor；
7. 本次完成门槛只有 `cargo check -p hammer-plugin-ip`。不新增、不修改、不运行测试。
8. adjacency rewrite 的 L3 header 读写也必须使用同一 Buffer 上的 zerocopy header borrow；
   adjacency 保存的二层 rewrite template 仍按既有接口复制到 Buffer 前置区。

### 1.2 非目标

- 不改变 FIB/DPO lookup 顺序、flow-hash 算法、Feature Arc、node next 或 packet error taxonomy；
- 不改变 reassembly context、handoff、overlap、timeout 或 Buffer-chain ownership 算法；
- 不改变 primary/secondary opaque 固定容量，也不改变 `NetworkOpaque`、
  `NetworkReassemblyOpaque`、`IpSecondaryOpaque` 的 size、alignment 和既有字段 offset；
- 不删除 transport plugin 仍使用的 `BufferPacketCursor`；
- 不引入跨 Buffer segment 的 header borrow、linearization 或临时连续 header storage；
- 不重写 IPv4/IPv6 fragmentation。VPP fragmentation 会为新 fragment materialize header 和
  payload；本文不能把该协议操作描述成 zero-copy；
- 不顺带修改 TCP/UDP header 模型；TCP 对 primary reassembly member 的现有使用另行处理；
- 不在本轮迁移依赖 IP public parser 的 ICMP plugin，也不以 workspace build/test 作为验收；
- 不增加测试。

### 1.3 `IpHeader` 的含义

本文中的 `IpHeader` 是 Buffer bytes 中实际存在的 family-specific header，不是一个待新增的
Rust 类型。不得新增 `IpHeader<'a>` enum、newtype、`View`、trait object 或 owned aggregate。
family node 已经知道自己处理 IPv4 还是 IPv6，直接使用：

```text
&Ipv4Header / &mut Ipv4Header
&Ipv6Header / &mut Ipv6Header
&Ipv6FragmentHeader / &mut Ipv6FragmentHeader
```

reference lifetime 直接来自 `Buffer::current()` 或 `Buffer::current_mut()`，不得跨 Graph Node、
enqueue、handoff、advance、trim、chain relink 或 free 保存。

## 2. Encode/decode zero-copy 合同

### 2.1 Decode

- 先从 opaque 取得 offset，再对 `buffer.current()` 执行 checked slicing；
- 使用 `zerocopy::FromBytes::ref_from_prefix` 返回指向该 slice 的 `&Header`；
- 不使用 `ptr::read_unaligned`、`transmute`、`read_header<T>() -> T`、header clone、stack
  header array、`Vec`、`to_vec` 或 `copy_from_slice` 到临时 header；
- borrowed pointer 必须等于 `buffer.current().as_ptr() + offset`；
- borrow 结束后才允许 mutably borrow opaque 或改变 Buffer current window；
- malformed/split header 在写 opaque 前失败，不分配 linearization storage。

### 2.2 Encode

- caller 先在最终 destination Buffer 中保留并验证完整 header range；
- 使用 `zerocopy::FromBytes::mut_from_prefix` 直接取得 `&mut Header`；
- network-order scalar、address octets、length、flags 和 checksum 直接写最终 header fields；
- 不构造 owned header、encoded `Vec<u8>`、stack encoded array 或第二段 scratch storage；
- 不允许 `encode() -> bytes`、header-to-header copy 或 read-modify-copy-back；
- header borrow 结束后才发布 opaque cursor 或 enqueue Buffer；失败不得暴露半编码 packet。

scalar decode、address value、checksum accumulator、flow hash、next/error 和 reassembly key 不属于
packet/header copy。`IpFragmentKey` 的生命周期超过一次 Buffer borrow，是 Bihash/context 必须
拥有的稳定 identity，因此保留。

这里的 zero-copy 只约束 protocol header codec。新 packet 的最终 bytes 仍必须写入 Buffer；
fragmentation 按协议创建独立 fragment 时仍会复制 payload。这不是 codec staging copy。

## 3. `IpSecondaryOpaque` 的结论

### 3.1 Hammer 当前定义

Hammer 确实存在 `IpSecondaryOpaque`，定义在
`crates/hammer-plugins/net/ip/src/lookup.rs:589-597`。它是 IP plugin 对 Buffer 56-byte
`opaque2` 的 owner-defined overlay：

```text
byte  0 .. 24   LookupMetadata
byte 24 .. 48   reserved
byte 48 .. 56   icmp_error
```

它通过 `#[hammer_component_macros::buffer_opaque(secondary)]` 声明，lookup、adjacency、IP/ICMP
error path 都通过 `hammer_core::buffer_opaque!` 借用。它是 Hammer 当前跨 graph path 的
secondary layout，不是 VPP 中一个同名类型。

### 3.2 为什么 fragment offsets 不放这里

Vendored VPP 的 `vnet_buffer_opaque2_t` 位于
`third_party/vpp/src/vnet/buffer.h:440-523`。其中 `ip.reass` 只保存 shallow virtual
reassembly 的 `{thread_index, pool_index, id}` output identity；普通 fragment header offset、
fragment range 和 range-chain metadata 不在 `opaque2`。

这些 ordinary reassembly facts 位于 primary
`vnet_buffer(b)->ip.reass`（`third_party/vpp/src/vnet/buffer.h:174-240`）。IPv6 full/shallow
reassembly 都把 `ip6_frag_hdr_offset`、fragment range 和 mutable range 写到这里。

Hammer fragmentation 当前还会把整个 `IpSecondaryOpaque` 从 source Buffer 复制给每个新
fragment（`crates/hammer-plugins/net/ip/src/adjacency.rs:1628-1749,1795-1895`）。把 input
fragment 的临时 range/offset 塞进这个 secondary layout，会把错误生命周期的 metadata 一起
复制到 output fragments。

因此本文明确拒绝在 `IpSecondaryOpaque.reserved` 中增加 `IpReassemblyMetadata`。现有
`IpSecondaryOpaque` 的 lookup、reserved 和 ICMP error 布局全部保持不变。

## 4. VPP 证据

| ID | Vendored path 与 symbol | 已核实行为 | 本文约束 |
| --- | --- | --- | --- |
| V1 | `third_party/vpp/src/vlib/buffer.h:253-280`, `vlib_buffer_get_current`, `vlib_buffer_advance` | header 由 `data + current_data` 定位，advance 只改变 Buffer window | header reference 必须直接来自 current Buffer |
| V2 | `third_party/vpp/src/vnet/buffer.h:111-240`, `vnet_buffer_opaque_t` | primary opaque 保存 L2/L3/L4 offsets；`ip.reass` 是 primary union member | Hammer 普通 cursor 与 fragment facts都属于 primary `NetworkOpaque` |
| V3 | `third_party/vpp/src/vnet/buffer.h:440-523`, `vnet_buffer_opaque2_t` | secondary `ip.reass` 仅是 shallow reassembly output identity | 不把普通 fragment offsets/ranges放入 `IpSecondaryOpaque` |
| V4 | `third_party/vpp/src/vnet/ip/ip4_input.c:40-58,101-176`; `ip4_input.h:20-104,252-289` | IPv4 input 直接把 current pointer 当作 `ip4_header_t *` 读取 | Rust 用 bounds-first zerocopy borrow，不复制 header value |
| V5 | `third_party/vpp/src/vnet/ip/ip6_input.c:67-180`; `ip6_input.h:24-125` | IPv6 input 直接读取 current Buffer header | IPv6 fixed/extension headers同样直接借用 |
| V6 | `third_party/vpp/src/vnet/ip/ip4_forward.c:87-221`; `ip6_forward.c:722-878` | lookup/load-balance 从 packet header 取地址和 hash inputs | lookup 不依赖 parsed packet snapshot |
| V7 | `third_party/vpp/src/vnet/ip/reass/ip6_sv_reass.c:406-438` | reassembly 直接接收 fragment-header pointer，把 `ip6_frag_hdr_offset` 和 fragment/range facts写到 primary `ip.reass` | Hammer reassembly 在 header borrow结束后切换 primary union member并发布 fragment facts |
| V8 | `third_party/vpp/src/vnet/ip/reass/ip6_full_reass.c:940-995`; `ip4_full_reass.c:940-954` | full reassembly 的 per-Buffer fragment/range state位于 primary `ip.reass` | fragment metadata owner不是 secondary opaque |
| V9 | `third_party/vpp/src/vnet/ip/reass/ip4_sv_reass.c:561-572`; `ip6_sv_reass.c:558-569` | 只有 extended shallow-reassembly context identity写入 `vnet_buffer2(...)->ip.reass` | 不把 VPP 的特殊 output identity泛化成普通 fragment metadata |
| V10 | `third_party/vpp/src/vnet/ip/ip_frag.c:130-220,400-495` | fragmentation 创建新 Buffer 并复制协议要求的 header/payload | 不宣称整个 IP plugin 绝对无 copy |
| V11 | `third_party/vpp/src/vnet/ip/ip4_forward.c:87-221`; `ip6_forward.c:722-878`; `vnet/interface.c` rewrite path | forwarding 在当前 Buffer 原地更新 TTL/Hop-Limit，再把已发布的 L2 rewrite 前置到 packet | adjacency 的 L3 header 使用 zerocopy borrow；L2 template copy 保持为硬件 rewrite 语义 |

Scoped search `rg "owner_thread_index" third_party/vpp/src/vnet/ip/reass third_party/vpp/src/vnet/tcp`
只在 IP reassembly 路径发现 `vnet_buffer(...)->ip.reass.owner_thread_index`，TCP 没有对应使用。
Hammer TCP 的现有复用是独立差异，不改变本文对 IP fragment owner 的判断。

## 5. Hammer 基线

| ID | Hammer path 与 symbol | 当前事实 | 迁移结论 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-plugins/net/ip/src/protocol/wire.rs` | raw pointer helper + `read_unaligned` owned copy + owned write | 删除整个 module，不增加 replacement helper |
| H2 | `protocol/ip.rs:112-135` | `ParsedIpPacket`、`ParsedIpFragment`复制 header-derived facts | 删除两个类型和全部 parser |
| H3 | `protocol/ip.rs:201-325` | IP headers 是 packed、Copy layout，部分 method 按值接收 | 改为 zerocopy layout，取消 Copy/Clone，getter接收 borrow |
| H4 | `crates/hammer-service/src/opaque.rs:53-217`, `NetworkIpOpaque` | 已保存 packet/L3/L4 cursor、version/protocol/ECN/FIB facts | 普通跨节点 offset 继续复用该 owner |
| H5 | `opaque.rs:219-255`, `NetworkReassemblyOpaque` | primary `NetworkOpaque` union 已有 reassembly member；前11 bytes保存 next/error/owner/rewrite，后17 bytes reserved | fragment facts只使用这17 bytes，不改变既有字段位置或总layout |
| H6 | `opaque.rs:259-349`, `NetworkOpaque::{ip,reassembly}` | primary union member有直接 borrow API | consumer先通过 `buffer_opaque!` 取得 `NetworkOpaque`，再选择正确member |
| H7 | `crates/hammer-plugins/net/ip/src/lookup.rs:576-597`, `IpSecondaryOpaque` | secondary overlay是 lookup + reserved + final ICMP word | layout保持不变，不增加fragment member |
| H8 | `input.rs:128-220` | input 先构造 Parsed packet 再发布 cursor | family node直接borrow，validation完成后才写 `NetworkOpaque::ip()` |
| H9 | `lookup.rs:884-1175`; `local.rs:515-930` | lookup/local依赖 Parsed aggregate和owned header reads | 改为 opaque cursor + concrete header borrow |
| H10 | `reassembly.rs:523-750,1018-1066` | reassembly 构造 `ParsedIpFragment`，context随后保留 key/index/range/header length | parsed carrier删除；physical offsets留在Buffer primary reassembly opaque |
| H11 | `transport/tcp/src/input.rs:298` | TCP 调用 `NetworkOpaque::set_handoff_source_worker` | 既存差异，按用户要求留给后续工作 |
| H12 | `crates/hammer-plugins/net/ip/src/adjacency.rs:1100-1290` | rewrite path 原先按 byte offset 读取/修改 IP header，L2 rewrite template 通过 `push_uninit` 前置 | L3 field access 改为 `Ipv4Header`/`Ipv6Header` zerocopy borrow；仅保留 L2 template copy |

## 6. 最终 Buffer metadata 设计

### 6.1 普通 IP cursor

IP input 在所有 bounds、version、length、checksum 和 extension validation 成功后，结束 header
borrow，再通过 opaque macro 一次发布 cursor：

```rust
let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
network.set_packet_cursor(cursor);
network.ip_mut().set_ip_version(version);
network.ip_mut().set_ip_protocol(protocol);
```

实际实现应合并 mutable member borrow，避免示例式重复 borrow。这里的要点是：不允许
`buffer.primary_opaque`、raw storage、pointer cast 或任意 offset API；macro 是唯一入口。

`NetworkOpaque::ip()` 继续拥有：

- packet length；
- L3 header offset/length；
- L4 header offset/length与payload offset；
- IP version/protocol/ECN；
- FIB selection facts。

### 6.2 Reassembly member

`NetworkOpaqueOverlay` 是 union。packet 进入 reassembly node 前，`ip()` member有效；reassembly
node从它读取所需cursor，直接借用IP/fragment headers并完成validation。header borrow结束后，
node通过同一个 opaque macro取得mutable primary overlay并选择`reassembly_mut()`，此后该Buffer
的 active interpretation 是 reassembly member，不能继续读取旧`ip()` facts。

`NetworkReassemblyOpaque` 的既有17-byte `reserved` 改成以下private fixed-byte facts，不新增
`IpReassemblyMetadata` wrapper：

```text
fragment_header_offset   2 bytes
fragment_payload_offset  2 bytes
fragment_start           2 bytes
fragment_end             2 bytes
more_fragments           1 byte
reserved                 8 bytes
```

字段语义：

- offsets 都相对当前 `buffer.current()`；
- IPv4 `fragment_header_offset` 指向 IPv4 header，IPv6 指向 Fragment extension header；
- `fragment_payload_offset` 指向该 fragment 的payload起点；
- `fragment_start..fragment_end` 是原packet payload中的half-open logical range；
- `more_fragments` 只能由 producer 写0或1；
- 所有 `usize -> u16` conversion 在 opaque mutation 前 checked；
- 不增加 `VALID` tag。Graph path与union member transition就是有效性合同，符合 ADR-0007；
- storage 使用无padding、all-bit-valid的byte representation，owner method负责`u16`转换。

VPP internal range使用inclusive last；Hammer现有reassembly算法使用half-open end。本文保留Hammer
算法，不为表面字段一致修改overlap/completion语义。这是representation差异，不是owner或packet
data movement差异。

producer/consumer只能按以下形状访问：

```rust
let reassembly = hammer_core::buffer_opaque!(buffer => NetworkOpaque).reassembly();
let reassembly = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque).reassembly_mut();
```

不得通过 `IpSecondaryOpaque`、raw primary storage或缓存的pointer访问这些facts。

`NetworkReassemblyOpaque` 的总size/alignment，`next_index`、`error_next_index`、
`owner_thread_index`、`save_rewrite_length` offset，以及外层 `NetworkOpaque` layout保持不变。

## 7. Header layout 与借用

三个existing public packet layouts保留名称与IP plugin owner：

```rust
#[derive(
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
#[repr(C)]
pub struct Ipv4Header { /* byte and byte-array fields */ }
```

`Ipv6Header`、`Ipv6FragmentHeader` 使用相同representation contract。

- multi-byte network fields继续用`[u8; N]`；
- size固定为20、40、8 bytes，alignment为1；
- 不derive `Clone`/`Copy`；
- getter接收`&self`，mutation使用owner-defined narrow method；
- IPv4 options和IPv6 variable extension payload仍是Buffer slice borrow；
- 不新增`header_ref<T>`、`header_mut<T>`、`PacketHeader` trait或borrow wrapper。

每个caller直接执行 bounds-first concrete cast。`zerocopy` 已提供所需能力，generic Hammer
helper只会重建被删除的`protocol/wire.rs`抽象。

## 8. Node flow

### 8.1 Input、lookup 与 local

`Ip4InputNode`/`Ip6InputNode`直接borrow对应header，保存当前node决策需要的local scalars，结束
borrow后通过`buffer_opaque!`发布`NetworkOpaque::ip()` facts。validation error不更新cursor。

lookup从opaque取得offset/FIB，再借用`&Ipv4Header`或`&Ipv6Header`读取destination。flow hash
保持 LPM -> selected load-balance config -> packet header inputs 顺序，不接收Parsed aggregate。

local从opaque读取version/protocol/offset，从concrete L3/L4 header borrow读取地址、端口和
checksum输入。成功后才更新L4 length/payload offset；checksum继续迭代Buffer chain，不
linearize payload。

### 8.2 Reassembly

reassembly node按family执行：

1. 通过`buffer_opaque!(buffer => NetworkOpaque).ip()`读取input cursor；
2. 直接borrow IP header和IPv6 Fragment header；
3. 验证chain length并计算`IpFragmentKey`、physical fragment offsets、logical half-open range和
   more-fragments scalar；
4. 结束所有packet/header borrows；
5. 通过`buffer_opaque!(mut buffer => NetworkOpaque).reassembly_mut()`一次写入fragment facts；
6. context operation接收stable key与`BufferIndex`，需要physical boundary时从被保留Buffer的
   primary reassembly member读取，不保存header reference、raw pointer、packet slice或
   Parsed replacement；
7. handoff只转移`BufferIndex`；target worker重新通过opaque macro和Buffer borrow取得facts。

reassembly完成时直接mutably borrow first Buffer中的concrete header并原地更新length、fragment
fields和checksum。payload继续通过既有chain relink/trim语义保留，不增加linearization或copy。

`IpFragmentKey`保留，因为context和directory必须在header borrow结束后拥有稳定identity。它不
包含packet bytes、header mirror或physical offset。

### 8.3 Encode 与生成路径

既有packet的TTL、Hop Limit、address、fragment和checksum都通过同一Buffer中的`&mut Header`
原地修改。`write_ipv4_push_header`、`write_ipv6_push_header`、ICMP error header生成也必须直接
写最终destination Buffer，不先生成临时header/byte array。

IPv4/IPv6 fragmentation创建独立output Buffers的payload copy继续保留；它不是本文删除的
header codec staging copy。fragment clone/refcount sharing需要单独ADR，不在本次范围。

### 8.4 Adjacency rewrite

`ip4-rewrite` 与 `ip6-rewrite` 在执行 adjacency MTU、TTL/Hop-Limit 和 multicast suffix 逻辑时，
直接从 `buffer.current_mut()` 取得 `Ipv4Header` 或 `Ipv6Header`。版本、长度、DF、目的地址和
生存期字段都来自该 borrow；IPv4 checksum 在 header borrow 结束后对同一 Buffer window 计算，
再通过第二个短 mutable borrow 写回。MTU 分支需要回滚 TTL 时也只修改同一 concrete header。

adjacency 的 `rewrite_data` 是接口/硬件层保存的二层前置模板，不是 IP header codec。它仍由
`push_uninit(...).copy_from_slice(...)` 放入最终 Buffer；这一次 copy 是 VPP adjacency rewrite
的 L2 materialization，不产生 owned IP header、临时 encoded IP bytes 或 copy-back。IPv4/IPv6
fragment node 创建独立 output Buffer 时的 header/payload copy仍按 8.3 的 fragmentation 非目标处理。

## 9. Error、lifetime 与 ABI

不增加error type或variant。zerocopy borrow failure映射到owner已有packet errors：header不足、
version错误、variable header越界和chain total length错误都在opaque mutation前失败。mutable
encode在首次field mutation前验证完整destination range。

header reference不改变Buffer ownership/refcount。mutable header reference存活期间不得读取或
写同一Buffer opaque；opaque mutation前必须结束header borrow。shared chain segment没有exclusive
ownership时不得mutably borrow。

删除public Parsed/parser APIs是source-breaking change。按用户指定，本ADR的实现验收只覆盖
`hammer-plugin-ip`；依赖这些API的ICMP plugin迁移和workspace-wide compatibility不在本次完成
条件中，不能据此宣称workspace已经完整迁移。

primary/secondary fixed capacity不变，但`NetworkReassemblyOpaque` reserved bytes获得明确语义，
因此依赖Buffer layout的daemon/plugin artifacts在最终集成时仍须统一rebuild。

## 10. 三向语义差异

| 维度 | 当前 Hammer | 本 ADR 目标 | Vendored VPP | 决策 |
| --- | --- | --- | --- | --- |
| Header decode | raw pointer + owned unaligned copy | zerocopy reference into Buffer | typed pointer into current Buffer | 对齐direct Buffer access |
| Header encode | owned/raw/byte-index paths混用 | final Buffer mutable borrow，无staging | direct typed pointer mutation | 对齐single final storage |
| Parsed state | 两个owned aggregate | opaque facts + short header borrow | hot path无对应parsed aggregate | 删除 |
| Ordinary offsets | Parsed与`NetworkOpaque::ip()`重复 | primary IP member是唯一跨node owner | primary vnet opaque | 删除双publication |
| Fragment facts | Parsed fragment临时carrier | primary reassembly member | primary `ip.reass` | owner对齐 |
| Secondary IP opaque | lookup + ICMP + reserved | 保持不变 | opaque2不承载ordinary fragment facts | 不新增fragment member |
| Opaque access | typed macro已存在 | 所有caller只用`buffer_opaque!` | direct fixed overlay | 复用既有API |
| Range representation | context half-open range | opaque/context仍用half-open | VPP per-buffer range是inclusive | 有意保留Hammer算法 |
| TCP use | TCP写primary reassembly owner field | 本文不改 | VPP scoped search无TCP对应使用 | 后续事项 |
| Verification | workspace tests可用 | 只compile IP plugin，不加测试 | 不适用 | 用户明确限定 |

## 11. 变更清单

### Delete

- `crates/hammer-plugins/net/ip/src/protocol/wire.rs`；
- `protocol::wire::{header_ptr,header_mut_ptr,read_header,write_header}`；
- `ParsedIpPacket`、`ParsedIpFragment`；
- `parse_ip_header`、`parse_ip_fragment`、`parse_ip_fragment_with_chain_len`；
- `hammer_plugin_ip::ip::ip_header`；
- IP plugin内部针对这些surface的imports/re-exports/callers。

### Modify

- `crates/hammer-service/src/opaque.rs`：只在existing
  `NetworkReassemblyOpaque.reserved`中定义fragment facts及narrow getter/setter；保留总layout和
  既有field offsets；
- `crates/hammer-plugins/net/ip/src/protocol/ip.rs`：header zerocopy derives、borrow getters和
  direct destination encode；
- `input.rs`：family-specific borrow与failure-atomic primary IP opaque publication；
- `ip.rs`：删除parsed reconstruction；
- `lookup.rs`、`local.rs`、`icmp_error.rs`：opaque offset + concrete header borrow；
- `reassembly.rs`：direct fragment borrow，通过primary reassembly opaque保存offset/range；
- `adjacency.rs`：rewrite path 通过 concrete zerocopy IP header 读写 TTL/Hop-Limit、长度、DF 与
  multicast suffix；保留二层 rewrite template materialization；
- `protocol/mod.rs`：删除`wire` module。

### Preserve

- `IpSecondaryOpaque`及其lookup/ICMP layout；
- `BufferPacketCursor`；
- `IpFragmentKey`和existing reassembly context/directory/handoff algorithm；
- graph nodes、next slots、packet errors、FIB/DPO/Feature Arc behavior；
- protocol fragmentation所需的output payload copy。

## 12. API 审批状态

| 项目 | 状态 | 理由 |
| --- | --- | --- |
| existing IP header types增加zerocopy traits并删除Copy/Clone | proposed modification | 改变access contract，不新增owner |
| 删除Parsed/parser/raw-pointer APIs | explicitly requested design | 用户明确要求删除，不保留兼容层 |
| `NetworkReassemblyOpaque` fragment getter/setter | proposed narrow API | primary network overlay由hammer-service拥有；IP plugin只能经`buffer_opaque!`与member borrow访问 |
| `Ipv4Header::{set_ttl}` / `Ipv6Header::{set_hop_limit}` | accepted narrow methods | adjacency 需要在最终 Buffer header 上原地更新生存期字段；不引入 header wrapper 或 copy API |
| `IpSecondaryOpaque` fragment fields | rejected | 与VPP primary owner不符，且output fragmentation会复制该secondary layout |
| `IpReassemblyMetadata` nested type | rejected | 无需wrapper；existing primary member有足够reserved bytes |
| `IpHeader<'a>`/generic header helper | rejected | 重新包装borrow或重建generic raw access layer |
| new test surface | rejected for this change | 用户明确要求不加测试 |

本 ADR 已按上述边界实施；不得借zerocopy迁移扩张到TCP、UDP、ICMP plugin或fragment clone
redesign。TCP 现有 primary reassembly member 使用留给后续工作。

## 13. 验证与 verdict

实现完成、review和format结束后，本次唯一功能门槛是：

```bash
cargo check -p hammer-plugin-ip
```

不新增测试，不运行`cargo test`，也不把workspace compile/test写成本次完成条件。

**Implementation verdict: Aligned for the bounded IP slice.** VPP证据确认
`IpSecondaryOpaque` 是 Hammer 自有的 lookup + ICMP secondary overlay；VPP 没有同名 Hammer
类型，VPP 的 ordinary fragment facts 仍由 primary `ip.reass` 承载。本实现已将 adjacency 的
L3 rewrite 纳入同一 zerocopy contract，同时保留 VPP adjacency 的 L2 rewrite materialization。

实际验证：`cargo check -p hammer-plugin-ip` 通过（仅已有 warning）；`cargo fmt --all -- --check`
和 `git diff --check` 通过。未新增或运行测试，TCP 未纳入本次修改。
ordinary fragment metadata属于primary `ip.reass`，不是secondary opaque。Hammer目标因此是：
通过`buffer_opaque!`取得`NetworkOpaque`，普通cursor使用`ip()` member，进入reassembly后切换到
`reassembly()` member；现有`IpSecondaryOpaque`只保留lookup和ICMP error职责。encode/decode均
直接借用最终Buffer storage，不保留Parsed packet/fragment、owned header或中间encoded bytes。

# ADR-0026: VPP 风格 device-input、ethernet-input 与输入 Feature Arc

Status: accepted

Date: 2026-09-19

本文记录已批准的实现设计。用户已明确授权按本文范围实现；实现不得增加第 11 节未定义
的类型、API、状态或能力。

本文补充 ADR-0025 的 Feature Arc 机制，并取代 ADR-0005 中“device RX node 可直接把
Ethernet 流量送往任意协议 parser”但尚未定义公共 Ethernet 输入路径的空白。ADR-0005
仍然有效的边界是：设备插件拥有真实 RX node，`InterfaceMain` 拥有接口和队列，
`DeviceMain` 拥有 device-input worker scope 与 RX 统计，IP 插件拥有 IP parser。

## 1. 问题、范围与完成定义

Hammer 当前没有 VPP 的公共 `device-input` graph anchor、`device-input` Feature Arc、
`ethernet-input` node 或 EtherType 注册表。设备插件因此没有统一的 Ethernet ingress
合同，IP plugin 也没有在不反向依赖 service 的前提下声明 `0x0800 -> ip4-input` 和
`0x86dd -> ip6-input` 的入口。

本 ADR 设计以下完整 slice：

1. service-owned、默认 disabled 的 `device-input` Driver Node，真实 device RX nodes 以
   graph sibling 方式共享其 next table；
2. 以 `device-input` 为 start、以 `ethernet-input` 为 `last_in_arc` 的 Feature Arc；
3. Hammer 对 `vnet_feature_start_device_input` 的等价 packet-path 操作；
4. service-owned `ethernet-input` Internal Node；
5. 直接借用 Buffer 当前窗口、解析 Ethernet/VLAN header 并只推进 cursor 的 zero-copy
   parser；
6. VPP 风格的 EtherType 到 graph next slot 注册与分发，注册者可以是 IP、LLDP、MPLS
   或其它独立 plugin；
7. packet error、startup ordering、publication 和测试合同。

“支持注册”在本 ADR 中是明确的 startup contract：graph nodes 已全部 materialize 后、
Data Workers 启动前，协议 owner 把自己的 EtherType 和 `NodeId` 注册给 Ethernet owner。
本文不授权 worker 运行期注册、覆盖或注销 EtherType。

本文不实现以下能力：

- Ethernet L2 mode、bridge/BVI、VLAN subinterface classification；
- LLC/SNAP input node；
- destination MAC filtering、secondary MAC 或 interface-down classification；
- `ethernet-input-type`、`ethernet-input-not-l2` variants；
- Ethernet RX redirect 或 L3 redirect；
- device plugin、具体 NIC driver 或 tunnel parser；
- runtime plugin load 后的 EtherType mutation；
- 新 Buffer owner、payload copy、临时 packet `Vec` 或新的 opaque wrapper。

这些非目标不得用占位 callback、空 trait、`Option<fn()>`、install flag 或预留状态提前
进入实现。

## 2. VPP 证据账本

| ID | vendored VPP 来源 | 已验证行为 | Hammer 约束 |
| --- | --- | --- | --- |
| V1 | `third_party/vpp/src/vnet/devices/devices.c:15-30`, `device_input_fn`, `device_input_node` | `device-input` 是 disabled input node；自身函数返回 0，真实 device RX nodes 使用它的 graph 形状 | Hammer 的 node 是 disabled Driver anchor，不轮询设备、不拥有 driver state |
| V2 | `devices.c:32-68`, `VNET_FEATURE_ARC_INIT(device_input)` 与 `VNET_FEATURE_INIT(ethernet_input)` | arc start 是 `device-input`，`ethernet-input` 是 `last_in_arc`；其它 ingress features 排在它之前；arc index 缓存在 `feature_main` | Hammer 用现有 `#[feature_arc]`/`#[feature]` 和 `FeatureMain`，不新建 Feature registry |
| V3 | `third_party/vpp/src/vnet/feature/feature.h:325-350`, `vnet_device_input_have_features`, `vnet_feature_start_device_input` | 无 feature 时保留 caller 的 default next；有 feature 时按 `sw_if_index` 装载 config index，消费第一个 next 并写 Buffer cursor | Hammer 复用 `FeatureMain::has_features`/`start_feature_arc`，新增的 device 入口不复制 config 逻辑 |
| V4 | `third_party/vpp/src/vnet/devices/devices.h:12-40`, device input next table | VPP 静态列出 Ethernet、drop、punt 和多个 IP/MPLS next | Hammer 保留 service-owned Ethernet/drop/punt；IP/MPLS/tunnel next 由 owner 注册 graph edge，service 不出现 `ip4-input` 名称或类型 |
| V5 | `third_party/vpp/src/vnet/ethernet/node.c:70-163`, `parse_header` | parser 从当前 buffer 指针直接读 Ethernet header，记录位置并 advance；继续直接读最多两层 VLAN header；不复制 payload | Hammer parser 借用 `Buffer::current()`，在完整验证后调用一次 `Buffer::advance`；不分配、不复制 header 或 payload |
| V6 | `node.c:223-286`, `determine_next_node` | parse error 走 drop；L2 mode 恢复 L2 start；常见 EtherType 读 cache；其它 EtherType 查 sparse vector；未注册类型标记 `UNKNOWN_TYPE`；长度字段转 LLC | Hammer 首期只实现 L3 path：malformed/drop、registered/dynamic slot、unknown/punt；L2 与 LLC 明确延期 |
| V7 | `node.c:17-30`, `foreach_ethernet_input_next` | punt、drop、LLC 是固定 next，IP4 只是固定 fast-path slot | Hammer 只固定当前确实存在的 punt/drop；不存在的 LLC node 不建假 edge；协议 next 全部动态追加 |
| V8 | `node.c:2087-2127`, three Ethernet node registrations | 三个 variants 共享同一 next layout | 首期只有用户要求的 `ethernet-input`；如果以后增加其它 variants，必须用现有 sibling graph contract 共享 slots |
| V9 | `node.c:2146-2218`, `next_by_ethertype_init/register` | EtherType map 是 16-bit sparse index；注册写入 graph slot；IP4/IP6/MPLS direct fields 只是注册后 cache，不是注册所有权 | Hammer 直接复用 `hammer_infra::sparse_vec::SparseVec<u16>`；不增加 IP-specific cache fields |
| V10 | `node.c:2256-2288`, `ethernet_register_input_type` | 注册先给 Ethernet input nodes 增加 graph next，再发布 EtherType -> slot；多个 variants 必须得到同一 slot | Hammer 单 node 用 `NodeMain::add_node_next_slot`，成功后才 infallibly publish sparse mapping |
| V11 | `third_party/vpp/src/plugins/lldp/lldp_node.c:128`, `lacp/node.c:168`, `nsh/nsh.c:271` | 非 IP plugin 使用同一 EtherType registration surface | Hammer API 必须接受任意 `u16` EtherType 和 generic `NodeId`，不能是 IP 专用 API |
| V12 | `third_party/vpp/src/plugins/af_xdp/input.c:390-410`, `af_packet/node.c:440-466` | driver 先选 per-interface/default next；Ethernet ingress 再启动 device-input features；raw-IP paths 可不进入 Ethernet arc | Hammer device RX node 先有 default next，只对 Ethernet ingress 调用 device feature start；raw/tunnel path 保持 device-owned |
| V13 | `third_party/vpp/src/vnet/ethernet/error.def` | unknown type 是 PUNT；unknown VLAN、MAC mismatch、down 和 bad LLC length 是 DROP | Hammer unknown EtherType 记录 typed node error 后 punt；truncated/unsupported VLAN depth drop；未实现的分类不伪造 counter |

Scoped test search:

```text
rg "ethernet_register_input_type|UNKNOWN_TYPE|ethernet-input|device-input" \
  third_party/vpp/test third_party/vpp/src/plugins
```

vendored tests 主要通过 trace、具体 L2/subinterface/plugin 行为间接观察
`ethernet-input`；没有一个隔离的 EtherType registry unit suite。第 9 节的 Hammer 测试
因此从 V5-V13 的实现合同推导，并使用真实 graph behavior 验证，不声称复制上游测试。

## 3. Hammer 基线

| ID | Hammer 来源 | 当前事实 | 设计影响 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-service/src/feature.rs:102-156, 801-834` | `FeatureMain` 已拥有 per-arc config、presence bitmap 和 allocation-free `start_feature_arc` | device helper 必须委托现有逻辑，不增加 FeatureChain、install API 或第二套 cursor |
| H2 | `crates/hammer-service/src/feature.rs:1055-1065` | `feature_arc_init` 在 graph materialize 后、`start_workers` 前构建 arcs | device arc declaration和EtherType registration都属于同一 main-loop-enter startup window |
| H3 | `crates/hammer-runtime/src/node.rs:780-804, 880-1035` | graph siblings 共享 next table；给 owner 添加/修改 next 会传播到 siblings | device RX nodes 可以 `sibling_of = DeviceInputNode`，不需要 service device-next registry |
| H4 | `crates/hammer-runtime/src/node.rs:1849-1898` | `add_node_next_slot(s)` 已支持 validate、dedupe、failure atomicity 和 worker barrier/refork | Ethernet registration复用它，不新增 graph API |
| H5 | `crates/hammer-core/src/buffer/header.rs:194-244` | `current()` 返回当前窗口 borrow；`advance` 只改 offset/length；越界会 panic | parser 必须先用 slice bounds 验证完整 header，再 advance，不能把 malformed packet 变成 panic |
| H6 | `crates/hammer-infra/src/sparse_vec.rs:7-112` | 已有 VPP-style `SparseVec<T>`，支持 16-bit index space、get/insert/contains | EthernetMain 直接使用现有通用结构，不新增 map/helper |
| H7 | `crates/hammer-service/src/device/mod.rs:27-123` | `DeviceMain` 已是 process-global worker scheduling/RX counter owner | graph anchor与parser可放在同一 service domain，但不得把 interface、driver 或 Ethernet registry 塞进 DeviceMain |
| H8 | `crates/hammer-service/src/interface_model.rs:190-210` | `HwInterface.input_node_index` 已表达具体 device RX node | anchor 不替代真实 RX node，不改变 InterfaceMain 的 queue/node ownership |
| H9 | `crates/hammer-service/src/opaque.rs:259-281` | `NetworkOpaque` 只有 current-relative `l3_hdr_offset`，40-byte primary opaque 已有既定用户 | 首期 L3 parser不扩大 opaque；L2 offset/VLAN metadata等到 L2/subinterface设计时统一处理 |
| H10 | `crates/hammer-plugins/net/ip/src/input.rs:46-52` | IP4/IP6 input 是 plugin-owned concrete nodes | 只有 IP plugin 可以引用这些类型并注册对应 EtherTypes |
| H11 | `crates/hammer-plugins/net/ip/src/punt.rs:260-305` | IP 已有 graph materialize 后、feature arc init 前的 `ip_feature_init` callback | IP 的两项 Ethernet registration加入既有 callback，不新增 init framework |

### 3.1 Rust 类型与 VPP 类型逐项对照

下表是目标 Rust surface，不是当前已实现代码。每个类型只保留当前 slice 所需的 VPP
语义；“子集”表示相邻 VPP 字段属于第 1 节明确排除的 L2/LLC/subinterface 能力，不能在
本次实现中以占位字段提前加入。

| ID | 目标 Rust 类型/字段 | Owner 与可见性 | VPP 类型/字段 | 精确源码依据 | 对齐结论 |
| --- | --- | --- | --- | --- | --- |
| T1 | `hammer_service::device::DeviceInputNode` | service public zero-state node | `device_input_node` registration | `third_party/vpp/src/vnet/devices/devices.c:15-30` | disabled input anchor；process 返回 0；真实 device nodes 作为 sibling |
| T2 | `hammer_service::device::DeviceInputNext` | service public next enum | `vnet_device_input_next_t`、`VNET_DEVICE_INPUT_NEXT_NODES` | `third_party/vpp/src/vnet/devices/devices.h:12-40` | 保留 service-owned Ethernet/drop/punt；IP/MPLS next 改由 owning plugin动态添加 |
| T3 | `hammer_service::ethernet::EthernetInputNode` | service public zero-state node | `ethernet_input_node` | `third_party/vpp/src/vnet/ethernet/node.c:1689-1710, 2087-2103` | Internal Node处理完整Frame并返回packet count；next和error属于此node |
| T4 | `hammer_service::ethernet::EthernetInputNext` | service public next enum | `ethernet_input_next_t` | `node.c:17-30` | 当前只固定punt/drop；LLC延期；所有EtherType consumers动态追加 |
| T5 | `hammer_service::ethernet::EthernetMain` | service process-global owner | `ethernet_main_t` | `third_party/vpp/src/vnet/ethernet/ethernet.h:262-312` | 当前只实现其L3 EtherType dispatch子集，不复制interface/L2/redirect state |
| T6 | `EthernetMain.next_by_ethertype: UnsafeCell<SparseVec<u16>>` | private startup-published table | `ethernet_main_t.l3_next`、`next_by_ethertype_t.input_next_by_type` | `ethernet.h:235-249, 266-268`; `node.c:2146-2218` | key是host-order `u16` EtherType，value是`ethernet-input` local next slot；不建IP caches |
| T7 | private `EthernetHeader` | service private packet layout | `ethernet_header_t` | `third_party/vpp/src/vnet/ethernet/packet.h:21-29` | `#[repr(C)]`，字段为`[u8; 6]` destination/source和`[u8; 2]` EtherType，总长14 |
| T8 | private `EthernetVlanHeader` | service private packet layout | `ethernet_vlan_header_t` | `packet.h:96-106` | `#[repr(C)]`，TCI和inner EtherType均用`[u8; 2]`，总长4，避免unaligned `u16` borrow |
| T9 | `EthernetInputError` | service public node-local packet classification | `ethernet_error_t` active subset | `ethernet.h:170-176`; `error.def:40-45` | 只声明当前会产生的`HeaderTooShort`、`UnknownType`、`UnknownVlan`；完整语义见第7节 |
| T10 | `FeatureMain.device_input_feature_arc_index: UnsafeCell<u8>` | FeatureMain private cached index | `vnet_feature_main_t.device_input_feature_arc_index` | `third_party/vpp/src/vnet/feature/feature.h:76-108`; `devices.c:32-38` | arc registration写一次，packet path只读；不是installed state |
| T11 | existing `FeatureConfigMain`/`ConfigMain`/Buffer config cursor | FeatureMain private config state | `vnet_feature_config_main_t`/`vnet_config_main_t`/`current_config_index` | `feature.h:70-74, 333-348`; ADR-0025 | 不新增device-specific config owner；device入口复用generic Feature config |

T10 与 T11 的字段边界必须保持：`FeatureMain`只拥有per-arc
`Vec<FeatureConfigMain>`和VPP同位的device arc index；每个arc的`config_main`与
`config_index_by_sw_if_index`只放在`FeatureConfigMain`中。device入口不把这些config字段
复制回`FeatureMain`，也不新增`FeatureState`。

目标 packet layouts 明确为：

```rust
#[derive(zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
struct EthernetHeader {
    destination: [u8; 6],
    source: [u8; 6],
    ether_type: [u8; 2],
}

#[derive(zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
struct EthernetVlanHeader {
    priority_cfi_and_id: [u8; 2],
    ether_type: [u8; 2],
}
```

这两个类型表达真实packet header，不是 observer/view/wrapper；它们不拥有packet bytes，
也不从parser返回。`[u8; 2]` 通过 `u16::from_be_bytes` 解码，不要求当前slice满足`u16`
alignment。

### 3.2 Rust 方法与 VPP 函数逐项对照

| ID | 目标 Rust 方法/函数 | 目标签名或调用合同 | VPP symbol | 精确源码依据 | 对齐结论 |
| --- | --- | --- | --- | --- | --- |
| M1 | `DeviceInputNode::process` | `fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame) -> usize`，始终返回0 | `device_input_fn` | `devices.c:15-20` | 精确语义对齐 |
| M2 | generated device graph init | 注册Driver、named nexts、state Disabled | `VLIB_REGISTER_NODE(device_input_node)` | `devices.c:22-30`; Hammer macro `crates/hammer-component-macros/src/lib.rs:2145-2179` | Rust graph registration表达同一node事实 |
| M3 | `device_input_feature_init` | graph存在后调用arc/feature macro生成的registration，并缓存arc index | `VNET_FEATURE_ARC_INIT(device_input)`、`VNET_FEATURE_INIT(ethernet_input)` | `devices.c:32-38, 64-68`; `feature.h:113-151` | Hammer用现有startup inventory，不复制C constructor list |
| M4 | `FeatureMain::start_device_input` | `(&self, sw_if_index, &mut Buffer, default_next) -> u16` | `vnet_feature_start_device_input` | `feature.h:325-350` | 无feature保留default；有feature装载config cursor并取first next |
| M5 | `ethernet_main_init` / `EthernetMain::init` | 发布空`SparseVec<u16>` owner | `ethernet_input_init`、`next_by_ethertype_init` | `node.c:2146-2172, 2233-2253` | 只采用L3 dispatch table初始化；VLAN/interface pools延期 |
| M6 | `EthernetMain::global` | `fn() -> RuntimeResult<&'static EthernetMain>` | global `ethernet_main`、`ethernet_get_main` | `ethernet.h:312-314, 424` | Rust显式检查init boundary；packet node在boundary内将缺失视为bug |
| M7 | `register_ethernet_input` | private graph factory：注册node、固定nexts和error descriptors | `VLIB_REGISTER_NODE(ethernet_input_node)` | `node.c:2081-2103` | explicit factory是Hammer安装node errors所需，不增加public lifecycle API |
| M8 | `EthernetInputNode::process` | 使用fixed scratch逐包parse/classify，记录error并一次fanout | `ethernet_input_node`、`ethernet_input_inline` | `node.c:1180-1687, 1689-1710` | Frame批处理、buffer-local error和per-packet next对齐；不分配next `Vec` |
| M9 | private `parse_header` | `fn(&[u8]) -> Result<(u16, u8), EthernetInputError>`；返回final EtherType/header len scalars | `parse_header` | `node.c:70-163`; header layouts见T7-T8 | bounds-first Rust实现是安全差异；borrow原bytes且不复制 |
| M10 | private `EthernetMain::next_for_ethertype` | `fn(&self, u16) -> Option<u16>` | `eth_input_next_by_type`、`determine_next_node` sparse branch | `node.c:437-444, 268-284` | `Some(slot)`为registered next，`None`转`UnknownType`+punt |
| M11 | `EthernetMain::register_input_type` | `fn(&self, &NodeMain, u16, NodeId) -> RuntimeResult<u16>` | `ethernet_register_input_type`、`next_by_ethertype_register` | `node.c:2174-2218, 2256-2288` | 先add graph next，后infallibly publish mapping；重复type更新mapping，见第7节 |
| M12 | IP `ip_feature_init`内两次调用M11 | 从`NodeMain::node_by_name`取得`Ip4InputNode`/`Ip6InputNode`的`NodeId`，调用`register_input_type(nodes, 0x0800/0x86dd, node)` | `ethernet_register_input_type` in `ip4_init`/`ip6_init` | `third_party/vpp/src/vnet/ip/ip4_input.c:375-381`; `ip6_input.c:208-212`; Hammer callback `crates/hammer-plugins/net/ip/src/punt.rs:267-380` | concrete types、EtherType constants和调用只属于IP plugin；service不反向依赖IP |

M9 的 `Result` 是node内部的Copy classification，不是控制面 `RuntimeResult`，不分配也不
跨crate传播。M8 必须把它立即转换为 `EthernetInputError` counter、Buffer error index和
next slot；packet path不能把malformed frame作为函数级错误返回给main loop。

## 4. 所有权与模块边界

### 4.1 Device input graph anchor

`hammer-service::device` 增加零状态 `DeviceInputNode`。它是：

- graph name `device-input`；
- `NodeKind::Driver`；
- initial state `Disabled`；
- process 函数返回 0，不接触队列、Buffer 或 interface；
- `device-input` Feature Arc 的唯一 declared start node；
- 真实 device RX nodes 的 sibling owner。

`DeviceInputNode` 不是“通用 RX driver”。具体 plugin node 继续拥有 queue polling、descriptor
reclaim、offload metadata 和 device errors，并在 `HwInterface.input_node_index` 中发布自己。
设备 node 声明 `sibling_of = DeviceInputNode` 后共享 next slots；这正是 VPP device input
siblings 的 graph 关系，不需要 trait object、callback carrier 或 next-table mirror。

`DeviceInputNext` 只包含：

| Slot | Target | 原因 |
| --- | --- | --- |
| `Ethernet` | `ethernet-input` | 普通 Ethernet frame 的 default next |
| `Drop` | service `drop` | device 已判定不可处理的 frame |
| `Punt` | service `punt` | device 选择交给慢路径的 frame |

不包含 `Ip4Input`、`Ip6Input`、MPLS 或 tunnel variants。设备 plugin 确实需要 bypass
Ethernet 时，直接用现有 `NodeMain::add_node_next_slot` 把 owner node 加到 sibling next
table并保存返回 slot。service 不导入、查询或命名该协议节点。

### 4.2 Ethernet owner

`hammer-service::ethernet::EthernetMain` 是独立 process-global owner，只持有 Ethernet
input dispatch state：

```rust
pub struct EthernetMain {
    next_by_ethertype: UnsafeCell<SparseVec<u16>>,
}
```

这是目标 ownership shape，不是实现 patch。`SparseVec` 以 `u16::BITS` 初始化；key 是
host-order EtherType，value 是 `ethernet-input` 的 compact next slot。

`EthernetMain` 不属于 `DeviceMain` 或 `InterfaceMain`：device 不拥有协议分发，interface
不拥有 parser，Ethernet dispatch 也不拥有 consumer plugin。它不保存 IP type、trait、
function table、引用或 concrete state，只保存 graph `u16` slot。

`UnsafeCell` 只表达与 `FeatureMain` 相同的 owner-local startup publication：main thread 在
workers 启动前写，worker 启动后只读。没有 lock、atomic pointer、snapshot、Arc handle、
thread-local selector、installed flag 或 registration-closed flag。late registration 是
lifecycle programmer bug，直接断言，不增加可恢复状态机错误。

### 4.3 FeatureMain 的 device arc index

VPP 把 `device_input_feature_arc_index` 直接放在 `feature_main`。Hammer 同样在
`FeatureMain` 增加 private `device_input_feature_arc_index: UnsafeCell<u8>`，由 device arc
declaration callback 写一次，在 workers 启动后只读。初始化 sentinel 只用于发现违反
startup ordering 的 programmer bug；它不是 installed 状态，也没有 install/clear
生命周期。

不在 `DeviceMain` 再保存同一 index，不按 packet 用 arc name 查询 `BTreeMap`，也不把
index 包进 handle/newtype。

### 4.4 IP plugin 注册 Ethernet types

Ethernet owner只提供generic `(u16 EtherType, NodeId consumer)` registration。IPv4/IPv6
映射由`hammer-plugin-ip`在既有`ip_feature_init` main-loop-enter callback中声明，因为此时
所有graph nodes已经materialize，而Data Workers尚未启动。目标调用形状是：

```rust
fn ip_feature_init(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let nodes = main.nodes();
    let ethernet = EthernetMain::global()?;
    let ip4_input = nodes
        .node_by_name(Ip4InputNode::NODE_NAME)
        .expect("IP4 input node exists after graph materialization");
    let ip6_input = nodes
        .node_by_name(Ip6InputNode::NODE_NAME)
        .expect("IP6 input node exists after graph materialization");

    ethernet.register_input_type(nodes, 0x0800, ip4_input)?;
    ethernet.register_input_type(nodes, 0x86dd, ip6_input)?;
    // Existing IP Feature Arc declarations continue here.
}
```

这是调用合同伪代码，不要求保留注释或局部变量拼写。两个EtherType常量应是IP plugin私有
domain constants，不能加进service。`Ip4InputNode`/`Ip6InputNode`已经由同一plugin的
`#[graph_node]` inventory声明；在graph materialization成功后仍查不到它们表示plugin
registration bug，因此本地`expect`，不新增`IpInputNodeMissing` recoverable error。

`EthernetMain::global()`和`register_input_type`的现有`RuntimeError`使用`?`原样传播；不包
成`FeatureError`或字符串错误。随后现有`register_ip_features`产生的`FeatureError`仍只在
既有`GraphNodeInitialization` startup seam转换一次。callback明确声明
`runs_after = ["device_input_feature_init"]`和`runs_before = ["feature_arc_init"]`，确保
Ethernet owner、device arc和consumer nodes都已存在，并且mapping在worker启动前发布。

该ownership逐项对应VPP：`ip4_init`在`ip4_input.c:375-381`注册
`ETHERNET_TYPE_IP4 -> ip4_input_node.index`，`ip6_init`在`ip6_input.c:208-212`注册
`ETHERNET_TYPE_IP6 -> ip6_input_node.index`；两处都由协议owner调用generic
`ethernet_register_input_type`，不是Ethernet owner写死IP next。

## 5. 初始化与注册顺序

目标顺序为：

```text
feature_main_init + ethernet_main_init + device_main_init
  -> materialize all service/plugin graph nodes
  -> resolve named static nexts and graph siblings
  -> device_input_feature_init
       register device-input arc
       publish FeatureMain.device_input_feature_arc_index
       register ethernet-input as last feature
  -> protocol startup callbacks
       EthernetMain::register_input_type(ether_type, consumer_node)
       register each plugin's own Feature Arcs
  -> feature_arc_init
  -> start_workers
```

`device_input_feature_init` 是 main-loop-enter callback，`runs_before =
["feature_arc_init"]`。IP plugin 复用现有 `ip_feature_init`，并声明
`runs_after = ["device_input_feature_init"]`、`runs_before = ["feature_arc_init"]`。
它先执行：

```text
0x0800 -> Ip4InputNode
0x86dd -> Ip6InputNode
```

然后继续注册既有IP Feature Arcs。这两项常量和 concrete node imports 只存在于 IP
plugin。service 的 next enum、`EthernetMain`、parser 和 tests 不含
`ip4-input`/`ip6-input` 的静态 edge。其它 plugin在自己的startup callback中调用同一
generic API。

`EthernetMain::register_input_type(nodes, ether_type, consumer)` 的 mutation 顺序是：

1. main-thread/pre-worker lifecycle assertion；
2. `NodeMain::add_node_next_slot(ethernet_input, consumer)`校验consumer并解析/复用slot；
3. graph operation 成功后，把返回的 `u16` slot插入或替换`SparseVec` mapping。

步骤1-2失败时registry不变；步骤3使用已验证16-bit key和现有infallible
`SparseVec::insert`，发布后没有第二个fallible operation。因此不需要rollback guard。
多个 EtherType 指向同一 consumer 时复用同一 graph slot，但保留独立 sparse entries。

同一 EtherType 再次注册时以新 consumer slot 替换旧 mapping，旧 graph edge可以保留供
其它 EtherType 使用。这与 VPP `ethernet_register_input_type` 重写
`ethernet_type_info_t.node_index/next_index` 后调用 `next_by_ethertype_register` 的行为
一致；不新增 duplicate error。不存在 `EthernetError`、`Exhausted`、`Other`、字符串
invariant 或“注册表已安装”错误。

## 6. Packet path

### 6.1 Device feature start

Hammer 对 `vnet_feature_start_device_input` 的领域名称是
`FeatureMain::start_device_input`：

```rust
#[inline(always)]
pub fn start_device_input(
    &self,
    sw_if_index: u32,
    buffer: &mut Buffer,
    default_next: u16,
) -> u16;
```

它只读取缓存的 device arc index并委托现有 `start_feature_arc`：

- 该 interface 没有 enabled device features 时，返回 `default_next`，不改
  `current_config_index`；
- 有 features 时，按 `sw_if_index` 选 `FeatureConfigMain` entry，写 Buffer config
  cursor并返回第一个 feature next；
- 链末固定到 `ethernet-input`；
- 无 allocation、lock、name lookup、error construction 或 recoverable `Result`。

真实 RX node 对一个 Ethernet frame 先选择自己的 default slot（通常是
`DeviceInputNext::Ethernet`），写 RX `sw_if_index`，再调用本操作。raw-IP、tunnel 或
其它非 Ethernet ingress 只有在其 owner 明确选择这个 arc 时才调用；本文不把所有
device traffic 强制送到 Ethernet。

### 6.2 Zero-copy Ethernet parser

`ethernet-input` 对每个 Buffer 执行以下顺序：

1. 取得 `buffer.current()` 的 immutable slice；
2. 用 private `#[repr(C)]` `EthernetHeader` 通过 `zerocopy::FromBytes + KnownLayout +
   Immutable` 直接借用前 14 bytes，读取 network-order EtherType；
3. 如果 outer EtherType 是 802.1Q、802.1ad、0x9100 或 0x9200，直接借用随后的
   `EthernetVlanHeader`；与 VPP 一致，只有 outer tag 后的 type 是 802.1Q
   `0x8100` 时才继续解析 inner tag，最多取得两层后的最终 EtherType；
4. 在任何 cursor mutation 前验证所需的 14/18/22 bytes 全部位于第一段当前窗口；
5. 验证成功后结束 header borrows，执行一次 `buffer.advance(header_len)`；
6. `NetworkOpaque.l3_hdr_offset` 设为 0，因为 Hammer 的 `BufferPacketCursor` offsets
   相对 advance 后的 `buffer.current()`；
7. 用最终 EtherType 选择 next。

parser 不返回 header borrow 给 caller，不缓存 raw pointer，不创建观察 wrapper。内部
结果只需要 `(ether_type, header_len)` 两个 scalar；不新增 `Parsed*`/`View` 类型。

“zero-copy”是可测试合同：没有 `Vec`、`Box`、stack payload array、`copy_from_slice`、
packet clone 或新 Buffer；advance 后的 `current().as_ptr()` 必须等于原 pointer 加
`header_len`，payload bytes 原位不变。读取两个 EtherType bytes形成 host-order `u16`
是 scalar decode，不是 packet copy。

如果 L2 header 跨 Buffer chain，首期返回 header-too-short 并 drop；不得为了拼 header
分配临时 bytes。将来若要支持 split L2 header，必须先定义 generic chained-prefix
borrow，不得给 Ethernet 添加私有 copy helper。

### 6.3 VLAN 与 metadata 边界

首期支持 untagged、single-tag 和 double-tag L3 dispatch。两层之后的 type 仍是
802.1Q `0x8100` 时记录 `UnknownVlan` 并 drop，与 VPP 的
“3-or-more/unknown VLAN” 分类方向一致。

首期没有 L2 mode/subinterface consumer，因此 VLAN IDs、L2 start 和 L2 length 不发布
到 `NetworkOpaque`。这不是遗忘字段，而是避免在 40-byte primary opaque 中为未实现的
owner预占布局。将来实现 L2/subinterface 时必须单独对照 VPP 的 `l2_hdr_offset`、VLAN
depth flags、rewind 和 interface classification，并重新审批 opaque ABI；本 ADR 不授权
挤占 `NetworkIpOpaque` 或 `BufferFlags` bits。

### 6.4 Next selection

`EthernetInputNext` 只有固定 `Punt` 和 `Drop`。每个 packet 的选择为：

| 条件 | Node error | Next |
| --- | --- | --- |
| base/VLAN header 不完整 | `HeaderTooShort` | `Drop` |
| 两层之后仍是 802.1Q `0x8100` | `UnknownVlan` | `Drop` |
| final field `< 0x0600`，当前没有 LLC owner | `UnknownType` | `Punt` |
| EtherType 在 `SparseVec` 注册 | `None` | registered dynamic slot |
| EtherType 未注册 | `UnknownType` | `Punt` |

`UnknownType` 与 malformed packet 是预期 per-packet classification，使用
`EthernetInputError: NodeErrorCode` 和 node counter；node process 不为每包返回控制面
`Result` 或格式化字符串。只有 runtime API 本身出现 owner/lifecycle bug时才终止当前
node execution scope。

VPP 对 IP4/IP6/MPLS 的 direct fields 是 sparse registration 的 cache。Hammer 首期
不复制这些 cache，因为它会把具体协议重新写进 generic Ethernet owner。一次
`SparseVec<u16>::get` 是统一 hot path；只有真实 benchmark 证明其成为瓶颈，才能设计
不编码协议名称的通用 cache，并重新审批。

## 7. 同步、错误与生命周期

### 7.1 VPP Ethernet packet errors 与 Rust node errors

VPP 的完整 error enum 由 `third_party/vpp/src/vnet/ethernet/ethernet.h:170-176`
include `error.def` 生成；字符串/action来自
`third_party/vpp/src/vnet/ethernet/error.def:40-45`。packet path在
`node.c:1467-1474, 1663-1667` 先决定next，再把`error_node->errors[error]`写入Buffer；
tag fast path在`node.c:550-589`执行同一分类。

Rust 不复制当前scope永远不会产生的counter。目标映射是：

| VPP error | VPP 产生位置与 next 语义 | 当前 Rust 表达 | Rust next | 对齐状态 |
| --- | --- | --- | --- | --- |
| `ETHERNET_ERROR_NONE` | 正常parse；next由L2/registered EtherType决定 | 没有`None` enum variant；`buffer.clear_node_error()` | registered slot | 语义对齐；Hammer全局error column 0已表示无error，不注册“no error”business counter |
| `ETHERNET_ERROR_BAD_LLC_LENGTH` | `error.def`声明DROP；scoped search `rg "BAD_LLC_LENGTH" third_party/vpp` 未找到vendored producer | 本scope不声明 | 不适用 | LLC延期；不增加永远不产生的variant |
| `ETHERNET_ERROR_UNKNOWN_TYPE` | `determine_next_node` sparse miss，`node.c:268-284`；punt | `EthernetInputError::UnknownType` | `EthernetInputNext::Punt` | 对齐 |
| `ETHERNET_ERROR_UNKNOWN_VLAN` | `eth_identify_subint`无匹配，`ethernet.h:488-529`；error path drop | `EthernetInputError::UnknownVlan`，首期只用于2 tags后仍为`0x8100`的3+分类 | `EthernetInputNext::Drop` | outcome对齐；完整subinterface match producer延期 |
| `ETHERNET_ERROR_L3_MAC_MISMATCH` | `node.c:193-219`等DMAC检查；普通path drop | 本scope不声明 | 不适用 | MAC filtering延期；不增加占位counter |
| `ETHERNET_ERROR_DOWN` | matched subinterface的`sw_if_index == ~0`，`node.c:217-219`；drop | 本scope不声明 | 不适用 | subinterface lifecycle延期；不增加占位counter |
| VPP无对应error | VPP parser在`node.c:70-163`直接解引用，并依赖输入header已连续可读 | `EthernetInputError::HeaderTooShort` | `EthernetInputNext::Drop` | Rust safety差异；malformed external input不能触发`Buffer::advance` panic或越界borrow |

目标 Rust packet error surface 是：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum EthernetInputError {
    HeaderTooShort,
    UnknownType,
    UnknownVlan,
}

impl NodeErrorCode for EthernetInputError {
    fn local_code(self) -> u16 {
        self as u16
    }
}
```

`register_ethernet_input` 同时注册三个 `NodeErrorDescriptor`：

| Rust variant | descriptor name | severity | description | 依据 |
| --- | --- | --- | --- | --- |
| `HeaderTooShort` | `header-too-short` | `Error` | `Ethernet header is too short` | Rust bounds safety差异，next=drop |
| `UnknownType` | `unknown-type` | `Warn` | `Unknown Ethernet type` | VPP `UNKNOWN_TYPE`, action/next=punt |
| `UnknownVlan` | `unknown-vlan` | `Error` | `Unknown VLAN` | VPP `UNKNOWN_VLAN`, action/next=drop |

VPP `error.def` 的第二列是 packet action，不是 Hammer 的统计 severity。上表 severity 是
Hammer `NodeErrorDescriptor` 必须提供的本地元数据；packet disposition只由独立的next列
决定，不能根据 severity 推导。VPP `PUNT`/`DROP` 与 Rust next 的对齐以本节第一张表为准。

每个失败packet只执行以下固定步骤：取得预安装的node-local error index、增加当前worker
counter、写`Buffer.node_error_index`、选择上表next。它不分配，不格式化，不记录日志，
也不返回`RuntimeResult`给main loop。正常packet必须clear旧Buffer error，防止recycled
Buffer继承上一包分类。

### 7.2 注册和初始化错误

VPP注册面没有Ethernet domain error family：

- `next_by_ethertype_register` 的签名是 `clib_error_t *`，但
  `node.c:2174-2218` 的所有路径都返回0；
- `ethernet_register_input_type` 在`node.c:2256-2288`返回`void`；未知
  `ethernet_type_info_t`只warning后return；重复type直接重写node/next mapping；
- `vlib_node_add_next_with_slot` 在`third_party/vpp/src/vlib/node.c:139-203`要求main
  thread、复用existing edge、给siblings安装同slot，并以assert维护slot invariant。

Hammer因此不新增`EthernetError`。目标control contract及两边源码依据为：

| 场景 | VPP行为与源码 | Hammer行为与源码 | Error owner/恢复动作 |
| --- | --- | --- | --- |
| 首次注册 | add next并写sparse mapping；`node.c:2256-2288` | `Ok(slot)`；`NodeMain::add_node_next_slot`在`crates/hammer-runtime/src/node.rs:1849-1889` | 无error |
| 同type/同consumer重复注册 | 重写type info/mapping；`node.c:2275-2285`，existing edge由`vlib/node.c:156-162`复用 | 复用existing graph slot并`Ok(slot)`；`node.rs:959-979` | 无error，幂等 |
| 同type/不同consumer再次注册 | 添加/复用新edge并重写mapping；`node.c:2275-2285` | graph成功后替换mapping并`Ok(new_slot)`；graph mutation在`node.rs:982-1037`先完成 | 无duplicate error；新consumer成为owner |
| 任意未预列出的`u16` type | `ethernet_type_info_t`缺失则warning/return；`node.c:2269-2274` | 接受并注册；`SparseVec<u16>`覆盖完整16-bit key space，见`crates/hammer-infra/src/sparse_vec.rs:7-112` | 有意差异：Hammer没有type-name catalog，generic API不引入catalog rejection |
| consumer `NodeId`无效 | `vlib_node_add_next_with_slot`直接取得node并以main-thread/runtime assertions维护前置条件；`vlib/node.c:148-154` | 现有`RuntimeError::NodeNotRegistered { node }`；定义在`crates/hammer-runtime/src/error.rs:301-304`，校验在`node.rs:865-869, 959-963` | runtime graph owner；修正plugin startup declaration后重启 |
| next slot超过Hammer `u16` | VPP next slot是`uword`，本路径没有对应error；`vlib/node.c:139-167` | 现有`RuntimeError::NodeNextCountOverflow { count }`；`error.rs:271-272`、`node.rs:993-1000` | runtime graph ABI owner；startup失败，不增加Ethernet exhausted variant |
| graph sibling slot不一致 | 三个Ethernet variants用`ASSERT`要求slot相同；`node.c:2276-2282`，generic sibling传播也assert；`vlib/node.c:188-198` | 本scope只有一个Ethernet node，不产生该failure；若未来增加variants，复用现有`RuntimeError::NodeNextSlotMismatch`/owner assertion，见`error.rs:273-281`、`node.rs:942-957` | runtime graph owner；Ethernet不翻译/包裹 |
| `EthernetMain`在startup缺失 | VPP使用process global `ethernet_main`，没有可缺失lookup；`ethernet.h:312-314` | 复用现有`RuntimeError::RuntimeCapabilityMissing`；`error.rs:254-257`，模式与`FeatureMain::global`的`feature.rs:149-156`一致 | startup callback中止启动；不新增`NotInstalled` |
| IP input node在`ip_feature_init`缺失 | VPP static node registration先于`ip4_init`/`ip6_init`，调用点直接使用node index；`ip4_input.c:375-381`、`ip6_input.c:208-212` | graph materialization已成功后的plugin invariant，按4.4本地`expect` | programmer bug；不新增recoverable IP/Ethernet error |
| device arc/feature声明无效 | feature init返回`clib_error_t *`或以assert维护声明不变量；`feature.h:354-360`及feature实现 | existing `FeatureError`具体variant；`crates/hammer-service/src/feature.rs:12-60`，只在现有`feature_arc_init` seam包装`GraphNodeInitialization`，见`feature.rs:1053-1065` | FeatureMain owner；修正startup declaration |

`EthernetMain::register_input_type`直接返回`RuntimeResult<u16>`；它不把
`RuntimeError`包进service-local wrapper，也不使用legacy `RuntimeError::subsystem`。
main-loop-enter callback需要附着node identity时，最多在既有真实runtime seam转换一次为
`RuntimeError::GraphNodeInitialization { node: "ethernet-input", source }`，source chain必须
保留。测试匹配concrete variant/fields和`Error::source()`，不匹配display string。

### 7.3 Programmer invariants 与 publication

- `EthernetMain` 在ordinary init发布空owner；startup callbacks单线程注册；
  `start_workers` 是本文scope的freeze boundary。
- Data Workers只调用`next_for_ethertype`，不能取得或保留`SparseVec` borrow。
- 本scope不支持live registration，因此不进入Worker Barrier，也不增加lock、atomic、
  RCU snapshot、completion counter或refork protocol。VPP `vlib_node_add_next_with_slot`
  可在runtime内自己进入barrier是已记录的生命周期差异，不授权Hammer提前增加live API。
- startup callback查询缺失的`FeatureMain`/`EthernetMain`时，沿用
  `RuntimeCapabilityMissing`并中止启动；workers启动后packet path仍缺失Main、未发布device
  arc index、late registration和packet path读到不属于`ethernet-input`的slot，才是owner
  lifecycle/programmer bugs，使用带identity的本地assert/panic，并由既有node/plugin
  boundary终止execution scope。
- 不定义`NotInstalled`、`RegistrationClosed`、`Invariant(String)`或其它可恢复variant，
  不维护installed/closed bool。
- registration先完成fallible graph edge insertion，再infallibly replace sparse mapping；
  graph失败时旧mapping不变。成功替换后旧edge可继续存在，和VPP graph add语义一致。

## 8. 与 VPP 的三向语义差异

| 主题 | VPP | Hammer 决策 | 理由 |
| --- | --- | --- | --- |
| device-input role | disabled input node，driver nodes siblings | disabled Driver anchor，plugin RX nodes siblings | Hammer `Driver` 是 external input runtime role |
| device static nexts | 固定列出 IP4/IP6/MPLS 等 | 固定 Ethernet/drop/punt，其余 dynamic edge | 防止 service 依赖 plugin nodes |
| device feature helper | free inline function读取global `feature_main` | `FeatureMain::start_device_input` owner method | 复用现有 Rust owner borrow，不创建 global wrapper function |
| arc index | `feature_main.device_input_feature_arc_index` | `FeatureMain` private compact field | 避免 packet-path name lookup和双 owner |
| EtherType map | VPP sparse vector + common protocol caches | existing `SparseVec<u16>` only | 同一注册语义，不硬编码 IP |
| Ethernet variants | ethernet/type/not-l2 三节点共享 slots | 首期只有 ethernet-input | LLC/L2 consumers尚不存在 |
| LLC length | 固定 `llc-input` next | typed unknown + punt | 不注册不存在的 node；后续 LLC ADR补齐 |
| VLAN/subinterface | 完整 0/1/2/3+ tag classification | 0/1/2 tag L3 dispatch；3+ drop | 当前 InterfaceMain无相应 L2/subinterface policy |
| header offsets | VPP 保存绝对 L2/L3 offsets | 当前窗口推进后 L3 offset 为 0 | 保持 Hammer 现有 current-relative `BufferPacketCursor` contract |
| truncated header | VPP依赖调用点保证header连续可读，没有独立error | bounds-first parse，`HeaderTooShort`后drop | Rust不能对外部malformed packet越界borrow或让`Buffer::advance` panic |
| registration lifetime | init-driven，C surface未强制静态 | 明确 startup-only | 当前 plugin graph在worker前冻结；避免无需求的同步设计 |
| duplicate registration | 重写type info和sparse mapping | graph成功后重写mapping，返回新/复用slot | 对齐VPP replacement语义；不增加duplicate error |

这些差异保留 VPP 的 ownership、feature traversal、zero-copy cursor 和 dynamic dispatch
语义，同时服从 Hammer 的 crate dependency、Rust ownership 和 plugin boundaries。

## 9. 测试矩阵

| Test | 层级 | 必须断言 | 证据 |
| --- | --- | --- | --- |
| `device_input_is_disabled_graph_anchor` | service graph integration | node kind Driver、state Disabled、process不产出frame；静态 Ethernet/drop/punt slots可解析 | V1, V4 |
| `device_rx_sibling_shares_input_nexts` | runtime/service integration | synthetic driver sibling初始slots相同；追加owner edge后双方slot/target相同 | V1, H3-H4 |
| `device_input_arc_ends_at_ethernet` | service Feature integration | arc index缓存在FeatureMain；ethernet是last；无feature保留default且cursor不变 | V2-V3 |
| `device_input_feature_selects_first_node` | graph behavior | interface启用一个synthetic feature后先走feature、再走ethernet；另一个interface仍直达default | V2-V3 |
| `ethernet_parser_advances_without_copy` | service unit | untagged/single/double tag分别advance 14/18/22；pointer只增加header len；payload原地址/bytes不变 | V5 |
| `ethernet_parser_rejects_truncation_before_advance` | service unit | 0..13 base lengths、每层截断均返回具体error；cursor/length不变，无panic | V5, H5 |
| `ethernet_input_dispatches_registered_type` | graph behavior | 注册任意非IP EtherType到synthetic consumer并真实enqueue；证明API不是IP专用 | V9-V11 |
| `ip_plugin_registers_ethernet_types` | plugin integration | IP callback注册0x0800/0x86dd；实际Ethernet frames到各自concrete input node；service graph无静态IP edge | V9-V12, H10-H11 |
| `ethernet_unknown_type_punts` | graph behavior | 未注册EtherType记录`UnknownType` counter并到punt | V6, V13 |
| `ethernet_malformed_and_deep_vlan_drop` | graph behavior | truncated记录`HeaderTooShort`；3+ tag记录`UnknownVlan`；均到drop | V5-V6, V13 |
| `ethernet_registration_replaces_mapping` | service control test | 首次注册成功；同type/同consumer复用slot；同type/不同consumer重写mapping并返回新/复用slot | V9-V10 |
| `ethernet_registration_is_failure_atomic` | service control test | invalid node或graph error匹配现有`RuntimeError`具体variant/fields；旧mapping不变；startup seam包装时保留`Error::source()` | V10, H4 |
| `ethernet_registration_freezes_before_workers` | lifecycle integration | callbacks先于feature_arc_init/start_workers；late call触发lifecycle invariant；无installed state | V2, H2 |
| `raw_device_path_can_bypass_ethernet` | graph behavior | device-owned dynamic next可直达synthetic parser，且未调用device Ethernet feature start | V4, V12 |
| `ethernet_packet_path_cost` | release benchmark/codegen | 无allocation、lock、name lookup、payload copy或protocol-specific branch | V5-V10 |

不得用读取 `.rs` 后做 `contains`/regex 的测试证明 service 没有 IP hardcode。该边界通过
crate imports、next table的类型、真实 non-IP registration和 graph behavior验证；review
时可用 `rg` 做辅助 cleanup audit，但不能替代 executable test。

实现完成后的最终 pre-commit gate至少包括：

```text
cargo fmt --all -- --check
git diff --check
cargo test -p hammer-service
cargo test -p hammer-plugin-ip
cargo test -p hammer-runtime
cargo clippy -p hammer-service -p hammer-plugin-ip -p hammer-runtime --all-targets
```

具体 package/target 以实现时 workspace 为准。根据仓库规则，这些命令只在实现、review
和格式化全部完成且下一步就是 commit 时运行；本 ADR-only turn 不运行 Rust tests。

## 10. 变更清单

### Add

- `crates/hammer-service/src/ethernet.rs`：`EthernetMain`、Ethernet header layouts、
  parser、input node、node errors和startup registration；
- service graph tests：device anchor/sibling、feature path、zero-copy parser、dynamic
  EtherType dispatch和failure atomicity。

### Modify

- `crates/hammer-service/src/device/mod.rs`：增加 `DeviceInputNode`/
  `DeviceInputNext` declarations；不改变 `DeviceMain` ownership；
- `crates/hammer-service/src/feature.rs`：缓存 device-input arc index并提供
  `start_device_input`；不改 `FeatureConfigMain` 字段归属；
- `crates/hammer-service/src/lib.rs`：导出 Ethernet module，注册 nodes/init/
  main-loop-enter callback；
- `crates/hammer-plugins/net/ip/src/punt.rs` 或届时更合适的既有 startup module：在
  `ip_feature_init` 中注册 IPv4/IPv6 EtherTypes；
- `crates/hammer-plugins/net/ip/src/lib.rs`：只在 callback symbol移动时更新现有
  registration image；不增加第二个 registration path；
- `CONTEXT.md`：实现后补充 Device Input Arc 与 EthernetMain 术语。

### Delete / Replace

- 不删除现有生产类型；
- 将任何 device plugin 对 Ethernet frame 的临时直接 IP hardcode替换为默认
  Ethernet slot + owner registration，但当前仓库尚无具体 device plugin caller；
- 不保留兼容 helper、duplicate registry 或旧/新双 path。

## 11. 新类型/API 审批

根 `AGENTS.md` 要求非平凡 VPP 工作中的每个新 type/API 在实现前获得明确批准。以下
项目已随本文整体获得实现批准：

| 已批准项 | 最终职责 | 现有 surface 为什么不能满足 | 不采用的替代 |
| --- | --- | --- | --- |
| `DeviceInputNode` | disabled Driver graph/Feature anchor和device RX sibling owner | 当前没有公共 device input node或共享next owner | 让每个driver复制Feature start/next table |
| `DeviceInputNext` | service-owned Ethernet/drop/punt固定slots | sibling需要一个共同初始layout | 硬编码IP/MPLS、device-specific enum |
| `EthernetInputNode` | zero-copy L2 header parse和EtherType dispatch | IP input从L3开始，不能承担Ethernet ownership | device内复制parser、让IP识别Ethernet |
| `EthernetInputNext` | punt/drop固定slots，dynamic consumers随后追加 | unknown/malformed需要稳定owner next | 固定`ip4-input`/`ip6-input` slots |
| `EthernetMain` + `global/init` | 唯一EtherType dispatch owner | DeviceMain/InterfaceMain均不拥有协议分发 | registry service、Arc handle、snapshot、lock |
| `EthernetMain::register_input_type` | startup graph edge + sparse mapping的failure-atomic owner operation；返回现有`RuntimeResult<u16>`；重注册替换mapping | `NodeMain`只知道graph edge，不知道EtherType key | IP专用注册函数、caller直接借用SparseVec、service层错误wrapper |
| `EthernetInputError` | header-too-short/unknown-type/unknown-VLAN node counter | control error不能用于每包分类 | 每包`Result`分配、log-only error |
| private `EthernetHeader` / `EthernetVlanHeader` | `zerocopy::FromBytes + KnownLayout + Immutable`借用的准确14/4-byte packet layout | `TapEthernetMetadata`含bool且是构造metadata，不是packet ABI | stack copy、手写raw pointer cast |
| `FeatureMain.device_input_feature_arc_index` | VPP一致的hot-path compact arc cache | name map lookup不适合packet path；DeviceMain保存会形成错误owner | duplicate index、handle wrapper |
| `FeatureMain::start_device_input` | Hammer版`vnet_feature_start_device_input`，委托generic start | caller自行查arc index会重复lifecycle和hot-path细节 | free global wrapper、复制Feature config逻辑 |
| `device_input_feature_init` | graph存在后声明arc/final feature并发布cached index | ordinary init早于graph materialization | install API、lazy initialization |

现有 `SparseVec<u16>`、`NodeMain::add_node_next_slot`、graph sibling、Feature macros、
`FeatureMain::start_feature_arc` 和 Buffer APIs 已满足需求，不申请新 infra/runtime API。
本文不申请新 allocator、deallocator、heap、wrapper、trait、closure access、sync primitive、
installed state、exhausted error或 Buffer opaque field。

## 12. 实现顺序与 verdict

获得批准后的实现顺序必须保持一个 coherent slice：

1. 在 service 注册 `EthernetMain`、device/ethernet nodes和静态 nexts；
2. 声明 device-input arc及ethernet final feature，发布唯一 cached arc index；
3. 实现 bounds-first zero-copy parser与typed node errors；
4. 实现 startup-only generic EtherType registration；
5. IP plugin注册IPv4/IPv6 EtherTypes，不让service引用其nodes；
6. 增加完整 graph/parser/lifecycle tests并更新CONTEXT；
7. review确认没有L2占位状态、runtime mutation、IP hardcode、copy parser或重复registry；
8. 只在commit candidate完成后执行第9节final gate并立即commit。

证据、用户要求和设计推断的边界如下：

- **仓库事实**：FeatureMain、SparseVec、Buffer cursor、graph siblings和dynamic next API
  已存在；device/ethernet input path不存在。
- **VPP事实**：device-input是disabled sibling anchor和Feature Arc head；Ethernet是arc
  末端；EtherType registration建立graph slot再发布sparse mapping；common protocol fields
  只是cache。
- **用户要求**：引入两个nodes和arc；提供device-input feature-start语义；parser
  zero-copy；next参考VPP且可注册；service不得写死`ip4-input`；先写ADR、不改代码。
- **设计推断**：首期startup-only registration、L3 dispatch和current-relative L3 offset
  是对现有Hammer lifecycle/opaque合同的最小完整接入；L2/LLC/runtime mutation明确延期，
  不能由实现者擅自补成占位surface。

**Design verdict: Aligned and approved.** 目标设计对齐 VPP 的 graph ownership、
Feature Arc start、zero-copy cursor和注册式next选择，同时消除VPP静态IP next在Hammer
service层造成的反向依赖。

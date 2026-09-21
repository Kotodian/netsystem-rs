# ADR-0030: VPP 风格 interface、Ethernet、IP output 与 tuntap 图注册

Status: accepted

Date: 2026-09-20

本文只定义设计，不包含 Rust 实现。目标是按 vendored VPP 的真实 owner 和调用链完成以下工作：

- 从 `InterfaceMain` 删除全部 DPO 状态、操作与 class registration；
- 将 generic registration 唯一命名为 `InterfaceMain::register_interface`；
- 将 Ethernet registration 唯一命名为 `EthernetMain::eth_register_interface`；
- 补全 `interface-output` Feature Arc、`vnet_if_update_lookup_tables` 的 Hammer 对应语义；
- 在 IP plugin 中分别建立 `ip4-output`、`ip6-output`，两者的 last feature 都是公共
  `interface-output`；
- 明确普通 adjacency、公共 `interface-output` fanout、per-interface output 和显式
  interface-TX DPO stack 的入口场景，不把它们混成一条默认路径；
- 注册并实现 `tuntap-rx`、`tuntap-tx`，并让 DeviceClass 的 `tuntap_intfc_tx` 驱动通用注册动态
  生成的 `<name>-tx` 节点；
- 让 tuntap 的 startup config 可选配置一个 IPv4 prefix，并在 interface registration 完成后直接调用
  IP plugin owner 的现有 address operation；
- 用现有独立 ICMP plugin闭合TUN上的真实IPv4 echo request/reply，不把ICMP responder塞进
  tuntap、IP plugin或generic net owner。

本文取代以下旧结论：

- ADR-0004 中 `register_hardware_interface` 命名、registration 返回 `InterfaceResult`、callback
  error 回滚以及 output/tx node 尚未确定的部分；
- ADR-0005 中把 receive、interface-RX、interface-TX DPO 的 pool、DB、operations、class
  registration 或 fallback node 放进 interface owner 的全部内容；
- ADR-0025 中 Feature Arc 不允许空 start list、start list 构建后不可追加的结论；
- ADR-0026 中 `EthernetMain` 不拥有 Ethernet interface pool 的延期结论；
- ADR-0027 中“不设置 tuntap DeviceClass TX function”“不注册 RX/TX nodes”“不注册 FileMain”
  “不创建动态 output/tx nodes”和 `tuntap_phase_one_has_no_packet_path` 的结论。
- ADR-0027 中 tuntap config 拒绝 address 字段的结论；本 ADR 只批准 `ip4_address`，不顺带扩张为
  generic `IpAddr`、IPv6 或 service-owned address config。

其它 ADR 已决定的 class inventory、EtherType input dispatch、IP interface address、FIB、Linux
provisioning syscall 和 worker barrier 规则继续有效。本文不借 registration 重构增加新的控制面错误。

## 1. 决策边界

### 1.1 固定名称

目标 API 只有以下两个 registration 名称：

```rust
InterfaceMain::register_interface(...) -> u32
EthernetMain::eth_register_interface(...) -> u32
```

目标设计中不存在 `register_hardware_interface`、`register_device_interface`、
`EthernetMain::register_interface`、兼容 alias 或第二套 registration wrapper。

用户口语中的“ip46-output”在本文只表示 IPv4/IPv6 output 这一组能力。VPP 没有名为
`ip46-output` 的 Feature Arc，Hammer 也不得创建合并 arc。实际名称只能是 `ip4-output` 和
`ip6-output`。

用户口语中的“tuntap-infc-tx node”对应 VPP 函数 `tuntap_intfc_tx`。它不是第三个静态 Graph
Node。静态节点只有 `tuntap-rx`、`tuntap-tx`；通用 interface registration 使用
`tuntap_intfc_tx` 作为 DeviceClass TX process，动态创建 `tuntap-0-output` 和
`tuntap-0-tx`。

### 1.2 本文决定

1. DPO、interface、Ethernet、IP plugin 和 tuntap plugin 的最终 owner；
2. `register_interface` 与 `eth_register_interface` 的输入、返回值、mutation 顺序和 callback
   语义；
3. 公共 output、per-interface output/tx、Feature Arc、arc-end 和 lookup table 的完整 packet
   path；
4. `ip4-output`、`ip6-output` 的声明、arc index storage、rewrite start 和 adjacency cache 更新；
5. Linux TUN static node、DeviceClass TX process、File readiness、唯一 generic interface
   registration 分支和startup IPv4 address；
6. exact packet errors、control-plane errors和programmer invariant；
7. 类型/API inventory 与唯一可执行startup ICMP example。

### 1.3 非目标

- 不设计 TAP、Ethernet frame RX/TX、ARP、Ethernet bridge/BVI、VLAN classification、secondary
  MAC 或完整 L2 output；`eth_register_interface`仍作为独立Ethernet registration API完成设计，但
  本轮tuntap不调用它，留给后续TAP/device slice；
- 不实现当前不存在的 `ip4-midchain`、`ip6-midchain`、`ip4-dvr-dpo`、`ip6-dvr-dpo` 占位节点；
- 不增加 DPO class、DPO error、interface registration error 或 tuntap TX error；
- 不设计 tuntap RX/TX/interface counter 的存储、更新或 stats publication；本轮只完成
  descriptor readiness、packet ownership 和 graph dispatch；
- 不新增 tuntap packet-path error family；startup IPv4 address 只保留 IP owner 已有
  `IpInterfaceAddressError` 作为 source；
- 不在 service 中引入 IP plugin type、arc index 或 callback；
- 不创建 host TUN/TAP，也不在本地运行 lab。

## 2. 第一原则：InterfaceMain 中没有 DPO

### 2.1 必须删除的内容

`InterfaceMain`、`InterfaceState`、`HwInterface`、`SwInterface` 和 interface graph initializer 中
不得出现以下任何内容：

- `DpoId`、`DpoType`、`DpoProto`、`DpoError` 字段或 import；
- receive DPO pool；
- interface-RX DPO pool及 `(protocol, sw_if_index)` DB；
- interface-TX DPO storage 或 fallback node；
- DPO format、lock、unlock、create、query、next resolver；
- receive、interface-RX、interface-TX DPO class registration。

当前 DPO 出现在 interface 中的直接原因是
`crates/hammer-service/src/interface.rs::register_interface_output_graph` 同时注册了公共 Graph Node、
interface-TX DPO class，并把公共 `interface-output` 写成 TX DPO fallback。这是 Hammer 当前实现的
owner 混合，不是 VPP 语义，迁移后整段 DPO registration 从该 initializer 删除。

### 2.2 最终 owner

| 状态或操作 | 最终 owner | interface 可以提供的唯一事实 |
| --- | --- | --- |
| receive DPO pool、引用计数与operations | `NetMain` / `hammer-service::net::dpo` | 无 |
| interface-RX pool、protocol/interface DB与operations | `NetMain` / `net::dpo` | lifecycle callback需要的普通interface identity |
| interface-TX operations/class registration | `NetMain` / `net::dpo` | 由`sw_if_index`解析专属output node |
| interface-TX object pool | 不存在 | DPO identity的index就是`sw_if_index` |
| per-interface output/tx NodeId与next slots | `InterfaceMain` | 直接拓扑查询，不返回DPO |

interface-TX next resolver 从 `DpoId.index` 取得 `sw_if_index`，调用
`InterfaceMain::tx_node_index_for_sw_interface`。该查询通过 software interface 的 super hardware
interface 返回 `HwInterface.output_node_index`。它不读取公共 `interface-output`，不读取
`output_node_next_index`，不构造 `DpoId`，也不返回 `Option` 或新 error。live interface-TX DPO 必须
指向具有 output node 的 live interface；违反该前置条件是 DPO/interface lifecycle bug。

interface-TX DPO 不是普通 adjacency rewrite 的默认出口，也不是一个 Graph Node。它只在 DPO owner
显式构造并 stack 该 DPO 时，把“通过指定 `sw_if_index` 发送”适配成 child next node；stack 完成后
packet path 使用预先建立的 next slot，不在每包路径重新调用 resolver。vendored VPP 中
`interface_tx_dpo_add_or_lock` 的唯一构造调用点是 PPPoE：PPPoE midchain 把 `adj-midchain-tx`
stack 到底层 encapsulation interface 的 `<encap-if>-output`。其它 midchain 可以 stack FIB 贡献的
DPO，不得因为它们也是 midchain 就一律构造 interface-TX DPO。

普通 neighbor 和 multicast adjacency 必须像 VPP 一样，在 rewrite 初始化时直接取得目标 interface
的 output node 并建立 next。Hammer 当前 `AdjacencyRewrite::init` 为每个普通 adjacency 构造
`DpoId::interface_tx` 的做法必须删除；这不是 interface-TX DPO 的合法默认用途。

`HwClass.update_adjacency` 保留在 hardware class，因为 VPP 也把“由硬件类型更新 adjacency”定义为
interface-class capability；其 adjacency 参数必须是 opaque `u32 adj_index`，不是 `DpoId`。调用该
callback 的 adjacency owner 解释 index，`InterfaceMain` 不保存或解析 DPO identity。

DPO module 可以订阅普通 interface lifecycle callback，但 subscriber 身份不转移 DPO ownership。
本设计不为对称性增加无行为的 callback。

## 3. 源码证据与当前差异

### 3.1 Vendored VPP 证据

| ID | VPP 路径与symbol | 已核实行为 | 设计约束 |
| --- | --- | --- | --- |
| V1 | `third_party/vpp/src/vnet/interface.c:820-1060`, `vnet_register_interface` | 返回`u32`；分配hw/sw；可创建动态output/tx；最后调用create callbacks | Hammer registration返回`u32`且顺序一致 |
| V2 | `interface.c:883-884` | DeviceClass无TX function/registrations仍注册成功，不创建output/tx | no-TX不是error，graph fields表达absence |
| V3 | `interface.c:886-1045` | deleted node pair只按相同device class复用；新建`<name>-tx`、`<name>-output`并复制Feature next layout | recycle key和node slots必须一致 |
| V4 | `interface.c:992-1024` | TX slot 0到drop；output slot 0到drop、1到TX；output只有down/deleted/no-queue三项error | 不增加registration或packet error |
| V5 | `interface.c:1025-1052` | output加入`interface-output` starts；arc-end增加到TX的next；随后更新lookup table | start、arc-end和lookup publication属于一次registration |
| V6 | `interface.c:513-526`, `vnet_if_update_lookup_tables` | 发布按`sw_if_index`索引的`hw_if_index_by_sw_if_index`和`if_out_arc_end_next_index_by_sw_if_index` dense vectors，未安装值为`~0` | InterfaceMain以一个成对的dense entry表达这两个VPP字段 |
| V7 | `interface.c:528-619` | 每次software interface创建都更新表；subinterface解析到super hardware | subinterface复用super hardware topology |
| V8 | `interface.c:1054-1060` | create helper的callback error不进入registration返回值，也不触发rollback | 不保留当前callback rollback |
| V9 | `interface_output.c:664-750,1232-1416` | 公共output按每个Buffer TX interface分发；arc-end再按TX interface分发到正确TX | 两级fanout不可合并 |
| V10 | `interface_output.c:1358-1380` | `interface-output` arc初始start list为空，last为`interface-output-arc-end` | Feature owner必须支持零start和late append |
| V11 | `dpo/dpo.c:584-610`, `dpo_module_init` | receive、interface-RX、interface-TX均由DPO module注册 | interface initializer不得注册DPO |
| V12 | `dpo/interface_rx_dpo.c`, `dpo/receive_dpo.c` | 两类对象pool、DB、lock/unlock都在各自DPO module | pools从InterfaceMain移走 |
| V13 | `dpo/interface_tx_dpo.c:9-77`; `adj/rewrite.c:56-61` | interface-TX无pool，identity包装`sw_if_index`，next resolver返回super hardware output node | 只向interface查询topology fact |
| V14 | `interface.h:560-562` | `HwClass.update_adjacency`接收`u32 adj_index` | callback不泄露`DpoId` |
| V15 | `ethernet/ethernet.h:543-554`; `ethernet/interface.c:339-374` | Ethernet registration输入为VPP registration struct，返回`u32`，EthernetMain先分配pool再调用generic registration | Hammer字段和owner直接对应 |
| V16 | `ethernet/interface.c:359-372`; `ethernet/init.c:48-62` | min=64、default overhead=22、MTU=9000、default max=9022，MAC写入两处 | 采用相同zero-as-default与publication |
| V17 | `ip/ip4_forward.c:981-1016`; `ip/ip6_forward.c:619-644` | 两条独立output arcs，starts为rewrite/midchain/dvr，last为`interface-output` | 不创建`ip46-output` |
| V18 | `ip4_forward.c:2182-2221`; `ip6_forward.c:1898-1925` | rewrite默认沿adjacency next；仅`HAS_FEATURES`时按TX interface和cached config启动output arc | Hammer不能每包无条件启动Feature Arc |
| V19 | `adj/adj.c:396-455,632-647` | Feature update callback遍历该interface的adjacencies，更新feature flag与config index cache | IP adjacency owner维护缓存，不放进InterfaceMain |
| V20 | `unix/tuntap.c:137-215` | `tuntap-tx`写完整Buffer chain并释放buffers；short write只影响本次发送且不重试；VPP同时更新TX counter并打印warning；node无next和node errors | 注册一个静态internal node；保留consume/no-retry disposition，但按Hammer Node契约不复制warning；counter按本文非目标延期 |
| V21 | `unix/tuntap.c:229-400` | `tuntap-rx`是`device-input` sibling的interrupt input node；TUN按版本到IP4/IP6，未知类型drop；read-ready只置pending；非EAGAIN read error返回0但VPP打印warning | RX next layout复用device-input；保留恢复候选buffers并返回0的disposition，但按Hammer Node契约不复制warning；error counter明确延期 |
| V22 | `unix/tuntap.c:462-505,610-648` | `have_normal_interface`分支调用Ethernet registration；默认TUN/punt branch调用generic registration并置link/admin up；最后统一注册file read callback | 本轮只取TUN framing、generic registration和File时序；不引入TAP、Ethernet或OS punt/inject双模式 |
| V23 | `unix/tuntap.c:956-975` | `tuntap_intfc_tx`是DeviceClass TX function；normal时调用`tuntap_tx`，默认模式释放frame buffers | 不注册名为`tuntap-infc-tx`的静态node |
| V24 | `unix/tuntap.c:64-72,237-315` | RX buffer cache和iovecs按runtime thread分区，readv直接构建Buffer chain | 固定per-thread entries，无lock/thread-local |
| V25 | `adj/adj_nbr.c:311-314`; `adj/adj_mcast.c:63-66`; `adj/rewrite.c:56-72` | 普通neighbor/multicast调用`vnet_rewrite_init(vnm, sw_if_index, link_type, this_node, next_node, rw)`；该函数同一次初始化写`rw->sw_if_index`、source-local next和该interface/link的L3 MTU，target是`vnet_tx_node_index_for_sw_interface`返回的`<name>-output` | Hammer rewrite init必须显式接收`sw_if_index`和MTU类别并写三项事实；普通adjacency不得借interface-TX DPO建next |
| V26 | `plugins/pppoe/pppoe.c:174-243,400-401`; `adj/adj_midchain.c:89-156,167-196,369-388`; `adj/adj_midchain_node.c:20-178` | vendored tree唯一interface-TX构造点是PPPoE；virtual interface的L3 output end改为`tunnel-output`，midchain TX再沿stacked DPO到underlay output | interface-TX只用于显式DPO stack，不替代protocol/output arcs |
| V27 | `interface_output.c:669-758`; `ip/punt_node.c:638-653`; `ip6-nd/ip6_nd.c:362`; `arp/arp.c:704` | 公共`interface-output`供只携带TX `sw_if_index`的producer按packet分发到专属output，也作为IP output arc默认last | 公共fanout不是普通adjacency的默认direct next |
| V28 | `ip4_forward.c:2598-2610`; `ip6_forward.c:2178-2186` | `ip4-rewrite`/`ip6-rewrite`的固定next包含drop、ICMP error和fragment；fragment目标是`ip4-frag`/`ip6-frag` | rewrite node必须拥有独立的协议next layout，不能与ARP/ND node共用只有drop/punt的enum |
| V29 | `unix/tuntap.c:610-648`; `vppinfra/file.h:81-123` | tuntap config在interface创建后立即注册read callback；File owner删除registration时关闭descriptor | Hammer在tuntap config末尾调用`FileMain::add`，exit先停止read interest再结束descriptor lifecycle |
| V30 | `ip-neighbor/ip4_neighbor.c:130-294`; `ip-neighbor/ip6_neighbor.c:106-287` | IPv4 ARP/glean固定next只有`ip4-drop`；IPv6 ND/glean固定next为`ip6-drop`与`ip6-rewrite-mcast`；它们不复用IP rewrite next enum | resolution与rewrite从Graph registration开始就是不同layout |
| V31 | `adj/adj_mcast.c:14-66,370-475`; `ip4_forward.c:2614-2628`; `ip6_forward.c:2189-2205` | multicast adjacency使用独立`DPO_ADJACENCY_MCAST`并进入`ip4-rewrite-mcast`/`ip6-rewrite-mcast`，两个node是普通rewrite的siblings | multicast不能伪装成普通neighbor adjacency或复用普通rewrite NodeId |
| V32 | `unix/tuntap.c:610-640`; `interface_cli.c:897-923`; `ip/ip46_cli.c:144-159` | VPP把interface admin state与IP address作为两个独立control operations；tuntap的generic branch显式置link/admin up，Ethernet branch只完成registration | Hammer TUN startup分别调用interface owner和IP owner；address不能暗含admin-up，且不借Ethernet branch实现L3 TUN |
| V33 | `ip/icmp4.c:109-188,508-563`; `ip/ip4_forward.c:1846` | `ip4-local`把protocol 1送到`ip4-icmp-input`；ICMP owner再按type表分派，未注册type默认punt | IP local protocol dispatch和ICMP type dispatch是两层owner，不得把echo处理写进IP local或tuntap |
| V34 | `plugins/ping/ping.c:295-500,1588-1596` | ping plugin把ICMPv4 type 8注册到`ip4-icmp-echo-request`；reply原地改type/checksum、交换IPv4地址、重置TTL/fragment id、标记locally-originated，再到`ip4-load-balance` | example必须加载Hammer现有ICMP plugin并经其echo request node返回`ip4-lookup`，不能用`ip4-icmp-error`冒充responder |
| V35 | `ip/ip4_forward.c:412-470`; `interface.c:1327-1335` | P2P interface prefix使用zero-next-hop neighbor path，不需要ARP；TUN packet保持L3 framing | TUN example使用generic P2P HwClass与`/31` peer route，rewrite data为空，不能引入Ethernet/ARP旁路 |
| V36 | `unix/tuntap.c:675-688,804-817` | tuntap IP address callback在`have_normal_interface`时立即返回，只在punt/inject模式把VPP地址镜像成Linux alias | 本轮TUN不注册punt/inject address-sync callback；`ip4_address`只配置Hammer IP owner，host peer地址由CI单独配置 |
| V37 | `adj/rewrite.h:20-105`; `adj/adj.c:49-100`; `adj/adj_dp.h:60-91` | rewrite header直接包含一字节flags并紧邻data；debug allocation先以`0xfe` poison整个adjacency，再只初始化`sw_if_index`/flags等基础字段；具体rewrite consumer解释flags | Hammer保持12-byte header和128-byte连续rewrite区域；确定性poison替代C debug-only未初始化内存；IP-specific flag解释不进入service |

本地 vendored tree 没有针对这两个 registration 函数的独立 failure test。第13节只用真实startup
example覆盖主路径，不据此发明 VPP 没有的registration错误。

### 3.2 当前 Hammer 差异

| ID | 当前源码 | 差异 |
| --- | --- | --- |
| H1 | `crates/hammer-service/src/interface_model.rs` | `register_hardware_interface -> InterfaceResult<u32>`，callback error会rollback |
| H2 | `interface_model.rs` | InterfaceMain持有receive/interface-RX pools与interface-TX resolver；`UpdateAdjacency`接收`DpoId` |
| H3 | `crates/hammer-service/src/interface.rs` | 公共output initializer注册interface-TX DPO并保存公共fallback |
| H4 | `interface_model.rs` | 没有per-interface output/tx pair、公共output next、arc-end next和成对dense lookup table |
| H5 | `crates/hammer-service/src/feature.rs` | 拒绝空start list，start storage不可追加，构建后不能加入per-interface starts |
| H6 | `crates/hammer-service/src/ethernet.rs` | EthernetMain只有EtherType input dispatch，没有interface pool与registration |
| H7 | `crates/hammer-plugins/net/ip/src/lookup.rs` | `IpLookupMain`没有output arc index |
| H8 | `crates/hammer-plugins/net/ip/src/adjacency.rs` | rewrite直接返回`adjacency.next_index`，未启动IP output Feature Arc |
| H9 | `crates/hammer-plugins/device/tuntap/src/lib.rs` | plugin graph inventory为空，DeviceClass无TX function，未注册FileMain，只有generic旧registration分支 |
| H10 | `crates/hammer-plugins/net/ip/src/punt.rs::ip_feature_init` | IP plugin注册EtherType input与Feature Arcs，但尚未注册`device-input -> ip4-input/ip6-input` next |
| H11 | `crates/hammer-service/src/net/rewrite.rs::AdjacencyRewrite::{new,init}` | `new(sw_if_index, mtu)`预先写interface/MTU，`init(main, adjacency_node, proto)`却不接收`sw_if_index`，并为每个普通adjacency构造interface-TX DPO再stack；VPP的rewrite init在一个调用中接收interface/link并直接建立到目标`<name>-output`的next |
| H12 | `crates/hammer-plugins/net/ip/src/adjacency.rs:751-892` | `ip4-glean`、`ip4-arp`、`ip4-rewrite`（以及IPv6对应节点）共用只有drop/punt的next layout；rewrite只读取complete adjacency、做粗略长度判断和prepend，缺少TTL/Hop Limit、完整MTU分支、ICMP/fragment next、output Feature Arc启动和明确的设备TX闭环 |
| H13 | `crates/hammer-plugins/device/tuntap/src/lib.rs:62-85,116-330,574-651` | `TuntapMain`只保留raw fd和interface facts；config不向FileMain转移read interest，plugin没有RX/TX node、per-thread buffer cache或next resolution hook；exit直接close raw fd | 本文必须给出File ownership、read-ready、RX buffer chain、next slot和shutdown的完整迁移 |
| H14 | `crates/hammer-service/src/net/adj.rs:178-190`; `crates/hammer-plugins/net/ip/src/adjacency.rs:718-737` | 当前只有neighbor/incomplete/glean三类adjacency DPO binding，没有multicast DPO class或rewrite-mcast siblings | target不能继续把multicast写成普通`DPO_ADJACENCY -> ipX-rewrite` |
| H15 | `crates/hammer-plugins/net/icmp/src/{lib.rs,icmp.rs,protocol.rs}` | 独立ICMP plugin已经注册IPv4 protocol consumer、type 8 consumer并实现echo reply后回到`ip4-lookup`；但`tuntap`的`load_after = ["ip"]`不会传递加载ICMP sibling plugin | example必须把`icmp`列为root plugin；本ADR不重复设计或搬迁已有echo responder |
| H16 | `crates/hammer-service/src/net/rewrite.rs`; `crates/hammer-service/src/net/adj.rs` | 当前`AdjacencyRewrite`把VPP的header/data扁平为一个值，`Adjacency`只有`rewrite`字段；VPP `ip_adjacency_t`直接拥有`rewrite_header`和`rewrite_data` | 删除扁平aggregate，恢复`Adjacency.rewrite_header`/`rewrite_data`及128-byte rewrite区域 |

## 4. InterfaceMain::register_interface

### 4.1 API 与输入

```rust
pub fn register_interface(
    &self,
    main: &mut DataPlaneMain,
    dev_class_index: u32,
    dev_instance: u32,
    hw_class_index: u32,
    hw_instance: u32,
) -> u32;
```

该函数是 generic hardware interface 的唯一创建入口。它不接收 Ethernet registration，不构造
DPO，不返回 `InterfaceResult`。无效class index、错误线程、重复Graph Node identity或破坏publication
scope表示owner/programmer bug，使用现有runtime assertion/panic边界，不能转换成新的registration
error。

### 4.2 固定 mutation 顺序

`register_interface` 必须按以下顺序完成：

1. 从hardware pool取得slot，初始化`hw_if_index`、default RX mode、rate与公共frame字段；
2. 取得DeviceClass与HwClass，按device formatter、hardware formatter、`hw_class.name + hex(instance)`
   的优先级生成name并写name index；
3. 无callback地创建hardware类型的software interface，建立`hw_if_index`、`sw_if_index`、
   `sup_sw_if_index`关系，把全部L3 MTU初始化为0；
4. 立即调用第6节的`if_update_lookup_tables`。此时尚无TX edge，hardware table有值，arc-end table为
   absence；
5. 写dev/hw class index与instance；
6. 如果DeviceClass同时没有TX function和TX function registrations，跳过全部output/tx工作；
7. 否则从相同DeviceClass的deleted pair中复用，或创建新的`<name>-output`/`<name>-tx` pair；
8. 新NodeId加入`interface-output` Feature Arc starts；复用NodeId已经是start，不重复加入；
9. 增加`interface-output-arc-end -> <name>-tx` next，保存local slot；
10. 再次调用`if_update_lookup_tables`，将真实arc-end next发布到hardware software interface；
11. 安装output header formatter及TX process、trace、error descriptors；
12. 以flags=0先调用software create helper，再调用hardware create helper；hardware callback建立
    `interface-output -> <name>-output` edge并保存公共output local slot；
13. 返回`hw_if_index`。

步骤1至12对外是一次registration。workers运行后，调用者必须持有WorkerBarrier，完成pool、graph、
Feature starts、next slots、lookup tables与callback publication后才release；不再在外层叠加lock、
atomic pointer或第二套completion protocol。

### 4.3 no-TX 分支

无TX function/registrations时，registration仍返回live `hw_if_index`：

- `output_node_index = None`；
- `tx_node_index = None`；
- `output_node_next_index = None`；
- `if_out_arc_end_node_next_index = None`；
- 不追加Feature start，不增加arc-end next；
- create callbacks仍按步骤12执行。

这些 `Option` 是 Rust 对 VPP `~0`/未安装拓扑的表达，不是可恢复错误。

### 4.4 callback 返回语义

software/hardware create helper内部仍可按已有callback contract得到error并停止该callback chain，但
`register_interface` 不观察、不传播、不格式化该error，也不调用delete callback补偿。对象、name、
graph和lookup保持已注册状态，然后返回index。这一行为严格来自VPP `vnet_register_interface`。

如果未来需要调用方观察create callback是否完成，必须另立控制面操作；不能改变本registration的
返回类型。

### 4.5 删除与node复用

删除路径只用于说明registration的对称生命周期：

- 先将interface flags清零并运行VPP对应delete callbacks；
- 清queue、subinterface、software interface、name与lookup entries；
- 具有TX pair时不物理删除Graph Node，而是把per-worker output runtime标记为deleted，重命名并从
  active hardware record解除；
- recycle inventory保存`dev_class_index`与一对NodeId；
- 只有相同DeviceClass可复用，因为TX next/error/function layout由DeviceClass决定；
- 复用时更新所有workers的name、process、trace、errors和runtime identity，保留Graph next layout；
- delete callback error不改变void删除语义，不反向引入registration error。

## 5. Output graph 与 interface-output Feature Arc

### 5.1 节点角色

| 节点 | owner | 生命周期 | 作用 |
| --- | --- | --- | --- |
| `interface-output` | hammer-service | static | 按每个Buffer TX `sw_if_index`分发到hardware output node |
| `interface-output-template`语义 | hammer-service | static process/template | 为每个`<name>-output`提供process/trace/runtime shape，不作为fallback |
| `<name>-output` | InterfaceMain registration | dynamic/recyclable | interface状态、queue与`interface-output` Feature Arc start |
| `interface-output-arc-end` | hammer-service | static | Feature chain结束后按TX interface分发到正确TX node |
| `<name>-tx` | InterfaceMain registration | dynamic/recyclable | 执行DeviceClass TX process |

`<name>-output`表示registration为具体hardware interface创建的专属device output node。它与公共
`interface-output` node不是同一个node。`ip4-output`/`ip6-output`表示Feature Arc，也不是名为
`ip4-output`/`ip6-output`的单一Graph Node。

本文出现的两个`interface-output`必须按上下文区分：不带“Feature Arc”的`interface-output`是
按Buffer TX `sw_if_index`做公共fanout的Graph Node；`interface-output` Feature Arc则是已经进入
某个专属`<name>-output`之后执行的per-interface feature chain。前者的next是`<name>-output`，
后者的last是`interface-output-arc-end`，不能互相替代。

典型路径是：

```text
ordinary adjacency, no IP output feature
    -> <name>-output

ordinary adjacency, with IP output feature
    -> ip4-output/ip6-output feature chain
       -> interface-output (public fanout node)
          -> <name>-output

producer carrying only TX sw_if_index
    -> interface-output (public fanout node)
       -> <name>-output

all arrivals at <name>-output
    -> no interface-output feature: <name>-tx
    -> interface-output feature chain
       -> interface-output-arc-end
          -> <name>-tx

explicit interface-TX DPO stack
    -> resolver: InterfaceMain::tx_node_index_for_sw_interface
    -> stacked graph edge: <selected-name>-output
```

interface-TX DPO 不经过公共 `interface-output`，也不让 `InterfaceMain` 持有DPO。

### 5.2 输出入口场景

| 场景 | VPP packet path | interface-TX DPO | 公共`interface-output` node | `interface-output` Feature Arc |
| --- | --- | --- | --- | --- |
| IPv4/IPv6 neighbor或multicast adjacency，未启用协议output feature | `ipX-rewrite`或`ipX-rewrite-mcast -> <egress-if>-output` | 不使用；对应rewrite node直接缓存专属output next | 不经过 | 到达`<egress-if>-output`后按该interface配置决定是否进入 |
| neighbor或multicast adjacency启用至少一个`ipX-output` feature | 对应rewrite node `-> ipX-output features -> interface-output -> <egress-if>-output` | 不使用 | 作为`ipX-output`默认last，按TX `sw_if_index` fanout | 到达`<egress-if>-output`后独立执行，不能和`ipX-output`合并 |
| ARP/ND reply、punt reinject、L2 output等已经设置TX `sw_if_index`、但source node不缓存专属output next | `producer -> interface-output -> <egress-if>-output` | 不使用 | 作为per-buffer fanout入口 | 到达`<egress-if>-output`后按该interface配置决定是否进入 |
| 任一路径到达专属`<name>-output`且启用egress interface feature | `<name>-output -> interface-output features -> interface-output-arc-end -> <name>-tx` | 不使用 | 不经过；此处的`interface-output`是Feature Arc名称，不是公共fanout node | 使用，arc-end按TX interface选择对应TX slot |

选择标准不是“IP包还是非IP包”，也不是“物理interface还是virtual interface”：source已有具体
`<name>-output` next时直接进入；source只携带TX `sw_if_index`时使用公共fanout；协议output feature
存在时由`ip4-output`/`ip6-output`接管next并进入其per-interface end；只有DPO stack的child明确需要
表达“从这个interface发出”时才构造interface-TX DPO。当前vendored tree最后一种只有PPPoE。

PPPoE、`tunnel-output`和`adj-midchain-tx`不属于当前Hammer实现范围。本表不为它们建立节点、
next、DPO stack或验收场景；V26只作为vendored VPP中interface-TX DPO确有显式consumer的证据，
不是本文的实现承诺。

### 5.3 HwInterface 的四个独立graph facts

| 字段 | 含义 | 消费者 |
| --- | --- | --- |
| `output_node_index: Option<NodeId>` | 专属output或显式custom output | interface-TX resolver、hardware add callback |
| `tx_node_index: Option<NodeId>` | DeviceClass TX node | registration、delete/recycle |
| `output_node_next_index: Option<u16>` | 公共output到hardware output的local slot | `interface-output` |
| `if_out_arc_end_node_next_index: Option<u16>` | arc-end到TX的local slot | lookup publication、arc-end |

四者不得合并、互相fallback或用`NodeId(0)`表达absence。

动态节点的基础slots固定为：

| Source | Slot | Target |
| --- | ---: | --- |
| `<name>-tx` | 0 | `error-drop` |
| `<name>-output` | 0 | `error-drop` |
| `<name>-output` | 1 | `<name>-tx` |

`<name>-output` 只注册 `InterfaceDown`、`InterfaceDeleted`、`NoTxQueue` 三项packet error。它们是
node-local counter/drop classification，不进入 `InterfaceError`。TX node使用DeviceClass自己的
process、trace和error descriptors，不由interface owner包装。

### 5.4 公共 interface-output

公共node逐Buffer执行：

1. 读取TX `sw_if_index`；一个frame可以包含多个interfaces；
2. 经software interface解析super hardware interface；
3. 读取该hardware record的`output_node_next_index`；
4. 按local slot enqueue到专属或owner显式设置的custom output node。

VPP此处不读取 `hw_if_index_by_sw_if_index`。Hammer也不得虚构这张表是公共output的consumer。
live packet缺少software/hardware/output next表示上游破坏生命周期，属于assertion，不增加
`MissingEgressInterface`、`MissingTxNode`或trace-only伪错误。

### 5.5 专属 output process

每个worker的runtime保存`hw_if_index`、hardware `sw_if_index`、`dev_instance`与`is_deleted`，按
以下顺序处理：

1. deleted时整组记`InterfaceDeleted`并drop；
2. software interface非admin-up或hardware非link-up时记`InterfaceDown`并drop；
3. 当前worker没有可用TX queue时记`NoTxQueue`并drop；
4. 查询该hardware software interface的`interface-output`配置；无feature走slot 1，有feature则
   安装config cursor并进入第一个feature；
5. 统计主interface packet/bytes；Buffer TX interface为subinterface时同时统计subinterface。

checksum、VLAN、GSO与多queue hash由现有device/output owner扩展，不得绕过Feature Arc或arc-end。

### 5.6 interface-output-arc-end

arc-end不是固定跳到一个TX node。它必须：

1. 读取frame内全部Buffer的TX `sw_if_index`并按相同index分组；
2. 直接读取`InterfaceMain::interface_lookup_entry(sw_if_index).if_out_arc_end_next_index`；
3. 取得相同super hardware的当前worker queue runtime；零queue只对该组记`NoTxQueue`并drop；
4. 其它组继续按各自slot进入各自`<name>-tx`。

有效packet缺少lookup entry、super hardware或TX slot是publication/lifecycle bug，不逐层回查pool、
不查询DPO、不增加另一个packet error。

### 5.7 Feature Arc 声明和动态starts

`interface-output` arc由hammer-service拥有：

- startup声明允许`start_nodes = []`；
- last/default end为`interface-output-arc-end`；
- `interface-output-arc-end`注册为该arc最后一个feature；
- 第一个有TX的interface registration追加第一个start；
- 后续新NodeId追加一次；recycled NodeId不重复追加；
- 所有starts必须拥有完全相同的local-next slot layout。

Feature owner需要将start storage从不可追加的slice改为owner持有的可追加集合，并提供
`FeatureMain::add_feature_arc_start`。追加新start时，以第一个start为canonical layout，复制slot 2
及之后已经存在的feature targets；slot 0/1仍由interface registration建立。现有config heap words、
config index、引用计数和per-interface配置均不重建、不重编号。

arc没有start时仍可建立feature-to-feature及feature-to-end edges；没有hardware output的interface
不会进入该arc，因此不发明empty-start packet error。删除当前 `FeatureError::EmptyStartNodes`；
arc没有任何feature/last node仍由已有empty-arc错误处理。

### 5.8 Startup 与 live publication

Hammer normal init/config早于static graph materialization，设计分两条路径：

- startup registration可先保留owned dynamic names、runtime和named next intent；static graph与Feature
  declarations materialize后统一解析NodeId、next和starts；
- workers启动后的registration在WorkerBarrier内执行dynamic node创建/复用、所有worker runtime
  publication、Feature start append、arc-end edge、lookup更新和公共output edge，然后请求一次graph
  refork并release。

同一registration不能一半走startup pending、一半直接修改live workers。graph publication错误属于
runtime owner已有typed error或invariant，不翻译成`InterfaceError`，也不改变
`register_interface -> u32`。

## 6. vnet_if_update_lookup_tables 对应语义

### 6.1 owner与存储

`InterfaceMain`直接拥有按 `sw_if_index` 密集索引的成对 entry。不能使用
`Vec<Option<u32>>`、`Vec<Option<u16>>` 或两张可分别缺失的表：VPP 的对应状态是 dense vector
加 `~0` 哨兵，且 hardware 映射与 arc-end next 必须作为同一 software-interface row 发布。

```rust
pub const INVALID_HW_IF_INDEX: u32 = u32::MAX;
pub const INVALID_NODE_NEXT_INDEX: u16 = u16::MAX;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InterfaceLookupEntry {
    pub(crate) hw_if_index: u32,
    pub(crate) if_out_arc_end_next_index: u16,
}

impl InterfaceLookupEntry {
    pub const ABSENT: Self = Self {
        hw_if_index: INVALID_HW_IF_INDEX,
        if_out_arc_end_next_index: INVALID_NODE_NEXT_INDEX,
    };

    pub const fn has_hardware(self) -> bool {
        self.hw_if_index != INVALID_HW_IF_INDEX
    }

    pub const fn has_arc_end(self) -> bool {
        self.if_out_arc_end_next_index != INVALID_NODE_NEXT_INDEX
    }
}

pub(crate) struct InterfaceLookupTable {
    entries: Vec<InterfaceLookupEntry>,
}

impl InterfaceLookupTable {
    pub fn new() -> Self;
    pub fn publish(
        &mut self,
        sw_if_index: u32,
        hw_if_index: u32,
        if_out_arc_end_next_index: u16,
    );
    pub fn clear(&mut self, sw_if_index: u32);
    pub fn entry(&self, sw_if_index: u32) -> InterfaceLookupEntry;
}
```

`InterfaceLookupTable` 不是泛化 wrapper；它是 `InterfaceMain` 内部唯一维护
“software interface -> super hardware + interface-output arc-end slot”成对不变量的 owner type，
不跨 crate 导出。它不增加 handle、lock 或 DPO object。private 方法是唯一写入口：

```rust
impl InterfaceMain {
    fn if_update_lookup_tables(&mut self, sw_if_index: u32);

    pub(crate) fn interface_lookup_entry(
        &self,
        sw_if_index: u32,
    ) -> InterfaceLookupEntry;
}
```

`if_update_lookup_tables` 解析 software interface 的 super hardware interface，把当前
`hw_if_index` 和 `if_out_arc_end_node_next_index` 一起写入该 row。packet path 只读取一个
`InterfaceLookupEntry`；缺少 live hardware 或 next slot 是 publication/lifecycle invariant，不返回
`Option`，也不新增 missing-lookup packet error。

### 6.2 调用时机

1. 每次software interface创建后调用，包括hardware software interface与subinterface；
2. hardware registration建立arc-end到TX edge后，对hardware software interface再次调用；
3. subinterface不创建output/tx pair，其 entry始终复制super hardware事实；
4. interface delete在software slot可复用前把entry写成`InterfaceLookupEntry::ABSENT`；
5. slot复用时必须在任何create callback观察之前写入新super hardware facts。

首次hardware software interface publication允许 `hw_if_index` 已有效而
`if_out_arc_end_next_index == INVALID_NODE_NEXT_INDEX`，第二次publication覆盖为真实 slot。这个
两阶段顺序来自VPP，不得省略首次调用，也不得等到packet path再懒计算。

### 6.3 精确consumer

- `interface-output-arc-end`读取 `InterfaceLookupEntry.if_out_arc_end_next_index`；
- 当前vendored tree只证明第一张表被维护，没有找到本切片内的直接packet consumer；
- 公共`interface-output`解析super hardware并读取hardware record；
- interface-TX DPO resolver解析super hardware并读取`output_node_index`。

因此本文保留成对 entry 以对齐 publication contract，但不虚构第一字段的 packet consumer，不以它
替换上述两条 VPP 路径。

## 7. IP plugin 的 ip4-output 与 ip6-output

### 7.1 ownership与声明

两条arc都由 `hammer-plugins/net/ip` 声明、注册和启动。hammer-service只导出公共
`interface-output` node，不拥有IP arc index，也不依赖IP plugin。

| Arc | 当前可注册starts | VPP完整starts | last feature |
| --- | --- | --- | --- |
| `ip4-output` | `ip4-rewrite` | `ip4-rewrite`, `ip4-midchain`, `ip4-dvr-dpo` | `interface-output` |
| `ip6-output` | `ip6-rewrite` | `ip6-rewrite`, `ip6-midchain`, `ip6-dvr-dpo` | `interface-output` |

Hammer当前不存在midchain与dvr nodes，所以本轮只注册已存在的rewrite start。对应node实现后再加入
同一arc；不得现在注册名称占位、空process或假NodeId。这是有界实现差异，不是创建`ip46-output`
的理由。

`Ip4Main.lookup_main`与`Ip6Main.lookup_main`分别增加`output_feature_arc_index`，由现有
`ip_feature_init`在所有相关Graph Nodes materialize后、通用`feature_arc_init`前写入。IP4和IP6各自
保存自己的index，不共享field或合并owner。

跨 crate 的最小边界只传 topology/graph primitive；IP plugin-owned state 不进入
`hammer-service::net`：

```rust
// hammer-service::net
pub fn tx_node_index_for_sw_interface(
    &self,
    sw_if_index: u32,
) -> NodeId;

// hammer-plugins/net/ip
pub(crate) fn register_ip_output_arcs(
    features: &mut FeatureMain,
    nodes: &NodeMain,
) -> RuntimeResult<(u8, u8)>; // (ip4-output, ip6-output)

pub(crate) fn update_adjacency_output_config(
    sw_if_index: u32,
    protocol: DpoProto,
);
```

上面第一个方法只返回 interface owner 已发布的专属 output topology fact；后两个方法只由 IP
plugin 调用，返回/保存的 arc index 不回写 `InterfaceMain`。`DpoProto` 在这里仅表示 DPO graph
protocol，不是 `hammer-service` 对 IP header 或 IP feature policy 的所有权。

### 7.2 adjacency DPO 到 rewrite，再到设备 TX

#### 7.2.1 DPO 是进入 rewrite 的入口，不是设备出口

VPP 的 `DPO_ADJACENCY` class 对 `DPO_PROTO_IP4` 绑定 `ip4-rewrite`，对 `DPO_PROTO_IP6`
绑定 `ip6-rewrite`（`third_party/vpp/src/vnet/adj/adj_nbr.c:1161-1227`）。Hammer 已经在
`crates/hammer-plugins/net/ip/src/adjacency.rs:718-737` 做了同样的 class/node binding；因此正确的
进入关系是：

```text
ip4-lookup -> selected forwarding DPO -> DPO_ADJACENCY/IP4 -> ip4-rewrite
ip6-lookup -> selected forwarding DPO -> DPO_ADJACENCY/IP6 -> ip6-rewrite
```

这里没有一条静态的 `ip4-lookup -> ip4-rewrite` 或 `ip6-lookup -> ip6-rewrite` next。当前
`crates/hammer-plugins/net/ip/src/lookup.rs:1134-1184` 将 LPM 结果写入
`IpSecondaryOpaque.lookup.forwarding`，然后返回该 DPO 的已发布 `next`；DPO class/protocol edge
就是该 `next` 的来源。DPO identity 只携带 adjacency class、protocol 与 pool index；它不携带
interface-TX DPO，也不负责直接选择设备 TX。

邻居状态必须保持 VPP 的 class 分流：

| 邻居状态 | DPO class | DPO node binding | 是否进入 `ip4/6-rewrite` |
| --- | --- | --- | --- |
| complete neighbor adjacency | `DPO_ADJACENCY` | `ip4-rewrite` / `ip6-rewrite` | 是 |
| multicast adjacency | `DPO_ADJACENCY_MCAST` | `ip4-rewrite-mcast` / `ip6-rewrite-mcast` | 否，进入对应rewrite sibling |
| unresolved neighbor | `DPO_ADJACENCY_INCOMPLETE` | `ip4-arp` / `ip6-discover-neighbor` | 否 |
| connected glean | `DPO_ADJACENCY_GLEAN` | `ip4-glean` / `ip6-glean` | 否 |

Hammer 的 `AdjacencyMain::dpo` 当前只按 `AdjacencyLookupNext` 选择neighbor、incomplete、glean三个
class（`crates/hammer-service/src/net/adj.rs:178-190`）；VPP 的neighbor node arrays在
`third_party/vpp/src/vnet/adj/adj_nbr.c:1168-1216`，multicast class则在
`third_party/vpp/src/vnet/adj/adj_mcast.c:370-475`。因此 unresolved packet进入ARP/ND node，不能先进入
rewrite再在设备output处失败；multicast也不能作为普通neighbor进入`ip4-rewrite`/`ip6-rewrite`。
补齐multicast class与两个rewrite-mcast siblings是本文IP graph目标的前置工作，不增加新的packet或
control-plane error。

ARP/ND 完成后，`AdjacencyNeighborMain::update_rewrite(complete = true)` 在同一 main-thread
publication scope 内完成四件事：更新 rewrite bytes，建立 rewrite node 的 direct next，切换
`lookup_next` 为 `Rewrite`，再通过现有 Fib node back-walk 让引用该 adjacency 的 forwarding DPO
重新发布（当前状态转换位于 `crates/hammer-service/src/net/adj_nbr.rs:164-213`）。workers 看到
complete DPO 后才会沿上面的 class edge 进入 `ip4-rewrite` 或 `ip6-rewrite`。状态仍是 incomplete
时，DPO class 与 node binding 不变，数据包永远不走普通 TX rewrite。

#### 7.2.2 `Adjacency` 必须直接拥有 `rewrite_header` / `rewrite_data`

VPP 的普通 neighbor 与 multicast 在 `vnet_rewrite_init` 时直接传入
`vnet_tx_node_index_for_sw_interface`（`third_party/vpp/src/vnet/adj/adj_nbr.c:311-314`、
`third_party/vpp/src/vnet/adj/adj_mcast.c:63-66`）；该函数返回 software interface 所属
hardware interface 的专属 output node（`third_party/vpp/src/vnet/adj/rewrite.c:56-72`）。

VPP 的 `ip_adjacency_t` 通过 `VNET_DECLARE_REWRITE` 在 adjacency 第二、三条cache line中直接展开
`vnet_rewrite_header_t rewrite_header`和固定容量`rewrite_data`。Hammer当前把header fields和data
扁平放进`AdjacencyRewrite`，再让`Adjacency`只持有一个`rewrite`字段，既丢失了VPP字段边界，也让
IP代码出现`adjacency.rewrite.*`这一层假抽象。目标必须删除`AdjacencyRewrite`，恢复下面的直接布局：

```rust
// hammer-service::net::rewrite；只包含protocol-neutral dataplane facts。
pub const REWRITE_HAS_FEATURES: u8 = 1 << 0;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RewriteHeader {
    pub sw_if_index: u32,
    pub next_index: u16,
    pub data_bytes: u16,
    pub max_l3_packet_bytes: u16,
    pub flags: u8,
    pub dst_mcast_offset: u8,
}

impl RewriteHeader {
    pub const fn poisoned() -> Self;

    pub fn init(
        &mut self,
        main: &mut DataPlaneMain,
        sw_if_index: u32,
        mtu_kind: InterfaceMtuKind,
        source_node: NodeId,
        target_node: NodeId,
    );

    pub fn set_data(&mut self, storage: &mut [u8; 116], bytes: &[u8]);
    pub fn clear_data(&mut self, storage: &mut [u8; 116]);
    pub fn update_mtu(&mut self, interfaces: &InterfaceMain, mtu_kind: InterfaceMtuKind);
}

// hammer-service::net::adj
#[repr(C, align(64))]
pub struct Adjacency<P: FibProtocol> {
    pub node: FibNode,
    pub config_index: u32,
    pub subtype: P::AdjacencySubtype,

    // 第二、三条cache line；与VPP VNET_DECLARE_REWRITE同形。
    pub rewrite_header: RewriteHeader,
    pub rewrite_data: [u8; 116],

    pub delegates: u64,
    pub node_index: u32,
    pub lookup_next: AdjacencyLookupNext,
    pub link: P::Link,
    pub next_hop_protocol: FibProtocolId,
    pub flags: AdjacencyFlags,
    pub padding: [u8; 48],
}

impl<P: FibProtocol> AdjacencyMain<P> {
    pub fn insert(
        &mut self,
        subtype: P::AdjacencySubtype,
        link: P::Link,
        node_index: u32,
        lookup_next: AdjacencyLookupNext,
    ) -> AdjacencyIndex;
}

const _: () = assert!(size_of::<RewriteHeader>() == 12);

// hammer-plugins/net/ip；具体protocol owner证明自己的实例布局。
const _: () = assert!(size_of::<Adjacency<Ip4FibProtocol>>() == 256);
const _: () = assert!(size_of::<Adjacency<Ip6FibProtocol>>() == 256);
const _: () = assert!(offset_of!(Adjacency<Ip4FibProtocol>, rewrite_header) == 64);
const _: () = assert!(offset_of!(Adjacency<Ip4FibProtocol>, rewrite_data) == 76);
const _: () = assert!(offset_of!(Adjacency<Ip4FibProtocol>, delegates) == 192);
```

`RewriteHeader`必须是12 bytes，`rewrite_header + rewrite_data`必须恰好128 bytes；`Adjacency`继续是
4条64-byte cache line。`poisoned()`对应VPP `adj_poison`后立即建立的最小确定状态：写
`sw_if_index = u32::MAX`、`flags = 0`，其余header bytes保持`0xfe`，所以`next_index`、`data_bytes`和
`max_l3_packet_bytes`都是`0xfefe`。`AdjacencyMain::insert`不再接收`sw_if_index`或`mtu`，只分配
adjacency、安装非rewrite控制字段并给header/data填`0xfe`。VPP只在
`CLIB_DEBUG > 0`时poison；Hammer不能保留未初始化Rust值，所以始终写同一确定性pattern，这是初始化
安全适配，不改变已发布packet state。在任何rewrite node读取前，具体adjacency owner必须调用`init`
并调用`set_data`或`clear_data`，后两者把`data_bytes`改为
真实长度。`set_data`/`clear_data`同时维护header length和紧邻storage，不能再通过
`AdjacencyRewrite` clone整个128-byte区域，也不能把rewrite data搬到IP-private `Vec`。

service只拥有`flags: u8`的storage和跨协议通用的`REWRITE_HAS_FEATURES`位，不定义
`FIXUP_IP4_OVER_IP4`、flow-hash fixup或其它IP含义。若后续midchain slice需要这些VPP bits，常量、写入
规则和解释必须定义在IP plugin；不得为了让service理解它们而加入IP enum、IP link type或IP Feature
Arc index。本轮IP plugin只读写`REWRITE_HAS_FEATURES`。

`RewriteHeader::init` 的目标 API 显式接收 `sw_if_index`：

```rust
impl RewriteHeader {
    pub fn init(
        &mut self,
        main: &mut DataPlaneMain,
        sw_if_index: u32,
        mtu_kind: InterfaceMtuKind,
        source_node: NodeId,
        target_node: NodeId,
    );
}
```

这个签名不是为了把interface owner塞进IP adjacency policy；它精确保留VPP
`vnet_rewrite_init`的五个输入事实。
调用前先由IP adjacency owner用`sw_if_index`取得super hardware的`<name>-output` NodeId，并把
`P::mtu_kind(link)`作为protocol-neutral `InterfaceMtuKind`传入。`init`在main-thread publication scope
内固定完成：

1. 校验`sw_if_index`仍对应live software interface，并读取该interface的`mtu[mtu_kind]`；
2. 在graph owner中增加`source_node -> target_node`，取得source-local next slot；
3. 不再有fallible步骤后，一次写入`self.sw_if_index = sw_if_index`、`self.next_index = slot`和
   `self.max_l3_packet_bytes = min(interface_mtu, u16::MAX)`。

不得让`init`从`self.sw_if_index`反查interface，也不得认为`AdjacencyMain::insert`时已经拥有真实
rewrite合同；insert只建立poisoned header和poisoned data。真实interface、next和MTU全部由`init`
同时发布。缺少software interface、target node或next
registration表示adjacency/graph publication invariant被破坏，沿用现有assertion/runtime graph
边界；`init`不新增`RewriteInitError`或返回`Result`。`data_bytes`和`rewrite_data`由随后同一owner
publication中的`set_data`决定，不由`init`伪造。

它不接收 `DpoProto`，不构造 `DpoId::interface_tx`，不调用 DPO stack。普通 complete adjacency 的
`target_node` 是目标 `<name>-output`；`next_index` 保存的是 `ip4-rewrite` 或 `ip6-rewrite` 上到该
专属 output 的本地slot。普通 adjacency 因而不经过公共 `interface-output`，也不需要每包解析
interface-TX DPO。

调用边界必须明确：

- complete neighbor：由 IP adjacency owner 以 `ip4-rewrite` 或 `ip6-rewrite` 为
  `source_node`，显式传入该adjacency key中的`sw_if_index`、`P::mtu_kind(link)`和目标interface的
  `<name>-output`，写入 direct next；
- multicast：由IP adjacency owner以`ip4-rewrite-mcast`或`ip6-rewrite-mcast`为`source_node`，同样
  显式传入自己的`sw_if_index`/MTU类别并direct到目标`<name>-output`；它有独立DPO class，但不因此
  使用interface-TX DPO；
- glean：若 glean node 发送其自己的邻居解析请求，使用同一 direct-output helper 从
  `ip4-glean`/`ip6-glean` 到目标 `<name>-output`；glean 不伪装成 complete rewrite；
- incomplete neighbor：不为原始packet初始化普通L3 rewrite出口；IPv4为新分配ARP probe建立
  `ip4-arp -> <name>-output` direct next，IPv6新分配NS probe进入固定`ReplyTx`。原始packet仍drop，
  不能把它伪装成device TX。neighbor转为complete时，publication把同一adjacency缓存的next替换为
  `ip4-rewrite`/`ip6-rewrite -> <name>-output`的local slot；这个重新初始化仍显式传入该adjacency的
  `sw_if_index`和`P::mtu_kind(link)`，再发布complete DPO。

#### 7.2.3 `ip4-rewrite` / `ip6-rewrite` 的逐包职责

VPP 的实现证据是 `third_party/vpp/src/vnet/ip/ip4_forward.c:2068-2235` 与
`third_party/vpp/src/vnet/ip/ip6_forward.c:1745-1938`。Hammer 两个节点目前只做了 DPO/adjacency
读取、粗略长度判断、header prepend 和 TX metadata 写入
（`crates/hammer-plugins/net/ip/src/adjacency.rs:857-892`），缺少下面的完整闭环；这不是可接受的
实现状态。

两个节点仍是两个 IP plugin-owned concrete node，不把 rewrite policy 或 DeviceClass function
下沉到 `hammer-service::net`：

```rust
// hammer-plugins/net/ip
#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_rewrite,
    role = internal,
    name = "ip4-rewrite",
    next = Ip4RewriteNext,
)]
pub struct Ip4RewriteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

impl Node for Ip4RewriteNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime>;
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_rewrite,
    role = internal,
    name = "ip6-rewrite",
    next = Ip6RewriteNext,
)]
pub struct Ip6RewriteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

impl Node for Ip6RewriteNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime>;
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_rewrite_mcast,
    role = internal,
    name = "ip4-rewrite-mcast",
    sibling_of = Ip4RewriteNode,
)]
pub struct Ip4RewriteMcastNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

impl Node for Ip4RewriteMcastNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime>;
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_rewrite_mcast,
    role = internal,
    name = "ip6-rewrite-mcast",
    sibling_of = Ip6RewriteNode,
)]
pub struct Ip6RewriteMcastNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

impl Node for Ip6RewriteMcastNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime>;
}

fn register_ip4_rewrite_mcast(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal(Ip4RewriteMcastNode::new())
}

fn register_ip6_rewrite_mcast(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal(Ip6RewriteMcastNode::new())
}
```

rewrite 与邻居解析不得再共用当前 `Ip4AdjacencyNext`/`Ip6AdjacencyNext`。这不是在旧enum后追加两个
variant：Graph local slot属于source node，ARP/ND/glean和rewrite必须从声明、registration到process
返回值全部分开。目标固定next类型为：

```rust
// hammer-plugins/net/ip；只供 ip4-glean 与 ip4-arp
#[hammer_component_macros::node_next]
enum Ip4ResolutionNext {
    #[next("ip4-drop")]
    Drop,
}

// hammer-plugins/net/ip；只供 ip6-glean 与 ip6-discover-neighbor
#[hammer_component_macros::node_next]
enum Ip6ResolutionNext {
    #[next("ip6-drop")]
    Drop,
    #[next("ip6-rewrite-mcast")]
    ReplyTx,
}

// hammer-plugins/net/ip；供 ip4-rewrite 及其真实 rewrite siblings
#[hammer_component_macros::node_next]
enum Ip4RewriteNext {
    #[next("ip4-drop")]
    Drop,
    #[next("ip4-icmp-error")]
    IcmpError,
    #[next("ip4-frag")]
    Fragment,
}

// hammer-plugins/net/ip；供 ip6-rewrite 及其真实 rewrite siblings
#[hammer_component_macros::node_next]
enum Ip6RewriteNext {
    #[next("ip6-drop")]
    Drop,
    #[next("ip6-icmp-error")]
    IcmpError,
    #[next("ip6-frag")]
    Fragment,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_glean,
    role = internal,
    name = "ip4-glean",
    next = Ip4ResolutionNext,
)]
pub struct Ip4GleanNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_incomplete,
    role = internal,
    name = "ip4-arp",
    next = Ip4ResolutionNext,
)]
pub struct Ip4IncompleteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

fn register_ip4_glean(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip4GleanNode::new(),
        &Ip4ResolutionNext::NEXT_NAMES,
    )
}

fn register_ip4_incomplete(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip4IncompleteNode::new(),
        &Ip4ResolutionNext::NEXT_NAMES,
    )
}

fn register_ip4_rewrite(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip4RewriteNode::new(),
        &Ip4RewriteNext::NEXT_NAMES,
    )
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_glean,
    role = internal,
    name = "ip6-glean",
    next = Ip6ResolutionNext,
)]
pub struct Ip6GleanNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_incomplete,
    role = internal,
    name = "ip6-discover-neighbor",
    next = Ip6ResolutionNext,
)]
pub struct Ip6IncompleteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

fn register_ip6_glean(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip6GleanNode::new(),
        &Ip6ResolutionNext::NEXT_NAMES,
    )
}

fn register_ip6_incomplete(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip6IncompleteNode::new(),
        &Ip6ResolutionNext::NEXT_NAMES,
    )
}

fn register_ip6_rewrite(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip6RewriteNode::new(),
        &Ip6RewriteNext::NEXT_NAMES,
    )
}

fn rewrite_ip4_buffer(runtime: &mut DataPlaneMain, buffer_index: u32) -> u16;
fn rewrite_ip6_buffer(runtime: &mut DataPlaneMain, buffer_index: u32) -> u16;
```

`register_ip4_rewrite` 和 `register_ip6_rewrite` 必须分别以对应 `RewriteNext::NEXT_NAMES`
注册；glean/ARP/ND 注册只能使用对应 `ResolutionNext::NEXT_NAMES`。IPv6 resolution layout中的
`ip6-rewrite-mcast`必须是真实注册的IP-owned rewrite sibling；它和下述fragment nodes一样是实施本节
的graph前置条件，不能用punt/drop占位。

当前允许调用者把resolution enum传给rewrite的`impl_adjacency_node!`分支和接收protocol selector、
任意buffer index、单个drop slot的通用rewrite helper必须删除；后者的签名类型上无法表达
ICMP/fragment。目标使用两个协议具体的`rewrite_ip4_buffer`/`rewrite_ip6_buffer`；每个函数只操作
传入index指向的一个Buffer，并且只能返回自己的`RewriteNext`固定slot或adjacency中
缓存的动态`<name>-output` slot，不能接收任意next enum、`DpoProto` selector或fallback slot参数。

resolution process也不再把原始待解析packet统一返回`Punt`。按VPP语义，原始packet最终进入本协议
`Drop`；IPv4 ARP probe作为新buffer，使用该adjacency为`ip4-arp`/`ip4-glean`缓存的per-interface output
slot发送；IPv6 NS probe作为新buffer，取得该interface的multicast adjacency后直接提交到它的
`ip6-rewrite-mcast` NodeId。`ReplyTx`仍按VPP保留为同一真实target的固定slot，不能指向punt/drop；
原始packet不选择该slot。probe构造和throttle的node-local错误/计数不在本文增加，本文只固定graph
disposition与ownership。

`ip4-frag`、`ip6-frag` 是 MTU
分支的真实目标，不得依赖当前 named-next 缺失时回落到 drop 的行为冒充 fragment。若 fragment node
尚未实现，则 rewrite 的 MTU 分支不能声称完成；fragment node 本身的重组/分片算法不归本文扩写，
但其真实 graph registration 是实施本节的前置条件。

`hammer-service::net` 只拥有 protocol-neutral adjacency/rewrite storage 和 interface topology
fact；`hammer-plugins/net/ip` 读取这些 facts，拥有 IP header policy、IP errors、IP output arc index
和 `ip4-rewrite`/`ip6-rewrite` node。service 不导入 IP header、ICMP metadata 或 IP feature arc
index；IP plugin 不向 InterfaceMain 写 DPO 或设备 TX function。

每个节点对每个 Buffer 的目标顺序如下，IPv4/IPv6 只在协议字段和 MTU动作上分开：

1. 从 `IpSecondaryOpaque.lookup.forwarding` 取得 DPO；断言 class 是 complete adjacency，
   protocol 与当前 node 一致，并用 DPO index 取得本协议 adjacency。这个断言保护的是 graph/FIB
   invariant；正常的 incomplete/invalid lookup 已在前一节点选择 drop、ARP/ND 或 punt，不新增
   `RewriteDpoError`。
2. 从当前 packet cursor 读取 IP header。`ip4-input`/`ip6-input` 已经负责版本、基本长度和输入
   TTL/hop-limit 分类；rewrite 不为 malformed input 发明新的控制面错误。
3. 对非 locally-originated 的 IPv4 packet 递减 TTL 并更新 checksum；TTL 降到零时写入已有
   `TIME_EXPIRED` node error、清除 TX interface、设置 ICMP time-exceeded metadata，选择现有
   `ip4-rewrite -> ip4-icmp-error` next，且不 prepend L2 rewrite。IPv6 对非 locally-originated
   packet 递减 Hop Limit，零值选择对应的 `ip6-icmp-error` next，保持同一 metadata 语义。
4. 按 IP header 得到真正的 L3 packet length：IPv4 使用 `total_len`，IPv6 使用
   `payload_len + IPv6 header length`；GSO packet 使用现有 GSO length 语义。将它与
   `adjacency.rewrite_header.max_l3_packet_bytes` 比较，调用现有 VPP-style MTU helper（IPv4 helper 已在
   `crates/hammer-plugins/net/ip/src/protocol/ip.rs:668-699`）。
5. MTU 失败保持现有 IP node next/error：IPv4 的 DF-clear 选择 fragment，DF-set 生成 ICMP
   fragmentation-needed；IPv6 按 VPP 的 1280 下限和 locally-originated/transit 分支选择
   fragment 或 ICMP packet-too-big。失败分支不写 TX interface、不 prepend rewrite；IPv4 若已递减
   TTL，MTU failure 按 VPP 恢复 TTL/checksum 后再交给 fragment/ICMP next。
6. 先断言`adjacency.rewrite_header.data_bytes <= adjacency.rewrite_data.len()`；这保护的是owner未完成
   rewrite publication的programmer invariant，不产生packet error。成功分支将
   `adjacency.rewrite_data[0..adjacency.rewrite_header.data_bytes]` prepend 到 buffer
   current data，设置`NetworkOpaque.sw_if_index[TX] = adjacency.rewrite_header.sw_if_index`。rewrite node只改 L3/链路层 header
   和 TX metadata，不调用 DeviceClass、不写 fd、不构造 interface-TX DPO；本轮不扩写计数。
7. 以 adjacency 中缓存的 direct next 作为 fallback。没有 output feature 时直接返回该 slot；有
   feature 时以本协议 `output_feature_arc_index`、TX `sw_if_index` 和缓存的 adjacency config
   index 启动 Feature Arc，FeatureMain 将返回值替换为 arc 的第一个 feature next，并安装 Buffer
   config cursor。不能每个成功 rewrite 无条件查找或启动 arc。

#### 7.2.4 从 rewrite 到真正的设备发送

普通 IPv4/IPv6 的完整 TX 闭环是：

```text
ip4-rewrite / ip6-rewrite
  ├─ no HAS_FEATURES: cached <name>-output next
  └─ HAS_FEATURES: ip4-output / ip6-output feature chain
                     -> public interface-output (arc last)
                     -> <name>-output (TX sw_if_index fanout)
  -> interface-output Feature Arc (若该 interface 启用)
  -> interface-output-arc-end
  -> <name>-tx
  -> DeviceClass TX function
  -> device write / buffer release
```

`ip4-output` 和 `ip6-output` 的 last feature 都是公共 `interface-output`，公共 node 仅按
`NetworkOpaque.sw_if_index[TX]` 分发到具体 `<name>-output`。具体 output node 再做 interface
down/deleted/no-queue 分类；没有 interface-output feature 时直接选 `<name>-tx`，有 feature 时先
跑 per-interface arc，再由 `interface-output-arc-end` 选择该 interface 的 TX slot。

`<name>-tx` 才是 DeviceClass TX function 的 graph process。以 tuntap 为例，dynamic
`tuntap-0-tx` 调用 `tuntap_intfc_tx`，后者无条件进入 `tuntap-tx` 写完整 Buffer chain
到 fd；这条设备写路径不由 `ip4-rewrite`/`ip6-rewrite`承担。因而“rewrite 完成”只表示 packet
已经具备可发送的链路层头和 TX interface metadata，“真正 TX”一定发生在专属 TX node 的
DeviceClass function。

MTU、TTL/Hop Limit、fragment、ICMP 和 output node 的已有 node-local errors 保持原 owner；本节
不新增 registration error、DPO error、`MissingTxNode` 或 per-packet control-plane `Result`。

### 7.3 adjacency feature cache

Feature owner需要VPP同义的update callback inventory。IP adjacency owner注册一个callback；当某
`sw_if_index`上的feature enable/disable或end modification改变时：

1. callback只处理arc index等于本协议`output_feature_arc_index`的变化；
2. 遍历该software interface的IPv4或IPv6 adjacencies；
3. 根据该arc当前是否具有配置更新rewrite header的`REWRITE_HAS_FEATURES` bit；
4. 从Feature owner读取并缓存该`sw_if_index`当前config index到已有adjacency `config_index`字段；
5. 变更与graph/config publication处于同一main-thread barrier scope，workers只读，不增加lock或
   atomics。

`Adjacency`现有raw rewrite flags和config index足以表达该缓存，不增加wrapper。service只规定
`REWRITE_HAS_FEATURES`的bit位置，不知道IP arc index；callback inventory由
FeatureMain拥有；callback实现和adjacency walk由IP plugin拥有。service不得保存IP adjacency引用或
调用IP-private API。

### 7.4 per-interface end node

VPP `vnet_set_interface_l3_output_node`同时修改IP4、IP6、MPLS与Ethernet output arcs。Hammer crate
依赖方向不允许InterfaceMain持有这些plugin arc indices，因此不照搬这个聚合owner：

- IP plugin只负责`ip4-output`、`ip6-output`的per-interface end modification/reset；
- MPLS与Ethernet plugin在各自arc存在后负责自身end；
- FeatureMain继续拥有通用modify/reset config primitive；
- 默认/reset target为公共`interface-output`；
- 这不会改变`interface-output` arc自己的last `interface-output-arc-end`。

本文不新增一个service级“四协议setter”。这是Rust dependency边界的结构适配，packet semantics与
VPP一致。

### 7.5 device-input 的 IP next 归 IP plugin 注册

`ip4-input`、`ip6-input`是IP plugin拥有的Graph Nodes，所以指向它们的next也由IP plugin注册。
`hammer-service::device::DeviceInputNext`保持现状，不增加IP variants、IP node names或IP plugin
imports；tuntap plugin也不注册这两条next。

现有`ip_feature_init`在相关nodes全部materialize后执行以下owner操作：

1. 取得service公共`device-input`、IP-owned `ip4-input`与`ip6-input`的NodeId；
2. 调用现有`NodeMain::add_node_next_slot`分别建立`device-input -> ip4-input`和
   `device-input -> ip6-input`；
3. `NodeMain`按既有sibling语义把新增local slots同步到整个`device-input` sibling group；
4. 然后继续注册EtherType consumers与IP Feature Arcs。

该操作是IP plugin的通用graph初始化，不以tuntap是否加载为条件。registration得到的slot属于Graph
topology，不放进InterfaceMain。缺少service input node或IP-owned target表示plugin graph/lifecycle
错误，保留现有runtime graph error，不增加IP或tuntap domain error。

## 8. EthernetMain::eth_register_interface

### 8.1 registration value

`EthernetInterfaceRegistration`直接对应VPP字段，不增加嵌套callback wrapper或泛化命名：

```rust
impl EthernetMain {
    pub fn eth_register_interface(
        &self,
        main: &mut DataPlaneMain,
        registration: EthernetInterfaceRegistration,
    ) -> u32;
}
```

| Field | 语义 |
| --- | --- |
| `dev_class_index: u32` | DeviceClass index |
| `dev_instance: u32` | device instance |
| `max_frame_size: u16` | 0使用Ethernet default |
| `frame_overhead: u16` | 0使用Ethernet default |
| `flag_change` | 可选Ethernet flag-change callback |
| `set_max_frame_size` | 可选driver max-frame callback |
| `address: [u8; 6]` | primary MAC，按值拥有 |

Rust用`[u8; 6]`替代VPP临时`const u8 *`，长度由类型保证，不增加`InvalidMacLength`。

### 8.2 owner与调用顺序

`EthernetMain::eth_register_interface(main, registration) -> u32` 固定执行：

1. 从EthernetMain自己的interface pool分配record，保存两个callbacks；
2. 调用`InterfaceMain::register_interface`，dev class/instance来自registration，hw class固定为
   Ethernet HwClass，`hw_instance`为Ethernet pool index；
3. 取得返回的HwInterface，为其output node安装Ethernet formatter能力；
4. 写`min_frame_size = 64`；
5. `frame_overhead != 0`时原值写入，否则写22；
6. `max_frame_size != 0`时原值写入，否则写`9000 + overhead`；
7. software interface L3 MTU写9000；
8. primary MAC同时写HwInterface公共字段和Ethernet pool record；
9. 返回同一`hw_if_index`。

Ethernet pool record至少保存callbacks、flags事实与primary MAC。`HwInterface.hw_instance`是该record
index；InterfaceMain不能猜测或复制Ethernet-private lifecycle。

`eth_register_interface`不返回`EthernetError`。两个`u16` frame值的0是VPP定义的default selector，
不是invalid input。generic registration callback也不改变其返回语义。

Ethernet delete由Ethernet owner先定位`hw_instance`对应record，调用generic hardware delete，再释放
Ethernet record。返回语义为void；本文不为配对delete增加error。

## 9. tuntap 的 TUN graph registration

本轮只设计Linux TUN（L3 packet framing）。TAP、Ethernet framing、ARP、MAC读取/写入和
`eth_register_interface`的tuntap调用全部延期；第8节仍独立完成Ethernet owner API设计，不能因为
本轮没有TAP caller就把它重新并回generic registration。

### 9.1 三个不同角色

| 名称 | Graph Node | 注册位置 | 作用 |
| --- | --- | --- | --- |
| `tuntap-rx` | static input/interrupt | tuntap plugin graph inventory | 从TUN fd读L3 packet并进入device-input next layout |
| `tuntap-tx` | static internal | tuntap plugin graph inventory | 将L3 Buffer chain写入Linux TUN fd并结束ownership |
| `tuntap_intfc_tx` | 不是static node，是NodeProcessFn | TuntapDeviceClass TX function | 直接调用`tuntap_tx` |
| `tuntap-0-output` | dynamic internal | `register_interface` | 通用per-interface output/Feature start |
| `tuntap-0-tx` | dynamic output | `register_interface` | runtime process就是`tuntap_intfc_tx` |

不得注册名为`tuntap-infc-tx`或`tuntap_intfc_tx`的第三个静态node。不得由tuntap plugin手写
`tuntap-0-output`/`tuntap-0-tx`；它们必须由generic registration创建，才能共享第5、6节语义。

TuntapDeviceClass声明必须包含真实`tx_function = tuntap_intfc_tx`，因此generic registration创建
dynamic output/tx pair。目标TUN是正常P2P interface，不是VPP tuntap的OS punt/inject adapter；
`tuntap_intfc_tx`不得再保留一个“非normal就释放packet”的配置分支。ADR-0027 的no-TX branch不再
适用。

### 9.2 tuntap-rx

#### 9.2.1 config 与 owner state 不是摘要字段

`TuntapConfig`继续是tuntap plugin私有serde输入。IPv4 prefix由`ipnet::Ipv4Net`解析，prefix长度校验
因此属于config parse；该类型不进入`hammer-service`：

```rust
// hammer-plugins/device/tuntap
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TuntapConfig {
    enabled: bool,
    name: String,
    mtu: u32,
    admin_up: bool,
    ip4_address: Option<Ipv4Net>,
}

impl Default for TuntapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "vnet".to_owned(),
            mtu: 4_096 + 256,
            admin_up: false,
            ip4_address: None,
        }
    }
}
```

本轮删除目标配置中的`ethernet`和`have_normal_interface`；`deny_unknown_fields`使用既有parse error拒绝
这两个TAP/punt字段，不增加`UnsupportedMode`或其它新错误。字段名固定为`ip4_address`，例如
`ip4_address = "192.0.2.1/31"`。不增加含糊的`address`、
`addresses`或`ip_addr` alias；不增加`IpAddr`枚举、IPv6列表或service config facade。`None`是普通成功
状态，表示startup不配置IPv4地址。`admin_up`默认false，并且与`ip4_address`相互独立：前者组合VPP
`set interface state`语义，后者组合`set interface ip address`语义；地址存在不隐含admin-up。
`enabled = false`时其余设备字段与VPP一样不产生设备副作用，`admin_up`与`ip4_address`都不调用owner，
并且不发布`TUNTAP_MAIN`。已发布的Main恒表示一个创建成功的TUN，不允许用零index或假descriptor表达
disabled实例。

目标 plugin state 必须足以直接实现 File、RX cache、next layout 和 shutdown。不得用
`Vec<Option<T>>`表达已配置thread，也不得把IP next塞进service：

```rust
// hammer-plugins/device/tuntap
enum TuntapFile {
    Active {
        // 只用于TUNSETPERSIST和退出清理；packet I/O不绕过FileMain。
        control: OwnedFd,
        file_index: u32,
    },
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TuntapRxNext {
    drop: u16,
    ip4_input: u16,
    ip6_input: u16,
}

struct TuntapThreadState {
    rx_buffers: Vec<u32>,
    iovecs: Vec<libc::iovec>,
}

#[repr(C)]
struct TuntapThreadSlot {
    cacheline0: CacheLineAlignMark,
    state: RefCell<TuntapThreadState>,
}

struct TuntapMain {
    file: UnsafeCell<TuntapFile>,
    rx_next: OnceLock<TuntapRxNext>,
    threads: Box<[TuntapThreadSlot]>,
    mtu_bytes: u32,
    hw_if_index: u32,
    sw_if_index: u32,
}

impl TuntapMain {
    fn file_index(&self) -> u32;
    fn rx_next(&self) -> TuntapRxNext;
    fn thread(&self, runtime: &DataPlaneMain) -> RefMut<'_, TuntapThreadState>;
}
```

`TuntapFile` 是已创建descriptor的真实`Active -> Closed`生命周期状态，不是稀疏表或enabled selector。
plugin disabled由`TUNTAP_MAIN`尚未发布表达。`TuntapConfig.ip4_address: Option<Ipv4Net>`表达的
是用户可不配置地址这一真实domain absence，不是用`Vec<Option<_>>`掩盖半初始化record；成功调用
IP owner后不把prefix复制进`TuntapMain`，实际address/route/adjacency state只由IP plugin保存。
`threads`在config期间按runtime thread count一次性建立为fixed boxed slice，每个slot都包含真实state并
按cache line分隔。`thread`从借入的`DataPlaneMain`取得当前runtime thread index、转换为slot并断言范围，
再直接返回该slot的
`RefMut`；`RefCell`只检查owner contract，不进行跨线程同步。执行线程只能借用
`threads[runtime.thread_index()]`；禁止`thread_local!`、lock、atomic、caller closure、任意thread index
参数透传或跨worker借用。`unsafe impl Sync for TuntapMain`的安全依据必须写成这一永久slot assignment，
与现有TCP/UDP Main一致。

RX 的唯一入口是 `tuntap_rx(runtime, node_runtime, frame) -> usize`。它先从 runtime 分配以`u32`
index标识的Buffers，再把这些Buffers的可写segments临时借成iovecs，`readv`直接填充这些segments，随后由
同一个Node process完成chain construction、classification、Feature start和enqueue。tuntap层和
FileMain层都不定义、返回或接收独立packet值，不存在单包读取方法、optional packet中间协议或脱离
Buffer ownership的payload carrier。VPP的`tuntap_rx(vm, node, frame)`也在同一个Node process内完成
这组操作。

#### 9.2.2 node registration 与 sibling next layout

`tuntap-rx`是interrupt Driver Node；Hammer用`Driver`表达VPP input node。它必须显式设置interrupt
state并声明为`device-input` sibling：

```rust
#[hammer_component_macros::graph_node(
    graph = tuntap,
    init = register_tuntap_rx,
    role = driver,
    name = "tuntap-rx",
    sibling_of = hammer_service::device::DeviceInputNode,
)]
struct TuntapRxNode;

fn register_tuntap_rx(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_driver(TuntapRxNode::new())?;
    runtime
        .nodes()
        .set_node_state(node, NodeState::Interrupt)?;
    Ok(node)
}
```

不得为`tuntap-rx`声明第二个`node_next` enum。它从sibling owner取得自己可能选择的全部target slots：
启动时已有`drop`/`punt`，第7.5节再由IP plugin把`ip4-input`和`ip6-input`加入`device-input`；
`NodeMain`把相同slot同步给所有siblings。TUN不选择`punt`或`ethernet-input`，所以只固化
drop/IP4/IP6三个slot；它只在IP完成edge mutation后解析这些事实：

```rust
impl TuntapRxNext {
    fn resolve(nodes: &NodeMain) -> Self;

    fn slot(nodes: &NodeMain, rx: NodeId, target: NodeId) -> u16;
}

#[hammer_component_macros::main_loop_enter_function(
    name = "tuntap_input_init",
    runs_after = ["ip_feature_init"],
    runs_before = ["feature_arc_init"],
)]
fn tuntap_input_init(main: &mut DataPlaneMain) -> RuntimeResult<()>;
```

`resolve`分别取得`tuntap-rx`、service-owned `drop`和IP-owned`ip4-input`/`ip6-input`的NodeId，
再用`node_next_slot_for_target(rx, target)`取得三个slot。缺少node或
edge表示已经发布的plugin graph违反依赖关系，是startup graph invariant；不得用`None`保留半初始化
layout，也不得新建`tuntap next missing`错误。plugin disabled且`TUNTAP_MAIN`未发布时，
`tuntap_input_init`直接返回`Ok(())`；enabled时只解析并写一次`rx_next`，不调用`add_node_next_slot`、
不注册IP node、不拥有IP arc index。未发布Main也没有File readiness或dynamic TUN interface，所以
static node不会被调度。

#### 9.2.3 config 初始化时注册 FileMain

VPP在`tuntap_config`完成interface registration后立即`clib_file_add`。Hammer也必须在
`TuntapMain::init`的成功路径末尾、config callback返回前调用`FileMain::add`，不能等到第一次RX、
不能由read-ready懒注册，也不能把安装read interest留到`tuntap_input_init`。

```rust
fn register_tuntap_file(control: OwnedFd) -> RuntimeResult<TuntapFile> {
    let data = control
        .try_clone_to_owned()
        .map_err(|source| RuntimeError::FilePollerIo {
            operation: "duplicate tuntap descriptor",
            source,
        })?;
    let file = File::new(
        data,
        "vnet tuntap".to_owned(),
        0,
        FileFunctions {
            read: Some(tuntap_read_ready),
            ..FileFunctions::default()
        },
    );
    let file_index = FILE_MAIN
        .get()
        .expect("FileMain exists before tuntap config")
        .add(file)?;
    Ok(TuntapFile::Active {
        control,
        file_index,
    })
}

fn tuntap_read_ready(graph: &mut NodeMain, _: &mut File) -> RuntimeResult<()> {
    let node = graph
        .node_by_name(TuntapRxNode::NODE_NAME)
        .expect("tuntap-rx is materialized before File polling begins");
    graph.mark_interrupt_pending(node)?;
    Ok(())
}
```

Hammer的`FileMain::add(File)`取得`OwnedFd` ownership，`delete`会删除poll interest并close，因此不能把
同一个`OwnedFd`同时留在TuntapMain。这里duplicate与control descriptor共享同一Linux open file
description：FileMain-owned descriptor独占readiness、`readv`和`writev`；control descriptor只供
Linux TUN lifecycle ioctl与最后关闭。这是Hammer现有File ownership要求导致的Rust适配，不改变
VPP packet语义。

File callback在main loop开始polling之后才可能执行，而graph materialization和全部
main-loop-enter hooks先完成，所以config时尚未存在NodeId不构成竞态。read-ready通过NodeMain的
name index取得已经materialize的`tuntap-rx`，只置interrupt pending；它不得调用`readv`、不得分配
Buffer、不得调用Feature Arc、不得直接执行Node。随后固定main-loop顺序的
`schedule_interrupt_driver_nodes`提交空frame，真正I/O只在`tuntap-rx` process发生。

`FileMain::add`失败时，FileMain撤销其pool insertion并drop duplicate；tuntap config再按已有初始化
cleanup删除已创建interface、撤销TUN persistence并关闭control descriptor，原样返回
已有`RuntimeError`及source。不得翻译为InterfaceError或新Tuntap错误。成功add只把active
`TuntapFile`保留在尚未发布的local `TuntapMain`中；第9.5节IP owner address add也成功后，config才
一次性发布`TUNTAP_MAIN`。任何fallible startup操作都不得发生在该publication之后。

#### 9.2.4 `tuntap_rx`直接拥有完整RX算法

目标入口只有VPP同形的Node process；不存在先“读出一个packet id”再由另一层分类/提交的协议：

```rust
impl Node for TuntapRxNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        tuntap_rx(runtime, node_runtime, frame)
    }
}

fn tuntap_rx(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize;
```

`tuntap_rx`每次最多从TUN读一个L3 packet，与VPP一致。`frame`是interrupt调度产生的空frame，不是
RX packet来源；函数不读取其input vectors。完整body顺序固定如下：

1. 取`TuntapMain::thread(runtime)`。当前File固定由thread 0 polling，所以RX只改变
   thread 0 cache；其它thread entry仍供其TX iovecs使用。
2. 当`rx_buffers.len() < DEFAULT_BUFFER_FRAME_CAPACITY / 2`时，以固定栈数组向
   `DataPlaneMain::buffer_alloc`申请至一个frame的indices，并把实际成功数追加到plain
   `Vec<u32>`。不得预填`Option` slot。
3. 从cache尾部反向选择buffers；每个候选buffer先`buffer_chain_init`，再以`put_uninit`取得其完整
   writable data slice并形成一个`libc::iovec`，直到iovecs总capacity至少为`mtu_bytes`。这保持VPP
   “从cache末尾消费、一个MTU的iovecs”的顺序，又不要求新增一个runtime buffer-size API。
4. 若实际分配后capacity仍不足一个MTU，不调用`readv`、不消费cache、返回0，等待后续readiness重试。
   VPP在此有`ASSERT(rx_buffers >= mtu_buffers)`；Hammer必须尊重allocator的实际返回数，不能越界，且
   本分支不新增错误或counter。
5. 只调用现有`FILE_MAIN.readv(file_index, &mut iovecs)`一次。`Ok(None)`是WouldBlock；
   `Ok(Some(0))`是本次无packet；两者都把所有prepared buffer header恢复为unchained、length 0，
   cache长度不变并返回0。`Err(RuntimeError::FileRead { .. })`也做同样恢复并返回0；Graph Node内
   禁止`tracing`/`log`、字符串格式化或每次I/O错误输出，不构造per-packet error或新的control-plane
   `Result`。`FileIndexInvalid`不是read failure，而是config publication与exit ordering被破坏的programmer
   invariant；恢复prepared headers后以静态消息panic，由runtime worker boundary收口，不能用`Err(_)`
   把它吞成无packet。tuntap I/O counter按用户要求延期，因此本轮只固定ownership/disposition。
6. positive byte count先把全部候选header恢复为length 0，再按实际字节数确定used prefix；按iovecs
   顺序用`buffer_chain_buffer`链接used buffers，逐段`put_uninit(segment_bytes)`公布准确
   `current_length`，并在head写`total_len_not_including_first`。未使用候选仍留在cache且length 0。
7. 从`rx_buffers`尾部只移除used buffers；从这一刻起完整chain ownership转给head
   buffer index，cache不得再引用任何已链接segment。
8. 在head的`NetworkOpaque`写`sw_if_index[RX] = tuntap sw_if_index`、
   `sw_if_index[TX] = u32::MAX`和L3 offset 0。本文按用户要求不增加RX packet/byte counter。
9. 在同一个函数中读取head当前data首字节高4位：`4`选择`rx_next.ip4_input`、`6`选择
   `rx_next.ip6_input`，其它值选择`rx_next.drop`。不调用独立
   classification helper，不按node name做packet-hot-path查询。
10. software interface不是admin-up时，把default next覆盖为drop；本轮TUN始终是普通generic P2P
    interface，不存在punt/inject绕过admin gate的分支。
11. 无论default next是否为drop，都调用
    `FeatureMain::start_device_input(sw_if_index, runtime.buffer_mut(head), default_next)`；Feature owner把
    default next写入packet的config continuation，并返回首个feature slot或原default。tuntap不读取
    device-input arc index，也不直接调用通用`start_feature_arc`。
12. 用`get_next_frame`/`put_next_frame`把唯一head加入步骤11返回的slot并返回1；tail只通过Buffer
    header属于该chain，绝不逐段加入Frame。

步骤1-5的所有“本次没有完整packet”分支都在`tuntap_rx`中恢复prepared headers并直接返回0；步骤6
以后形成的chain必须继续完成步骤7-12，不能返回`Option`、不能把head暂存到Main，也不能让另一个
helper接管classification/Feature/enqueue。未知TUN版本当前只按VPP next decision进入drop；按用户
要求，本轮不设计`UnknownPacketType` counter或任何其它tuntap counter。

### 9.3 tuntap-tx 与 tuntap_intfc_tx

`TuntapTxNode`是无next、无node errors的static internal node；`tuntap_intfc_tx`不是node类型，而是
与它复用同一body的`NodeProcessFn`：

```rust
// hammer-service::interface_model
pub struct DeviceClass {
    // 其它既有字段保持不变。
    pub tx_function: Option<NodeProcessFn>,
}

impl DeviceClass {
    pub const fn with_tx_function(mut self, function: NodeProcessFn) -> Self {
        self.tx_function = Some(function);
        self
    }
}

// hammer-plugins/device/tuntap
#[derive(hammer_component_macros::DeviceClass)]
#[device_class(
    name = "tuntap",
    format_device_name = format_tuntap_interface_name,
    tx_function = tuntap_intfc_tx,
)]
struct TuntapDeviceClass;

#[hammer_component_macros::graph_node(
    graph = tuntap,
    kind = internal,
    name = "tuntap-tx",
)]
struct TuntapTxNode;

impl Node for TuntapTxNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        tuntap_tx(runtime, node_runtime, frame)
    }
}

fn tuntap_tx(
    runtime: &mut DataPlaneMain,
    _: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let main = TUNTAP_MAIN
        .get()
        .expect("tuntap main exists before graph dispatch");
    let packet_count = frame.vector_args().len();

    for &head in frame.vector_args() {
        let mut state = main.thread(runtime);
        state.iovecs.clear();
        let mut packet_bytes = 0usize;
        let mut segment_index = Some(head);
        while let Some(index) = segment_index {
            let segment = runtime.buffer(index);
            let bytes = segment.current();
            state.iovecs.push(libc::iovec {
                iov_base: bytes.as_ptr().cast_mut().cast(),
                iov_len: bytes.len(),
            });
            packet_bytes += bytes.len();
            segment_index = segment.next_buffer_slot();
        }

        let write_result = unsafe {
            FILE_MAIN
                .get()
                .expect("FileMain exists before graph dispatch")
                .writev(main.file_index(), &state.iovecs)
        };
        match write_result {
            Ok(Some(written)) if written == packet_bytes => {}
            Ok(Some(_)) | Ok(None) | Err(RuntimeError::FileWrite { .. }) => {}
            Err(RuntimeError::FileIndexInvalid { .. }) => {
                panic!("published tuntap File index must remain live during graph dispatch")
            }
            Err(_) => panic!("FileMain::writev returned an undocumented error category"),
        }
    }
    runtime.buffer_free(frame.vector_args());
    packet_count
}

fn tuntap_intfc_tx(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    tuntap_tx(runtime, node_runtime, frame)
}
```

`DeviceClass::with_tx_function`是class registration的窄构造能力；derive macro的
`tx_function = tuntap_intfc_tx`只展开成该方法调用。`register_interface`把这个函数直接安装为动态
`<name>-tx`的process，不增加trait object、plugin capability registry或interface层转发wrapper。

`tuntap_tx`对frame中的每个head直接执行：清空当前thread的iovecs，遍历完整Buffer chain，把每段
`current()`的pointer/length临时借成iovecs，不复制payload，然后只通过
`FILE_MAIN.writev(file_index, &iovecs)`写descriptor。TX的输入始终是Frame中的Buffer-chain heads；
tuntap层和FileMain层都不增加单包写入方法、packet carrier或packet ownership转换。iovecs只在本次
调用期间描述借入的Buffer segments，不能保存payload，也不能比Buffer borrow活得更久。
完整写入、short write、WouldBlock和write error都消费本packet且不重试；Graph Node内不得记录日志、
格式化错误或返回control-plane `Result`，也不增加`TuntapTxError`或node error。本文不更新TX
counter，所以后三类暂时只有相同的consume disposition；后续counter设计只能增加node-local统计，
不能改变ownership。`FileIndexInvalid`和`writev`返回未声明的error category不是packet failure：它们
表示File lifecycle/API contract被破坏，使用静态panic消息交给runtime worker boundary，不打印或格式化
底层I/O error。

Hammer Frame在这条路径中只携带每条chain的head。`tuntap_tx`完成全部write尝试后必须对
`frame.vector_args()`调用现有`DataPlaneMain::buffer_free`；该API按`NEXT_PRESENT`递归释放完整chain。
不得机械照搬VPP normal-interface分支的`vlib_buffer_free_no_next`：VPP的interface-output在进入driver前
具有自己的chain flatten语义，而Hammer当前dynamic output没有证明会把tail变成独立frame vectors。
在Hammer对heads调用`buffer_free_no_next`会泄漏tail。`tuntap_intfc_tx`无条件调用`tuntap_tx`，不再
按mode释放未发送packet，也不安装OS punt callback或no-punt callback；那些是本轮明确延期的
punt/inject adapter语义，不属于这个TUN device。

### 9.4 per-thread state与FileMain

ownership固定如下：

| 资源 | owner | 使用者 | 释放顺序 |
| --- | --- | --- | --- |
| FileMain duplicate descriptor与poll interest | runtime `FileMain` | read-ready、`readv`、`writev` | `FileMain::delete(file_index)`删除interest并close |
| TUN control descriptor | tuntap `TuntapFile::Active` | config ioctl、exit `TUNSETPERSIST` | exit把state改为`Closed`并drop |
| host interface socket | tuntap config/exit调用栈 | host TUN MTU/flags ioctl | 对应操作完成后由局部`OwnedFd`立即close，不存入`TuntapMain` |
| RX free buffer-index cache | executing runtime thread的`TuntapThreadState` | 仅`tuntap-rx` | 先删File interest，再由同一thread `buffer_free_no_next` |
| TX iovecs scratch | 每thread的`TuntapThreadState` | `tuntap_tx`/`tuntap_intfc_tx` | process lifetime，无payload ownership |
| linked RX packet | graph Frame | feature/input/downstream nodes | 正常graph terminal释放 |

exit在final WorkerBarrier内执行，固定顺序是：临时创建host interface socket并停止host interface、在control descriptor撤销persistence、
调用`FileMain::delete`停止readiness并close data duplicate、释放thread 0尚未消费的RX cache、关闭control
descriptor、删除interface。config与exit的host interface socket都只存在于对应调用栈，不进入`TuntapMain`；
cleanup仍按VPP warning policy尽量执行全部步骤；
撤销persistence或停止host interface失败只warning并继续；`FileMain::delete`返回的既有
`RuntimeError`是exit唯一保留并最终返回的typed error，后续cleanup warning不得替换它；`Ok(false)`表示
active `file_index`已失效这一内部lifecycle invariant，立即assert/panic。无论delete返回error与否，都继续
释放cache、关闭其余descriptors并尝试删除interface。File interest成功删除后不得再调用
`readv`/`writev`。Hammer只在data-plane main loop已经退出、Process Nodes已停止且final barrier持有时
运行exit hooks，因此delete失败后也不会再次poll该File；FileMain保留的duplicate最终随runtime进程
teardown关闭，不能由tuntap越权取得并手工close。

### 9.5 TUN interface registration 与 startup IPv4 address

#### 9.5.1 唯一registration分支

本轮TUN始终使用L3 framing和`TuntapHwClass`的P2P fact。tuntap自身不得分配`HwInterface`、
`SwInterface`或手工创建dynamic output/tx nodes，唯一实例创建路径为generic registration：

```rust
let interfaces = NetMain::global()
    .expect("NetMain exists before tuntap config")
    .interface_main();
let hw_if_index = interfaces.register_interface(
    data_plane,
    TuntapDeviceClass::class_index(),
    0,
    TuntapHwClass::class_index(),
    0,
);

let sw_if_index = interfaces
    .hardware_interface(hw_if_index)
    .sw_if_index;
```

代码块表达调用边界，不批准新的`class_index` getter；实现使用class derive已经发布的现有index
accessor。必须使用返回的`hw_if_index`查询真实`sw_if_index`，绝不使用二者相等的捷径。
TUN不得调用`eth_register_interface`，不得创建Ethernet record，也不得读取MAC；后续TAP slice只能
通过第8节的Ethernet入口创建实例，不能先generic register再补record。

registration后设置hardware `LINK_UP`；`admin_up = true`时再调用software admin owner。随后调用
第9.2.3节的`register_tuntap_file`。plugin inventory声明`TuntapRxNode`/`TuntapTxNode`，DeviceClass
TX function使generic registration生成dynamic output/tx pair；dynamic`tuntap-0-tx`固定经过
`tuntap_intfc_tx -> tuntap_tx -> writev`。

#### 9.5.2 `ip4_address`由IP plugin owner生效

VPP `tuntap_config`本身没有address token；
`ip4_address`是用户明确要求的Hammer startup-config扩展，而不是声称VPP已有该字段。扩展只改变配置
入口，不改变address owner：tuntap plugin已经按ADR-0028显式依赖IP plugin，因此在取得真实
`sw_if_index`后直接调用现有family API：

```rust
fn add_tuntap_ip4_address(
    data_plane: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv4Net,
) -> Result<(), TuntapConfigError> {
    ip4_add_del_interface_address(
        data_plane,
        sw_if_index,
        address.addr(),
        address.prefix_len(),
        false,
    )
    .map_err(|source| TuntapConfigError::Ip4Address {
        sw_if_index,
        address,
        source,
    })
}

#[derive(Debug, thiserror::Error)]
enum TuntapConfigError {
    // 既有Linux syscall variants保持不变。

    #[error("configure IPv4 address {address} on interface {sw_if_index}")]
    Ip4Address {
        sw_if_index: u32,
        address: Ipv4Net,
        #[source]
        source: IpInterfaceAddressError,
    },
}
```

这里不增加`TuntapAddressError`，也不复制或重命名IP错误码。`Ip4Address`只是tuntap config到runtime
lifecycle边界的一次owner-local context，source仍是IP plugin现有`IpInterfaceAddressError`及其VPP
`api_errno` code；unsupported、prefix、duplicate和address-in-use分支全部由IP owner决定。serde解析非法prefix仍由
现有`ConfigFunctionParse`报告，不进入该variant。

config成功顺序固定为：

1. 以`IFF_TUN | IFF_NO_PI`创建并配置Linux TUN descriptors；
2. 只调用第9.5.1节的`register_interface`，取得`hw_if_index`和真实`sw_if_index`；
3. 调用interface owner设置hardware `LINK_UP`；`admin_up = true`时再设置software `ADMIN_UP`；
4. duplicate data descriptor并调用`FileMain::add`，把read interest和packet I/O descriptor交给
   File owner；
5. `ip4_address`为`Some(prefix)`时调用上面的IP owner operation；为`None`时直接完成；
6. 前面所有fallible操作完成后，才一次性发布`TUNTAP_MAIN`并返回成功。

本轮不注册ADR-0028的tuntap IPv4/IPv6 address-sync callbacks；它们把任意VPP interface address镜像到
Linux punt/inject adapter，VPP在`have_normal_interface`模式本就立即返回，不属于本轮TUN。IP API的returned error在
第一次IP mutation前完成校验，因此失败时不留下半个IP address。tuntap随后执行现有startup cleanup：
删除File registration、删除刚创建的interface、撤销persistence并关闭descriptors，最终返回
`Ip4Address`且保留原始IP source；不得把它改写成Linux ioctl error、log后返回`Ok(())`或新增统一
config error。由于Main尚未发布，失败路径也不会留下可被Graph Node观察的`tuntap`实例。

### 9.6 plugin registration顺序

Tuntap plugin保持对IP plugin的显式依赖，但next registration仍由IP owner完成。Hammer config早于
static graph materialization，因此startup顺序固定为：

1. normal init安装TuntapDeviceClass/P2P TuntapHwClass，DeviceClass携带`tuntap_intfc_tx`；
2. config以TUN framing创建Linux fd并只调用`register_interface`；startup dynamic node intents按第5.8节
   等待graph materialization；设置link/admin事实后duplicate data descriptor并调用`FileMain::add`
   安装read interest，把`TuntapFile::Active`保留在local Main，按第9.5.2节应用可选`ip4_address`，最后
   才发布`TUNTAP_MAIN`；不注册punt/inject address callbacks；
3. runtime materialize service的`device-input`、`ethernet-input`、公共output与arc-end，IP plugin的
   input/rewrite nodes，以及tuntap的`TuntapRxNode` sibling和`TuntapTxNode`；
4. IP plugin的`ip_feature_init`按第7.5节注册两条IP next，`NodeMain`同步全部device-input siblings，
   然后完成两条IP output arcs；
5. `tuntap_input_init`运行在`ip_feature_init`之后；enabled时只读取并固化drop、IP4、IP6三个现有
   sibling slots，disabled时直接完成；它不注册File、不增加next；
6. 通用`feature_arc_init`在所需arc declaration、starts与next topology齐全后完成config publication；
7. main loop此后才第一次poll FileMain；readiness置`tuntap-rx` pending并由interrupt driver调度执行。

File record虽然在config阶段已经active，但runtime在graph materialization和全部main-loop-enter完成前
不poll它，所以任何callback都不会早于RX NodeId、resolved next、thread state、interface identity和
Feature Arc publication。这个时序既保留VPP“config注册file”，又满足Hammer的graph lifecycle。

## 10. 精确错误语义

| 场景 | 分类与行为 | 明确禁止 |
| --- | --- | --- |
| `register_interface` | 返回`u32`，无recoverable registration error | `InterfaceResult<u32>`、rollback error、duplicate-name error |
| `eth_register_interface` | 返回`u32`，沿用generic语义 | `EthernetError`、MAC length error、frame default error |
| no-TX DeviceClass | 成功注册，graph facts为absence | `MissingTxFunction` |
| create callback返回error | callback chain按既有规则停止；registration仍成功且不rollback | error translation、delete补偿 |
| invalid class/index/publication scope | owner/programmer invariant | 把bug改成调用者可重试error |
| `<name>-output` packet | 仅down/deleted/no-queue三个typed node errors | control-plane Result、字符串trace代替counter |
| arc-end zero queue | `NoTxQueue`并仅drop该interface组 | DPO fallback、generic missing lookup error |
| IP rewrite | 保留现有IP packet errors；feature选择next不返回Result | output-arc control error |
| tuntap RX unknown IP version | 本轮只选择既有drop slot；VPP对应error counter按明确非目标延期 | config error、新error enum或字符串error |
| tuntap RX read failure | WouldBlock、0和其它OS error都恢复全部prepared buffer headers、保持cache ownership并返回0；Node不记录日志 | warning/log、字符串格式化、新RX error family、每包allocation |
| tuntap TX short/WouldBlock/error | consume完整Buffer chain ownership且不重试；Node不记录日志 | warning/log、`TuntapTxError`、保留private payload copy |
| tuntap packet I/O时`file_index`失效 | programmer/lifecycle invariant；静态消息panic并由worker boundary收口 | 当成read/write failure返回、日志后继续 |
| config中的File duplicate/add失败 | 撤销tuntap interface/fd初始化并保留现有runtime typed error与source | 包装成InterfaceError或message-only error |
| config中的`ip4_address`解析失败 | 现有`ConfigFunctionParse`保留toml/ipnet source，尚未创建interface | 新IP error、字符串校验或默认地址 |
| config中的IPv4 address add被IP owner拒绝 | `TuntapConfigError::Ip4Address`只增加`sw_if_index`/prefix context并保留`IpInterfaceAddressError` source；执行既有startup cleanup | `TuntapAddressError`、复制IP状态码、warning后成功或部分设备状态继续启动 |
| 已发布graph缺少tuntap sibling next | startup programmer/lifecycle invariant | `Option`半初始化、可重试tuntap error |

本设计不增加catch-all、string-only、`Other`、`Internal`或`Invariant` variants。已有IP/interface
packet errors继续通过Node error与next处理；tuntap本轮只设计next disposition，全部counter明确延期。
registration的VPP void/u32路径保持void/u32；真正的Linux config errors继续由tuntap plugin现有具体
variants拥有。`Ip4Address`是用户要求的startup配置在plugin/lifecycle边界所需的唯一新增config
variant，不改变IP owner的六类address error，也不被packet path观察。

`tuntap-rx`、static `tuntap-tx`和dynamic `<name>-tx`都属于Graph Node执行边界，禁止
`tracing::{error,warn,info,debug}`、`log::*`、`println!`、`eprintln!`以及任何I/O error格式化。VPP在
`readv`非EAGAIN失败和`writev`短写时调用`clib_unix_warning`是已核实事实；Hammer只对齐其packet
ownership和next disposition，不把同步日志副作用带入Node热路径。第9.4节exit cleanup发生在
main-loop lifecycle边界，不是Graph Node，二者不得混为一种错误路径。

## 11. 三方语义差异

| 维度 | 当前Hammer | 目标Hammer | VPP | 结论 |
| --- | --- | --- | --- | --- |
| DPO owner | InterfaceMain与graph initializer混合持有 | NetMain/net::dpo独占 | DPO modules | 修正owner |
| generic名称/返回 | `register_hardware_interface -> Result` | `register_interface -> u32` | `vnet_register_interface -> u32` | 对齐 |
| callback失败 | rollback | 保持registered并返回index | 不传播、不rollback | 对齐 |
| output graph | 公共node直接找TX | 公共、专属output、Feature、arc-end、专属TX | 同目标 | 对齐 |
| 普通adjacency next | 每次构造interface-TX DPO并stack | 直接建立到`<name>-output`的next | neighbor/mcast直接传专属output node | 删除错误DPO使用 |
| rewrite init | interface/MTU在`new`预填，`init`不接收interface且stack DPO | `init`显式接收`sw_if_index`/MTU类别/source/target并一起发布interface、MTU、next | `vnet_rewrite_init`接收同组事实并写三项header state | 对齐，不从旧rewrite state反查interface |
| interface-TX DPO用途 | 被所有普通adjacency使用 | 仅显式DPO stack；当前无midchain producer | vendored tree仅PPPoE构造 | 对齐，不创建placeholder consumer |
| lookup tables | 缺失 | create与arc-end后发布一个成对dense table（含两个VPP字段） | `vnet_if_update_lookup_tables`维护两张dense vectors | Rust强类型owner适配 |
| output arc starts | 非空且固定 | 可空、registration追加 | 初始为空、registration追加 | 对齐 |
| IP output | 缺失 | IP4/IP6独立arc | IP4/IP6独立arc | 对齐 |
| IP starts | 只有rewrite nodes存在 | 当前只注册rewrite | rewrite/midchain/dvr | 有界差异，禁止placeholder |
| Ethernet owner | 无interface pool | EthernetMain pool + exact fields | EthernetMain pool | 对齐 |
| tuntap nodes | 全部缺失 | RX/TX static + dynamic pair | 同目标 | 对齐 |
| tuntap File注册时机 | 未注册 | config创建interface后立即`FileMain::add` | config末尾`clib_file_add` | 对齐 |
| tuntap descriptor ownership | raw fd由plugin直接read/write/close | FileMain duplicate负责poll/readv/writev，plugin control fd负责ioctl | 一个raw fd由clib File记录并由exit close | Rust ownership适配，open-file-description语义相同 |
| tuntap next ownership | 无RX node | IP plugin新增IP edges；tuntap只解析sibling slots | device-input固定layout | plugin边界适配 |
| tuntap I/O failure | 无packet path | RX恢复候选buffer并返回0；TX消费chain且不重试；两者均无日志 | VPP disposition相同，但会打印warning | ownership/disposition对齐；Node observability为Hammer有界差异 |
| tuntap counters | 无packet path | 本文延期 | VPP更新RX/TX并声明unknown-type error | 用户指定的有界差异 |
| tuntap startup admin/IP | admin/address字段被拒绝 | private `admin_up`显式调用interface owner，`ip4_address: Option<Ipv4Net>`直接调用IP owner API；所有fallible操作完成后才发布Main | VPP generic branch设置interface state，address仍由独立control operation配置 | 用户要求的startup组合；不让address隐含admin-up |
| tuntap RX组织 | 无packet path | 单个`tuntap_rx`直接完成allocation/readv/chain/classify/feature/enqueue | 单个`tuntap_rx`完成同一流程 | 对齐；无packet-returning中间协议 |
| L3 aggregate setter | 尚无 | protocol owners分别修改end | interface层按四arc名称循环 | dependency边界适配 |

## 12. 类型与 API 变更清单及审批

### 12.1 新增，需在实现前批准

| 类型/API | Owner与必要性 | 现有surface不足 | Recovery/consumer | 审批状态 |
| --- | --- | --- | --- | --- |
| `EthernetInterfaceRegistration` | Ethernet owner；表达VPP registration字段 | generic interface参数不拥有Ethernet callbacks/MAC | 无registration error；后续TAP/Ethernet devices消费，本轮TUN不消费 | approved |
| private `EthernetInterface` pool record | EthernetMain；保存callbacks与MAC lifecycle | HwInterface不能拥有Ethernet-private state | owner delete释放 | approved |
| `EthernetMain::eth_register_interface` | Ethernet唯一入口 | generic registration不能建立Ethernet owner record/defaults | 返回`u32` | approved |
| private `InterfaceLookupEntry`/`InterfaceLookupTable` | InterfaceMain；以一行dense entry维护两个VPP software-index lookup facts，并用`~0`表达未安装 | `Vec<Option<...>>`允许两列独立缺失且无法表达VPP sentinel contract | Interface output/arc-end packet consumers；无新error | approved |
| `FeatureMain::add_feature_arc_start` | Feature owner；动态加入per-interface output starts | 当前start storage构建后固定 | lifecycle violation为assertion/runtime owner | approved |
| Feature update callback registration | Feature owner inventory，IP plugin实现callback | 当前adjacency无法缓存output config变化 | 无新error；IP adjacency consumer | approved |
| dynamic/recycle Node publication capability | NodeMain；创建/复用per-interface pair并更新所有workers | 现有additive refork不能替换deleted runtime/function | runtime已有graph error owner | approved |
| private `Ip4ResolutionNext`/`Ip6ResolutionNext` | IP plugin；分别表达VPP IPv4 drop-only与IPv6 drop/rewrite-mcast resolution layout | 当前与rewrite错误共用drop/punt enum | packet next；无新control error | approved |
| private `Ip4RewriteNext`/`Ip6RewriteNext` | IP plugin；固定drop/ICMP/fragment layout | 当前rewrite缺少VPP next targets | packet next；沿用IP node errors | approved |
| `DpoType::ADJACENCY_MCAST` / `AdjacencyLookupNext::Multicast` | net adjacency/DPO owner；保持multicast与neighbor class identity分离 | 当前三类映射会把multicast误归普通或incomplete adjacency | IP plugin class binding消费；无新error | approved |
| private `Ip4RewriteMcastNode`/`Ip6RewriteMcastNode` | IP plugin；实现multicast adjacency DPO binding，并提供IPv6 resolution `ReplyTx`的真实target | 当前没有rewrite-mcast siblings，不能把multicast映射到普通rewrite或用punt/drop占位 | multicast adjacency与IPv6 NS消费；错误仍归IP node | approved |
| private `Ip4FragmentNode`/`Ip6FragmentNode` | IP plugin；提供rewrite MTU分支的真实target | 当前没有`ip4-frag`/`ip6-frag`，named-next fallback不能代替分片 | IP rewrite消费；具体fragment contract需独立批准 | approved |
| `RewriteHeader`/`REWRITE_HAS_FEATURES` | service net；直接表达VPP protocol-neutral rewrite header、紧邻storage与跨协议feature bit contract | 当前`AdjacencyRewrite`把header/data压成一层假aggregate，IP代码无法直接使用adjacency字段 | adjacency owner初始化；IP rewrite读取；无新error；IP-specific flags不进入service | approved |
| `TuntapRxNode` | tuntap plugin；真实interrupt RX | plugin当前无packet ingress | 本轮只有next disposition，无新node error/counter | approved |
| `TuntapTxNode` | tuntap plugin；真实TUN fd TX process | plugin当前无packet egress | 无node error；short/WouldBlock/error均consume且无日志 | approved |
| `TuntapThreadState`/`TuntapThreadSlot` | tuntap plugin；固定boxed slice中的cache-line分隔per-thread RX cache/iovecs | 共享Main当前无per-thread I/O state；`Vec<UnsafeCell<_>>`会暴露不健全的safe mutable borrow | 当前worker取得直接`RefMut`；shutdown释放cache | approved |
| private `TuntapRxNext` | tuntap plugin；固化已有sibling local slots | packet path不能每包按name查询，tuntap又不能注册IP edges | startup invariant；RX消费 | approved |
| private `TuntapFile` | tuntap plugin；表达已创建descriptor的active/closed lifecycle | raw fd与FileMain OwnedFd ownership不能重叠；disabled不发布Main | config与exit消费；沿用RuntimeError | approved |
| `DeviceClass::with_tx_function(NodeProcessFn)` | interface class registration；让device plugin声明真实TX process | 当前`tx_function: Option<fn()>`不能作为Graph Node process，dynamic TX无法调用driver | 无新error；class derive与dynamic TX registration消费 | approved |
| private `TuntapConfig.admin_up`、`ip4_address: Option<Ipv4Net>`及tuntap对`ipnet`的直接依赖 | tuntap plugin；startup分别组合VPP admin与IPv4 prefix操作 | 当前deny-unknown配置拒绝字段；generic registration保持admin-down，只有地址无法通过ICMP example | admin调用interface owner；`None`无IP操作，`Some`由IP owner消费 | approved |
| private `TuntapConfigError::Ip4Address` | tuntap config/lifecycle boundary；给IP source附加目标interface/prefix | `RuntimeResult`不能直接携带plugin-owned IP error，字符串转换会丢category/source | startup caller停止启动并清理tuntap；不进入packet path | approved |

### 12.2 修改

| Surface | 最终修改 |
| --- | --- |
| `InterfaceMain::register_hardware_interface` | 删除旧名；以`register_interface -> u32`替代，所有in-tree callers迁移 |
| `InterfaceMain` state | 增加四项hardware graph facts、一个成对的dense lookup table、output arc index与deleted pair inventory；删除全部DPO |
| `HwClass.update_adjacency` | `DpoId`改为`u32 adj_index` |
| `Adjacency<P>` | 删除`rewrite: AdjacencyRewrite`；直接增加`rewrite_header: RewriteHeader`与`rewrite_data: [u8; 116]`，保持128-byte rewrite区域和4 cache-line adjacency布局 |
| `AdjacencyMain::insert` | 删除`sw_if_index`/`mtu`输入；只建立poisoned rewrite storage，调用者随后通过`RewriteHeader::init`发布真实interface/MTU/next |
| `RewriteHeader::{poisoned,init,set_data,clear_data,update_mtu}` | insert只建立poisoned header/data；`init`显式接收`sw_if_index`、`InterfaceMtuKind`和source/target NodeId，一次发布interface、MTU与local next；data方法同时维护header length和adjacency-owned storage |
| `FeatureMain::register_feature_arc` | 允许零start但仍要求有效last/feature |
| Feature start storage/config compile | owner可追加；所有starts共享local slots；late start不重编号现有config |
| `Ip4Main/Ip6Main.lookup_main` | 各自增加`output_feature_arc_index` |
| `ip_feature_init` | 由IP owner注册`device-input -> ip4-input/ip6-input`后再完成EtherType与Feature注册；service/tuntap不注册这两条next |
| IP plugin private output registration/update helpers | 只在`hammer-plugins/net/ip`内写IP4/IP6 output arc index与adjacency config cache；不向`InterfaceMain`增加IP类型或setter |
| IP rewrite/adjacency update | complete DPO分别进入`ip4-rewrite`/`ip6-rewrite`；resolution与rewrite next enums分离；邻居完成时发布complete DPO；rewrite补全TTL/Hop Limit、MTU、header prepend、TX metadata、cached output Feature Arc和direct/device TX选择 |
| `Ip4RewriteNode`/`Ip6RewriteNode` | 保留两个具体IP节点；按协议实现VPP rewrite逐包职责，不把DeviceClass TX或interface-TX DPO放进IP节点 |
| multicast adjacency DPO | 增加独立`DpoType::ADJACENCY_MCAST`与`AdjacencyLookupNext::Multicast`映射；IP plugin把IP4/IP6 class edges绑定到对应rewrite-mcast sibling，任何状态都不进入InterfaceMain |
| adjacency node process helpers | 删除接受`DpoProto`、任意next enum和`drop_next`的通用rewrite路径；IPv4/IPv6使用协议具体rewrite函数，resolution process单独生成probe并drop原packet |
| `DeviceClass.tx_function` | placeholder `Option<fn()>`收敛为真实`Option<NodeProcessFn>`，供动态`<name>-tx`直接执行driver TX body |
| `#[device_class(...)]` derive | 增加可选`tx_function = path`参数并展开为`DeviceClass::with_tx_function(path)`；不增加第二套TX registration surface |
| `TuntapConfig/TuntapMain` | 删除`ethernet`、`is_ether`、MAC、alias/address-sync与双模式字段；Config增加`admin_up`/`ip4_address`，Main增加`TuntapFile`、三槽`TuntapRxNext`和固定per-thread entries且不复制IP address；不使用`Vec<Option<_>>` |
| TuntapDeviceClass | 安装真实`tuntap_intfc_tx: NodeProcessFn` |
| tuntap plugin manifest/hooks | graph inventory注册`TuntapRxNode`、`TuntapTxNode`；config只调用一次generic `register_interface`、注册File并调用IP owner address API；`tuntap_input_init`只解析drop/IP4/IP6三个sibling slots |

### 12.3 删除

| Surface | 替代 |
| --- | --- |
| `InterfaceState.receive_dpos` | `NetMain`/`net::dpo` receive storage |
| `InterfaceMain.rx_dpos`及protocol/interface DB | `NetMain`/`net::dpo` interface-RX storage |
| InterfaceMain DPO add/query/format/lock/unlock methods | 直接调用DPO owner API |
| `InterfaceMain::interface_tx_nodes`与公共output fallback | DPO resolver + `tx_node_index_for_sw_interface` |
| output graph initializer中的DPO registration | DPO module init |
| 普通adjacency中的`DpoId::interface_tx` | 直接使用`tx_node_index_for_sw_interface`得到的`<name>-output` NodeId |
| `FeatureError::EmptyStartNodes` | zero-start output arc |
| registration callback rollback | VPP u32 registration语义 |
| `AdjacencyRewrite` | `Adjacency.rewrite_header`与`Adjacency.rewrite_data`直接字段 |
| tuntap crate整个`#[cfg(test)] mod tests` | 一次删除整个module及其中现有7个测试：`config_defaults_and_unimplemented_fields_are_explicit`、`defaults_publish_a_disabled_main`、`non_root_enable_matches_vpp_warning_success`、`syscall_error_preserves_tuntap_subsystem_and_source`、`class_image_creates_and_deletes_interface_main_relationship`、`tuntap_dso_installs_class_image`、`tuntap_linux_lifecycle`；一个不保留，不迁移、不改写、不增加替代Rust test |
| `.github/workflows/tuntap-linux.yml`中的全部3个`cargo test`步骤 | 删除DSO ignored test、non-privileged lib tests和isolated lifecycle ignored test；workflow只build并运行第13节daemon/ping场景，不能再执行任何Rust test runner |
| ADR-0027 tuntap deferred node placeholders/no-packet test设计 | 本文第9节真实nodes与packet path；不保留测试 |
| 旧的RX读取/分类两段式private helpers | 单个`tuntap_rx(runtime, node_runtime, frame) -> usize`直接完成VPP RX流程 |

不保留deprecated alias、compatibility facade、第二套pool、临时fallback或空Graph Node。

## 13. 唯一验收：startup configuration ICMP example

实施时必须一次删除`crates/hammer-plugins/device/tuntap/src/lib.rs`的整个`#[cfg(test)] mod tests`及其中
现有7个测试，一个不保留。不得把它们迁移或改写到其它crate，也不得新增unit test、crate-local
integration test、ignored test或source-text assertion。`.github/workflows/tuntap-linux.yml`现有3个
`cargo test`步骤全部删除，之后不得再执行任何Rust test runner。

唯一验收是一个真实daemon startup example；它同时经过plugin loading、一次generic interface
registration、FileMain、RX sibling、
device-input Feature Arc、IPv4 local/ICMP、P2P adjacency rewrite和DeviceClass TX。example配置文件固定为
`examples/tuntap-ip4-icmp/startup.toml`：

```toml
plugins = ["tuntap", "icmp"]

[memory]
main_heap_size = "256 MiB"

[worker]
count = 1

[worker.buffer]
slots_per_numa = 4096
frame_pool_size = 64

[statseg]
socket_name = "/tmp/hammer-tuntap-ip4-icmp.stats"

[plugin.tuntap]
enabled = true
name = "hammer0"
mtu = 1500
admin_up = true
ip4_address = "192.0.2.1/31"
```

`tuntap`与`icmp`都通过`load_after = ["ip"]`加载同一个IP dependency；必须显式把`icmp`列为root plugin，
因为它是IP的sibling consumer，不会仅因加载`tuntap`而出现。example只创建`IFF_TUN | IFF_NO_PI`设备并
只调用`register_interface`；不出现`eth_register_interface`、Ethernet input、MAC或ARP。P2P HwClass配合
`/31`让IP owner为对端建立zero-next-hop complete adjacency，rewrite data为空但
`rewrite_header.sw_if_index`、MTU和direct output next仍完整初始化。

现有`.github/workflows/tuntap-linux.yml`删除全部Rust test步骤，只在独立的privileged Linux network
namespace内执行以下一个startup example场景：

1. build `hammer`、`hammer-plugin-ip`、`hammer-plugin-icmp`和`hammer-plugin-tuntap` artifacts；
2. 以`HAMMER_PLUGIN_DIR="$PWD/target/debug"`启动
   `target/debug/hammer examples/tuntap-ip4-icmp/startup.toml`；
3. 等待daemon创建`hammer0`，由workflow把host侧interface置up并配置`192.0.2.0/31`；
4. 执行`ping -I hammer0 -c 3 -W 1 192.0.2.1`；
5. 无论成功失败都由workflow终止daemon、收集daemon与`ip -details`诊断并删除network namespace。

成功标准只有一个：`ping`退出0。echo request实际路径必须是
`hammer0 fd -> tuntap-rx -> start_device_input -> ip4-input -> ip4-local -> icmp4-input ->
icmp4-echo-request -> ip4-lookup`；echo reply发送路径必须经P2P complete adjacency和
`ip4-rewrite -> tuntap-0-output -> tuntap-0-tx ->
tuntap_intfc_tx -> tuntap_tx -> writev`。这里没有启用`ip4-output` feature，所以rewrite使用缓存的direct
output next；路径不经过Ethernet或ARP。output Feature Arc的设计仍由第7节VPP证据约束，不为这个
example伪造一个无行为feature。

本地agent不创建TUN/TAP、不启动daemon、不运行该lab；特权host配置、诊断与清理由CI workflow拥有。

## 14. 最终决策

| ID | 决策 | 对齐状态 |
| --- | --- | --- |
| D1 | InterfaceMain中不存在任何DPO state/operation/registration | Aligned |
| D2 | generic API唯一名称为`register_interface`且返回`u32` | Aligned |
| D3 | Ethernet API唯一名称为`eth_register_interface`且返回`u32` | Aligned |
| D4 | callback error不改变registration结果且不rollback | Aligned |
| D5 | 公共output、专属output、interface Feature、arc-end与TX构成完整两级fanout | Aligned |
| D6 | 一个成对的dense software-index lookup table在create和TX edge后发布两个VPP字段 | Intentional Rust owner adaptation |
| D7 | `ip4-output`/`ip6-output`由IP plugin分别拥有，last均为`interface-output` | Aligned |
| D8 | 当前只注册存在的rewrite starts，midchain/dvr不做placeholder | Intentional bounded difference |
| D9 | IP rewrite仅在cached `HAS_FEATURES`为真时启动output arc | Aligned |
| D10 | `device-input`的IP next由IP plugin注册，service/tuntap不拥有该registration | Aligned ownership adaptation |
| D11 | tuntap注册两个static nodes；`tuntap_intfc_tx`只作为dynamic TX process | Aligned |
| D12 | 本轮Linux TUN只有一个实例创建路径：`register_interface`；不调用Ethernet owner，不保留TAP或punt/inject配置分支 | Intentional TUN-only scope using VPP generic registration |
| D13 | 不新增registration、RX read或TX write错误族 | Aligned |
| D14 | 四协议L3 end modification按plugin owner拆分 | Intentional Rust dependency adaptation |
| D15 | 普通neighbor/multicast adjacency直接缓存专属`<name>-output` next，不构造interface-TX DPO | Aligned |
| D16 | interface-TX DPO只供显式DPO stack使用；当前不为缺失的midchain/PPPoE增加placeholder consumer | Aligned with bounded implementation scope |
| D17 | complete neighbor、multicast、incomplete、glean使用各自DPO class；分别进入普通rewrite、rewrite-mcast sibling、ARP/ND或glean node | Aligned |
| D18 | 邻居完成通过 rewrite bytes、direct output next、complete lookup state 和 Fib back-walk 发布；不靠 interface-TX DPO 进入 rewrite | Aligned |
| D19 | `ip4-rewrite`/`ip6-rewrite`负责协议TTL/Hop Limit、MTU、L2 rewrite prepend、TX metadata和条件性output Feature Arc，不负责DeviceClass TX | Aligned |
| D20 | 真正设备发送只在`<name>-tx -> DeviceClass TX function`完成；direct path和`ip4/6-output -> interface-output` path最终汇合到该节点 | Aligned |
| D21 | `tuntap-rx`对每个成功RX buffer调用`FeatureMain::start_device_input(sw_if_index, buffer, default_next)`，再按返回slot enqueue | Aligned |
| D22 | net 只提供 interface topology、generic adjacency/rewrite storage 和 graph primitives；IP plugin 独占 IP DPO class binding、IP output arcs、IP rewrite policy 与 IP errors | Aligned ownership boundary |
| D23 | resolution与rewrite从node declaration、registrar到process完全分离；IPv4 resolution为drop-only，IPv6为drop/rewrite-mcast，普通与mcast rewrite siblings共享本协议drop/ICMP/fragment layout | Aligned |
| D24 | tuntap config在interface成功后立即向FileMain注册duplicate descriptor；read-ready只置`tuntap-rx` interrupt pending | Aligned with Rust descriptor-ownership adaptation |
| D25 | IP plugin注册device-input的IP edges；`tuntap_input_init`只固化drop/IP4/IP6三个sibling slots，packet path不按name查询且不保存`Vec<Option<_>>` | Aligned ownership adaptation |
| D26 | `tuntap-rx`一次readv构造一个Buffer chain，只enqueue head；WouldBlock/zero/read error不产生半包或新错误 | Aligned |
| D27 | tuntap所有counter在本文明确延期，不为满足counter引入error type、shared state或stats API | Intentional bounded difference requested for this scope |
| D28 | `DeviceClass.tx_function`是`NodeProcessFn`；static `tuntap-tx`和dynamic `<name>-tx`复用同一driver body，终止路径按Hammer head ownership释放完整chain | Aligned with Rust Graph/Buffer ownership adaptation |
| D29 | `Adjacency`直接拥有`rewrite_header`/`rewrite_data`；`RewriteHeader::init`显式接收`sw_if_index`、MTU类别和source/target nodes，并在一次publication中写interface、L3 MTU和local next | Aligned with `VNET_DECLARE_REWRITE` and `vnet_rewrite_init` |
| D30 | `eth_register_interface`作为独立Ethernet API保留给后续device slice；本轮TUN不调用它，也不创建Ethernet record | Aligned ownership with bounded caller scope |
| D31 | tuntap startup的`admin_up`与`ip4_address`分别调用interface/IP owner；地址不隐含admin状态，IP reject保留原`IpInterfaceAddressError` source；所有fallible操作完成后才发布`TUNTAP_MAIN` | Intentional startup-config composition of two VPP operations |
| D32 | `tuntap_rx`本身直接完成allocation、readv、chain、classification、admin gate、`start_device_input`和enqueue，不存在packet-returning中间helper | Aligned |
| D33 | 一次删除tuntap crate整个test module及现有7个Rust tests，删除workflow现有3个`cargo test`步骤，不迁移、不改写、不增加任何替代test；唯一验收是privileged CI运行`examples/tuntap-ip4-icmp/startup.toml`并要求真实`ping`退出0 | User-requested verification scope |
| D34 | tuntap RX/TX Graph Node内不记录、格式化或返回I/O错误；RX恢复候选buffers并返回0，TX消费chain且不重试 | Ownership/disposition aligned; intentional Hammer observability difference |

设计审查结论：**Aligned；第12.1节 API inventory 已由 issue #340 明确批准实施**。

该结论只表示目标设计与vendored VPP的ownership、graph path、Feature semantics和error semantics一致，
不表示当前源码已完成迁移。

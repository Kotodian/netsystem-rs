# ADR-0028: VPP 风格 IP interface enable 与 tuntap 地址同步

Status: accepted

Date: 2026-09-19

本文记录issue #337已批准的设计边界与实现验收合同。

本文细化并部分取代 ADR-0005 的 IP lookup owner 结构、ADR-0006 中将 interface address
事实交给 `InterfaceMain` 的表述，以及 ADR-0027 第 8 节对 address sync 的延期。ADR-0025
确定的独立 `FeatureMain`、配置链与 Worker Barrier 合同继续有效。

## 1. 问题与范围

当前 Hammer 已有 IPv4/IPv6 input、lookup、drop/punt 与 unicast Feature Arc，但缺少 VPP
用来表达“接口尚未启用该协议族”的控制面与 graph 行为；同时，IP 地址被错误地放在
`InterfaceMain`，导致 generic interface service 知道 `IpNet`、地址池和 IP source selection。
tuntap 因而也无法像 VPP 一样订阅 IPv4/IPv6 interface address 变化。

本 ADR 只完成以下能力：

1. 在 IP plugin 内建立真实的、协议族各自持有的 `IpLookupMain<A>`；
2. 删除 `InterfaceMain`/`SwInterface` 的 IP 地址状态和地址增删/查询 facade；
3. 增加 `ip4-not-enabled`、`ip6-not-enabled` 两个具体 Feature Node；它们各自拥有独立
   Graph Node identity，并分别注册到现有`ip4-unicast`、`ip6-unicast` arc 的
   `ip4-lookup`/`ip6-lookup`之前；
4. 暴露 VPP 对应的 `ip4_sw_interface_enable_disable` 与
   `ip6_sw_interface_enable_disable`，保留两个协议族不同的零引用 disable 语义；
5. 暴露 IP 范围内的 `fib_table_get_index_for_sw_if_index` 查询；
6. 由 IP owner 实现 IPv4/IPv6 interface address add/delete 与同步 callback；
7. tuntap 注册两套 address callback，按 VPP 的 FIB 过滤、alias pool/hash、enable 引用和
   Linux ioctl 顺序同步地址；
8. 只为以上能力增加聚焦测试。

“ip46 能力”在本文中表示 IPv4/IPv6 两套对称能力，不表示复制一个 VPP 中不存在的
`ip46_sw_interface_enable_disable`、`ip46_add_del_interface_address` 或
`ip46-not-enabled` symbol。对外操作、callback 和 Graph Node 仍按 VPP 分成 IPv4 与 IPv6。

以下内容不在范围内：

- IP multicast/MFIB Feature Arc；
- 新的 Binary API、CLI、external client 或配置格式；
- tuntap RX/TX、FileMain、punt/inject packet path、normal-interface mode；
- table create/bind、完整 FIB route contribution、neighbor/ARP/ND、IPv6 RA/MLD/DAD，以及
  `ip6_link` delegate、MFIB 与 multicast adjacency consumer；第 8 节只纳入 public address
  operation 必需的 IPv6 link-local identity、生成/替换与引用生命周期；
- runtime plugin unload、动态 callback 注销；
- 为未来能力预建空节点、空 callback 或兼容 facade。

## 2. VPP 证据账本

| ID | vendored VPP source | 已验证行为 | 本 ADR 的约束 |
| --- | --- | --- | --- |
| V1 | `third_party/vpp/src/vnet/ip/lookup.h:45-129`, `ip_interface_prefix_t`, `ip_interface_address_t`, `ip_lookup_main_t` | lookup main 拥有 interface-address pool、address-to-index、per-interface address head、prefix pool/hash、classify map、mcast/ucast/output arc index、local-next 与 builtin protocol tables | Hammer 必须有独立 `IpLookupMain<A>` 拥有本轮实际使用的address、unicast-arc与local-next状态；地址不能留在 `InterfaceMain`，也不预建无消费者的VPP字段 |
| V2 | `ip4.h:67-112`, `ip6.h:75-130`, `ip4_main_t`, `ip6_main_t` | IPv4/IPv6 Main 各自嵌入一个 `ip_lookup_main_t`；各自另外拥有 FIB/MFIB、`fib_index_by_sw_if_index`、`ip_enabled_by_sw_if_index` 和 family-specific callbacks | `Ip4Main`/`Ip6Main` 各自按值持有一个不同实例；不能合并成共享运行时 enum owner |
| V3 | `ip4_punt_drop.c:15-18,69-118`; `ip6_punt_drop.c:15-18,21-70`; `ip_punt_drop.h:180-237` | `ip4-not-enabled`/`ip6-not-enabled` 是两个 Graph Node，各自是 family drop node 的 sibling，执行与 drop 相同的 `ip_drop_or_punt`，并且是对应 drop arc 的 start node；drop/punt arc 以特殊 interface index `0` 启动，不以 packet RX interface 选配置 | Hammer 注册两个 concrete Feature Node；共享private process function并传入已选drop arc index，且drop arc使用`sw_if_index == 0` |
| V4 | `ip4_input.c:40-61`; `ip6_input.c:116-132,160-174`; `feature/feature.h:240-299`; `ip4_forward.c:876-947`; `ip6_forward.c:518-589` | input 从 packet RX `sw_if_index` 启动 family unicast arc；Feature framework 用该 index 选择 per-interface compiled configuration；两个 not-enabled feature 分别 `runs_before` lookup，而 lookup 是对应 arc 的 `last_in_arc` | 是否经过 not-enabled 由该 RX interface 的配置链决定；arc 只执行所选链，lookup 是终点而不是 enable 判定者 |
| V5 | `ip4_forward.c:567-605`, `ip4_sw_interface_enable_disable` | `u8` 引用计数；仅 `0 -> 1` 和 `1 -> 0` 改 feature；enable 禁用 not-enabled，disable 启用 not-enabled；零引用 disable 是 `ASSERT`；返回 `void` | IPv4 wrapper 返回 `()`；underflow 是 programmer invariant，不定义 recoverable error |
| V6 | `ip6_forward.c:207-242`, `ip6_sw_interface_enable_disable` | 与 IPv4 相同的 transition，但零引用 disable 明确 no-op；返回 `void` | shared enable core 必须保留 family policy，不能为了统一而把 IPv4 也改为 no-op |
| V7 | `ip4_forward.c:1020-1067`; `ip6_forward.c:646-696` | software-interface create 将 family FIB mapping 设为 table 0并启用 not-enabled；delete 删除 family addresses、恢复 table 0、mapping 写回 `~0` 并移除 not-enabled 配置 | IP plugin 通过既有 generic interface callback lifecycle维护状态；删除必须先于 interface slot 释放 |
| V8 | `fib_table.c:1003-1017`; `ip4_fib.c:236-248`; `ip6_fib.c:258-270` | `fib_table_get_index_for_sw_if_index(proto,index)`分派到 family helper；越界/未映射返回`~0`，不是 error | Rust IP API 以 `Option<u32>` 表达 `~0`；IP Main 未初始化不是 `None` |
| V9 | `ip_interface.c:13-126`; `ip4_forward.c:609-803`; `ip6_forward.c:262-483` | ordinary address pool 属于 lookup main；prefix length、冲突、重复、缺失与 IPv6 link-local 分支有确定 errno；ordinary success 更新 enable、routes并调用 family callback，link-local success 则提前返回 | 地址 mutation 和 error 归 IP plugin；不能把 link-local 排除出 public IPv6 entry point，也不能让它进入 ordinary pool/callback |
| V10 | `interface_api.c:400-438` | 外部 API 先 `VALIDATE_SW_IF_INDEX`，再按 decoded family 调用 `ip4_add_del_interface_address` 或 `ip6_add_del_interface_address`，owner errno 原样返回 | invalid interface 在外部边界校验；family operation 不新增 `InvalidInterface` error |
| V11 | `ip_interface.c:37-47,83-126`; `ip4_forward.c:621-625,637-763`; `ip6_forward.c:276-320,326-448`; `vnet/error.h:43-46,95,117-119` | address entry points 可返回的稳定类别为 unsupported、address length mismatch、address in use、duplicate interface address、address not found、address not deletable；具体类别取决于 family、link-local 分支和 VPP 检查顺序 | public Rust error完整表达这六类；不得把 duplicate/missing 改成幂等结果，也不得增加统一预校验来改变分支优先级 |
| V12 | `tuntap.c:45-105`, `subif_address_t`, `tuntap_main_t.subifs/subif_mhash` | tuntap 自己拥有 alias address pool/hash，key 含来源 `sw_if_index`、family 与 address | alias 状态留在 `TuntapMain`，不进入 IP Main 或 InterfaceMain |
| V13 | `tuntap.c:675-760`, `tuntap_ip4_add_del_interface_address` | disabled/normal mode no-op；family FIB 不同 no-op；alias=`<tun_name>:<pool-index>`；先更新 tuntap interface IPv4 enable refcount；add 用 `SIOCSIFADDR`/`SIOCSIFNETMASK`，delete 释放 alias，再更新 flags | IPv4 callback 顺序、过滤和 warning-only syscall semantics 原样保留 |
| V14 | `tuntap.c:790-886`, `tuntap_ip6_add_del_interface_address` | 相同过滤与 alias；先更新 IPv6 enable refcount；AF_INET6 socket 后以 `SIOCSIFADDR`/`SIOCDIFADDR`增删；delete 最后释放 alias | IPv6 callback 是独立入口；不能用 IPv4 ioctl 分支冒充统一实现 |
| V15 | `tuntap.c:986-1007`, `tuntap_init` | 初始化 alias hash，并把两个 family callback 分别加入 `Ip4Main`/`Ip6Main` callback list；callback 返回 `void` | tuntap 对 IP plugin 建立显式依赖并注册两次；不经 service capability carrier |
| V16 | `tuntap.c:736-759,847-884` | callback 内 socket/ioctl 失败只 warning，继续后续步骤，不返回 error、不回滚 alias 或 enable 引用 | 不增加 `TuntapAddressError`，也不把 callback 改成 `Result` |
| V17 | `interface.h:182-185,239-253`; `interface.c:205-217,257-262`; `feature/feature.c:698-741`; `ip4_forward.c:1067`; `ip6_forward.c:696` | 普通 software-interface callbacks 使用 LOW priority，Feature cleanup 使用 HIGH priority，dispatcher 先 LOW 后 HIGH | IP family delete callback 先删地址、mapping 和 not-enabled 配置，Feature cleanup 最后清理残留 arc state |
| V18 | `ip6_forward.c:283-320,453-477`; `ip6_link.c:24-40,138-243,260-371,445-525`; `ip6_link.h:9-20` | `ip6_link`独立拥有每接口 link-local address 与 lock count；link-local add要求 `/128`并set/replace，delete当前地址为not-deletable、delete其他地址为not-found；ordinary add/delete在family enable与callback之外成对enable/disable link；software-interface delete强制清理残留link | IP plugin增加独立于`IpLookupMain`的IPv6 link owner和LOW-priority lifecycle callback；public IPv6 address operation保留early-return、callback抑制和额外enable引用顺序 |

VPP 同时对 multicast arc 修改 not-enabled feature；Hammer 当前没有该 arc 和完整 MFIB packet
path。本 ADR 只对现有 unicast arc实施同义行为。这是明确的范围差异，不得用一个假
multicast arc、空 node 或仅测试名称来声称已实现。

## 3. 当前 Hammer 基线与错误边界

| ID | Hammer source | 当前事实 | 必须改变的原因 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-plugins/net/ip/src/lookup.rs:17-103` | `Ip4Main`/`Ip6Main`直接摊平 local/arc/FIB 字段，没有 `IpLookupMain`；FIB mapping 只有初始 `[0]` | 不符合 V1-V2 owner 结构，也不能维护 interface lifecycle |
| H2 | `lookup.rs:116-121`, `fib_index_for` | `IP*_MAIN.get()` 与 mapping lookup 都折叠进 `Option` | 把 lifecycle bug 与 VPP `~0` ordinary absence 混为一类 |
| H3 | `hammer-service/src/interface_model.rs:332-347,675-684` | `InterfaceState.addresses` 和 `SwInterface.addresses` 保存 IP 地址 | generic interface owner 被 IP policy 污染 |
| H4 | `interface_model.rs:1042-1074,1215-1247` | `InterfaceMain`公开地址查询/add/remove；duplicate add 幂等返回旧 index，missing remove 返回 `Ok(false)` | owner 与 VPP error semantics 均错误 |
| H5 | `hammer-service/tests/interface_address_lifetime.rs` | 测试固化上述 service-owned 地址模型 | 必须删除，不迁移成 facade 测试 |
| H6 | `ip/src/icmp_error.rs:293-318` | ICMP source selection 直接遍历 `SwInterface.addresses` 并回查 `InterfaceMain` pool | source selection 必须回到 family lookup owner |
| H7 | `ip/src/input.rs:46-52`; `ip/src/punt.rs:95-137,220-262` | 已有两个 unicast arc 和两个 family drop node，但没有 not-enabled node/feature；drop core 代码重复 | 增加concrete nodes并让drop/not-enabled把各自arc index传给同一private process function |
| H8 | `hammer-service/src/feature.rs:430-469,542-621` | Feature mutation需要`&mut DataPlaneMain`并在需要时进入Worker Barrier | interface lifecycle 必须传递真实 main borrow，不能缓存指针或延迟 mutation |
| H9 | `interface_model.rs:23,616-723,915-954,1286-1307` | generic interface callback不接收`DataPlaneMain`，create/delete方法也无法向 callback 传递它 | 无法在 IP callback 中完成VPP同步的Feature publication |
| H10 | `hammer-service/src/lib.rs:16-38` | plugin interface registration image固定把两组callback arrays设为空 | IP DSO无法通过既有image声明sw-interface callback |
| H11 | `hammer-plugins/device/tuntap/src/lib.rs:44-78,421-433` | `TuntapMain`没有subif pool/hash；plugin无IP依赖且`load_after=[]` | V12-V15能力缺失 |
| H12 | ADR-0025 / `FeatureMain` | feature arc在graph materialization后的`feature_arc_init`才构建；`local0`在更早的`net_main_init`创建 | startup既有interface需要一次有界catch-up；不能伪装成与VPP完全相同的时间点 |
| H13 | scoped search under `crates/hammer-plugins/net/ip/src/` | 当前没有IPv6 link owner、link-local identity或link enable/disable生命周期 | 若仍公开完整IPv6 address operation，必须补入V18所需owner语义，不能用ordinary-only前置条件规避 |

当前 `InterfaceError::NotRegistered` 继续属于外部/generic interface 边界。IP address API 不包装
或复制它。调用 family operation 前的外部 handler 先验证 index；IP 内部 callback得到的
`sw_if_index`来自仍存活的 `InterfaceMain` slot，因此缺失是 lifecycle programmer bug。

## 4. Owner 与 Rust 静态抽象

### 4.1 `IpLookupMain<A>` 是ordinary interface address owner

IP plugin 增加 private `IpLookupMain<A>`。`Ip4Main` 按值持有
`IpLookupMain<Ipv4Addr>`，`Ip6Main` 按值持有 `IpLookupMain<Ipv6Addr>`。目标所有权形状为：

```rust
struct IpLookupMain<A> {
    interface_addresses: Pool<IpInterfaceAddress<A>>,
    interface_address_index_by_key: HashMap<IpInterfaceAddressKey<A>, u32>,
    interface_address_head_by_sw_if_index: Vec<Option<u32>>,
    unicast_feature_arc_index: u8,
    local_next_by_ip_protocol: [u16; 256],
}

pub struct Ip4Main {
    lookup_main: UnsafeCell<IpLookupMain<Ipv4Addr>>,
    unicast_tables: Vec<Ip4FibTable>,
    fib_index_by_sw_if_index: UnsafeCell<Vec<Option<u32>>>,
    ip_enabled_by_sw_if_index: UnsafeCell<Vec<u8>>,
    add_del_interface_address_callbacks:
        UnsafeCell<Vec<IpInterfaceAddressCallback<Ipv4Addr>>>,
    // Existing IPv4-only state remains here.
}

pub struct Ip6Main {
    lookup_main: UnsafeCell<IpLookupMain<Ipv6Addr>>,
    unicast_tables: Vec<Ip6FibTable>,
    fib_index_by_sw_if_index: UnsafeCell<Vec<Option<u32>>>,
    ip_enabled_by_sw_if_index: UnsafeCell<Vec<u8>>,
    add_del_interface_address_callbacks:
        UnsafeCell<Vec<IpInterfaceAddressCallback<Ipv6Addr>>>,
    // Existing IPv6-only state remains here.
}

struct Ip6LinkMain {
    links_by_sw_if_index: UnsafeCell<Vec<Option<Ip6Link>>>,
}

struct Ip6Link {
    sw_if_index: u32,
    link_local_address: Ipv6Addr,
    locks: u32,
}

static IP6_LINK_MAIN: OnceLock<Ip6LinkMain> = OnceLock::new();
```

代码块只列本 ADR 涉及的字段；它不删除既有 family-specific FIB、ICMP throttle、local/drop/
punt arc index 等状态。`local_feature_arc_index`不进入`IpLookupMain`：VPP `lookup.h`只保存
mcast/ucast/output arc index，local arc由独立registration持有。Hammer因Graph owner形态而保存
drop/punt/local index的现有字段也继续属于family Main，不借此扩张`IpLookupMain`。

`IpInterfaceAddressKey<A>`对应VPP family-specific `address + fib_index` key；prefix length与
`sw_if_index`保存在pool record并由owner执行VPP冲突规则。per-interface list用
`Option<u32>`表达VPP的`~0` head/next/prev，不向外暴露pool borrow。`Pool`使用
`hammer-infra`现有实现；普通map/vec分配继续经过Hammer Main Heap。

`lookup_main` 是family Main内的唯一owner；`UnsafeCell`只用于startup单线程或
main-thread Worker Barrier期间的owner-local mutation。私有family operation在一次同步
操作内取得所需的直接borrow，不返回可跨mutation保留的reference，也不增加lock、
closure-mediated access或observer wrapper。

`Ip6LinkMain`与VPP `ip6_link.c`的file-static `ip6_links`一样，是IP plugin自己的独立
process-global owner；它不是`InterfaceMain`字段，也不塞入`IpLookupMain<Ipv6Addr>`。
`OnceLock`只发布一次完整构造的Main，dense vector用`sw_if_index`定位，`None`表达该interface
尚无IPv6 link。`Ip6Link`保存当前link-local identity与引用数；link-local生成优先使用super
Ethernet interface MAC，否则使用IP owner的随机源，顺序对齐`ip6_link_enable`。本轮没有消费方的
delegate、MFIB与multicast adjacency不伪造字段，作为第11节明确差异保留。

VPP `ip_lookup_main_t` 中的`is_ip6`、`format_address_and_length`由concrete `A`的单态化实例
静态确定，不复制runtime family flag或function pointer。
`fib_result_n_bytes/fib_result_n_words`服务于C FIB result layout；Hammer已有typed `DpoId`与
concrete FIB backend。prefix pool/hash属于本轮延期的interface route contribution，classify、
multicast/output arc与builtin-protocol table也没有本轮消费者。这些字段都不预建；后续只在
对应owner和行为同时实现时扩展`IpLookupMain<A>`。

以下 service 状态/API全部删除，不留deprecated alias、转发方法或兼容re-export：

- `InterfaceState.addresses`；
- `SwInterface.addresses`；
- `InterfaceMain::{add_address,remove_address,interface_address,interface_addresses,interface_address_index}`；
- `crates/hammer-service/tests/interface_address_lifetime.rs`。

`InterfaceConfig.address`当前没有应用路径。本迁移同时从generic service配置DTO删除该字段；
以后若增加interface address配置，配置与执行入口必须由IP plugin拥有，并分别调用family API，
不能把`IpNet`重新放回`hammer-service`。

ICMP source selection改为IP owner的窄操作：IPv4只借用IPv4 lookup main，IPv6只借用IPv6
lookup main；unnumbered关系仍从`InterfaceMain`读取，因为它是interface topology事实。该改动
不新增observer/view/wrapper，也不返回可越过owner mutation的地址borrow。

### 4.2 直接泛型与零成本分派

本设计不增加`IpFamilyMain` trait。`Ip4Main`与`Ip6Main`没有需要作为一个抽象能力传递的调用
边界；为它们增加trait只会把已有direct borrow藏在间接层后面。复用发生在实际同构的数据与
算法上：

- `IpLookupMain<A>`及其private methods以`A: Copy + Eq + Hash`单态化，统一address pool/hash/
  per-interface list的插入、查找和删除；
- `IpInterfaceAddress<A>`、`IpInterfaceAddressKey<A>`、`IpInterfaceAddressError<A>`与
  `IpInterfaceAddressCallback<A>`直接以concrete address type单态化；
- enable/disable core接收family Main字段的直接mutable borrow，并以
  `const ZERO_DISABLE_IS_NOOP: bool`保留IPv4 assert与IPv6 no-op差异；
- FIB mapping query接收`&[Option<u32>]`，callback遍历接收具体family callback slice；二者不需要
  Main trait；
- family drop与not-enabled nodes把各自concrete drop arc index传给同一个private process
  function；该函数没有family-dependent type，因此不伪装成泛型。

IPv4/IPv6 public functions只取得各自Main的直接borrow并调用这些core；编译后没有runtime family
dispatch。IPv4/IPv6冲突分类、IPv6 link-local、Linux ioctl等真正不同的policy留在concrete
operation。所谓“统一逻辑”不允许抹平VPP差异，也不允许引入`Ip46Main`、`IpAddressManager`、
trait object、boxed callback、closure-mediated access或宏生成两套难以审计的业务逻辑。

## 5. not-enabled Feature Node 与 Feature Arc

### 5.1 两个 concrete Feature Node

Graph inventory新增：

```rust
pub struct Ip4NotEnabledNode; // node name: "ip4-not-enabled"
pub struct Ip6NotEnabledNode; // node name: "ip6-not-enabled"
```

两个Feature Node分别实现`Node`，从各自concrete Main读取drop arc index，再调用private
`process_ip_drop_frame`。该function与现有family drop node共享：接收已经选定的drop arc
index，逐buffer以特殊interface index `0`启动family drop Feature Arc，最终到service `drop`
node；它不接收`IpVersion`，也不执行runtime family枚举分派。
该`0`与VPP `ip_drop_or_punt`一致：drop/punt features不属于某个RX interface，因此不得
继续沿用现有Hammer drop node读取RX `sw_if_index`的行为。

Hammer graph已有`sibling_of`注册能力，因此两个not-enabled node分别声明为对应family drop
node的sibling，并与family drop node调用同一private process function、使用相同drop arc和next
contract。两个node仍拥有独立Graph Node identity、name与trace入口。

not-enabled不是挂在input前面的独立检查节点，也不是直接写死next到service drop。这一
“unicast Feature Node + drop-arc start node”的双重注册正是VPP V3-V4的结构。

not-enabled node不定义专属node error，不覆盖`Buffer`已有error，不增加“IP not enabled”
per-packet控制面错误。它与VPP一样进入既有drop/punt pipeline；最终drop计数由既有owner记录。

### 5.2 RX interface选择配置链，lookup结束arc

packet path的决定顺序固定为：

1. `Ip4InputNode`或`Ip6InputNode`从`NetworkOpaque.sw_if_index[RX]`取得packet的RX
   `sw_if_index`；只有该family的unicast lookup目标才启动对应unicast Feature Arc；
2. `FeatureMain::start_feature_arc`以`(arc_index, sw_if_index)`查询该interface是否有feature，
   并从`config_index_by_sw_if_index[sw_if_index]`选择已经编译的配置链；
3. 当该interface的family enable引用为0时，控制面配置包含对应not-enabled occurrence；
   `0 -> 1` transition将它从该interface配置删除，`1 -> 0`再把它加入；
4. 配置包含not-enabled时，它在lookup之前执行，并以特殊index `0`启动family drop arc；packet
   不再到达unicast lookup；
5. 配置不含not-enabled时，其余已启用feature依序执行，最终进入作为`last_in_arc`的
   `Ip4LookupNode`或`Ip6LookupNode`。

因此，unicast arc只提供注册、排序与配置链执行机制；它不自行判断IP是否enabled。是否进入
not-enabled完全取决于packet RX interface当前发布的per-interface配置。不同RX interfaces在同一
arc上可以同时表现为“进入not-enabled”和“直接结束于lookup”，packet path不读取一份全局
family-enabled flag，也不临时检查address count。

### 5.3 Arc registration

- `ip4-drop` start nodes改为`[Ip4DropNode, Ip4NotEnabledNode]`；
- `ip6-drop` start nodes改为`[Ip6DropNode, Ip6NotEnabledNode]`；
- `Ip4NotEnabledNode`作为feature注册到`ip4-unicast`，`runs_before=[Ip4LookupNode]`；
- `Ip6NotEnabledNode`作为feature注册到`ip6-unicast`，`runs_before=[Ip6LookupNode]`；
- 两个lookup仍是各自unicast arc的`last_in_arc`。

当前不存在multicast arc，因此不注册multicast copy。不得用同一个node name、一个接收
`IpVersion`的graph node或`ip46-not-enabled`替代这两个节点。

## 6. interface enable/disable 与 FIB 查询

### 6.1 Family-specific public functions

IP plugin公开以下VPP同义入口：

```rust
pub fn ip4_sw_interface_enable_disable(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    is_enable: bool,
);

pub fn ip6_sw_interface_enable_disable(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    is_enable: bool,
);
```

两个函数返回`()`。有效的`sw_if_index`、已初始化的family Main、已构建的Feature Arc以及注册
成功的not-enabled feature都是内部调用前置条件；违反条件是调用方/生命周期bug，不能转成
`IpInterfaceEnableError`、`FeatureError`或`Option`。

共同core按以下顺序执行：

1. 将dense `u8`引用计数扩展到`sw_if_index`；
2. enable增加引用；只有结果为1时继续；
3. disable执行family policy：IPv4在0时assert，IPv6在0时return；非零减一，只有结果为0时继续；
4. `0 -> 1`禁用对应unicast arc的not-enabled feature；
5. `1 -> 0`启用对应unicast arc的not-enabled feature。

VPP还维护super hardware interface的`l3_if_count`。Hammer当前`HwInterface`没有该VPP字段，且
本范围没有消费者；本ADR不为计数增加service字段。它是三向差异表中的显式延期，不影响
not-enabled、family refcount或tuntap行为。IPv4 enable/disable callback list同样没有本轮调用者，
不预建空API。

### 6.2 FIB query

IP plugin公开：

```rust
pub fn fib_table_get_index_for_sw_if_index(
    version: IpVersion,
    sw_if_index: u32,
) -> Option<u32>;
```

现有`IpVersion`使IP4/IP6以外的protocol不可表示，因此无需新增只有两个variant的
`FibProtocol`。函数先按version取得已经初始化的`IP4_MAIN`或`IP6_MAIN`；Main缺失时
`expect` lifecycle invariant。随后读取family mapping：entry缺失或值为`None`返回`None`，对应
VPP `~0`；table 0返回`Some(0)`，不能被falsey/sentinel逻辑吞掉。

本文不承诺MPLS分支。未来MPLS owner若需要VPP完整dispatcher，应在拥有MPLS mapping后扩展
protocol类型与dispatcher；本轮不能用`DpoProto::NONE`或catch-all分支伪造它。

## 7. software-interface lifecycle

### 7.1 传递真实 DataPlaneMain borrow

IP interface callback必须同步修改FeatureMain，而现有generic callback没有所需owner borrow。
既有接口因此收紧为直接传递`&mut DataPlaneMain`：

```rust
pub type InterfaceCallback = fn(
    &mut DataPlaneMain,
    &InterfaceMain,
    u32,
    bool,
) -> InterfaceResult<()>;
```

`InterfaceMain::register_hardware_interface`、`delete_hardware_interface`、flags mutation及内部
callback dispatch都把调用者持有的同一main borrow向下传递；`NetMain::init`和tuntap config/
exit等现有调用者相应传入main。没有`DataPlaneMain` global、raw pointer cache、capability
wrapper、closure、thread-local selector或第二个mutation入口。旧签名直接删除，不提供overload。

`declare_interface_registration_image!`增加可选generic hw/sw callback slice参数，使IP DSO把
family callback通过现有`InterfaceRegistrationImage`安装。service仍只保存generic function
pointer和priority，不出现IP类型、IP trait或plugin-owned state。

### 7.2 IP callback行为

IP DSO声明IPv4 family、IPv6 family和IPv6 link三个low-priority software-interface callbacks。
前两个callback由接收owner字段direct borrows的shared core完成：

- create：扩展family FIB mapping和enable-count vectors；mapping设`Some(0)`、count设0；当
  Feature Arc已经注册并构建时，为新interface启用family not-enabled feature；
- delete：逐个调用family address delete，使每个成功delete仍执行enable transition与address
  callbacks；若mapping不是table 0则先走既有table-bind owner操作恢复0；最后mapping设`None`并
  移除not-enabled feature；callback返回`Ok(())`；
- callback只处理IP-owned状态，不删除`InterfaceMain` slot，不维护alias或设备状态。

IPv6 link callback在create时不创建link；delete时若link仍存在，则调用owner-local force cleanup，
清除link identity/locks并完成最后一次family disable。它与IPv6 family address teardown的LOW
priority先后次序不影响结果：一方先完成后，另一方只观察到已经清理的link；该private
`IP6_NOT_ENABLED`状态按VPP不是public address error。三者都先于HIGH-priority generic Feature
cleanup执行。

IP两个family callbacks使用LOW priority，FeatureMain的generic cleanup使用HIGH
priority。dispatcher按LOW到HIGH执行，因此delete时IP先删除family addresses、恢复/撤销
FIB mapping并移除not-enabled configuration，然后`vnet_feature_add_del_sw_interface`同义的
Feature callback清理所有arc的残留config。interface slot只能在两类callback都完成后释放。

delete所遍历的address index、key、interface owner都来自同一family `IpLookupMain<A>`；因此该
owner-internal delete必须成功。实现不得用`.ok()`、`let _ =`或log丢弃public address error；若
刚枚举出的record无法删除，这是IP owner invariant violation，应在slot仍存活时assert，而不是把
部分teardown作为recoverable `InterfaceError`返回。

### 7.3 Hammer startup的有界catch-up

Hammer与VPP的graph construction时点不同：`local0`在`net_main_init`创建，而Feature Arc只能在
graph materialization后的`feature_arc_init`构建。为此采用以下固定顺序：

```text
interface_main_init
  -> ip_lookup_init (publishes both family Mains)
  -> ip6_link_init (publishes the independent IPv6 link Main)
  -> net_main_init (creates local0; IP callback installs FIB mappings)
  -> remaining init/config
  -> graph materialization
  -> ip_feature_init (registers both not-enabled features)
  -> feature_arc_init (builds Feature config owners)
  -> ip_interface_feature_init (enables not-enabled for every live interface)
  -> start workers
```

`ip_interface_feature_init`只通过`InterfaceMain::software_interface_indices`复制authoritative
live software-interface indices并安装两个默认not-enabled features；它不维护pending list、
dirty bitmap、atomic ready flag或第二份interface inventory。该枚举入口与两个公开callback
dispatcher都在自身入口验证main-thread Worker Barrier publication scope，不能只依赖当前调用者
间接建立前置条件。startup此时尚无worker，因此同一检查无需实际暂停worker。之后创建的interface
由同一IP callback立即安装feature；main-loop-enter的arc construction窗口不允许创建interface，
违反该生命周期约束是programmer bug。

这是一项由Hammer graph materialization顺序造成的有界时间差异，不改变workers启动时可见的
最终VPP状态，也不把检查放到packet path。

## 8. IP interface address operation

### 8.1 Public operation与callback

IP plugin提供family-specific entry point，不提供`InterfaceMain` facade：

```rust
pub fn ip4_add_del_interface_address(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv4Addr,
    address_length: u8,
    is_delete: bool,
) -> Result<(), IpInterfaceAddressError<Ipv4Addr>>;

pub fn ip6_add_del_interface_address(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv6Addr,
    address_length: u8,
    is_delete: bool,
) -> Result<(), IpInterfaceAddressError<Ipv6Addr>>;
```

callback使用一个generic函数指针layout并由family registration API静态绑定：

```rust
pub type IpInterfaceAddressCallback<A> = fn(
    &mut DataPlaneMain,
    sw_if_index: u32,
    address: A,
    address_length: u8,
    if_address_index: u32,
    is_delete: bool,
);

pub fn register_ip4_add_del_interface_address_callback(
    callback: IpInterfaceAddressCallback<Ipv4Addr>,
);

pub fn register_ip6_add_del_interface_address_callback(
    callback: IpInterfaceAddressCallback<Ipv6Addr>,
);
```

function pointer monomorphization不产生dynamic dispatch。callback list由family Main拥有，只在
startup注册，workers启动后不允许改变。callback不接收或保存`Ip4Main`/`Ip6Main`引用；需要的
FIB与enable能力通过IP owner公开operation取得，满足plugin state不能泄漏到generic carrier的
仓库规则。

### 8.2 Ordinary address的VPP检查与mutation顺序

family public operation不通过一个统一`IpAddr` dispatcher；IPv4与IPv6 concrete entry point在
取得各自Main后复用`IpLookupMain<A>`的pool/list方法，但保留VPP的family branch顺序：

1. 外部边界已经验证live `sw_if_index`；owner调用generic interface capability检查，不支持
   addressing时返回`Unsupported`；
2. IPv6先识别link-local并进入第8.3节；其余地址用`address + current fib_index`构造family key；
3. add先扫描同family、同FIB的现有非stale地址。IPv4 overlap返回`AddressInUse`；IPv6 overlap
   返回`DuplicateInterfaceAddress`；VPP在此扫描后才进入generic pool add；
4. exact key已存在的非stale add返回`DuplicateInterfaceAddress`，但IPv4 normal exact duplicate
   通常已由上一步按`AddressInUse`返回；不得用一个提前的exact-key helper改变这种优先级；
5. add只有在真正进入`ip_interface_address_add`同义core时才校验ordinary prefix：0或超过family
   位宽返回`AddressLengthMismatch`，否则插入`IpLookupMain<A>` pool/hash/per-interface list；
6. ordinary delete按`address + fib_index`找record；key缺失或record属于另一interface返回
   `AddressNotFoundForInterface`，否则从pool/hash/list删除。VPP该delete path不先做ordinary
   prefix-length校验，Hammer不得为了“整齐”增加统一预校验并改变其结果；
7. mutation成功后调用对应family `sw_interface_enable_disable`；IPv6 add随后调用第8.3节的
   link enable；
8. 当前Hammer尚无本路径所需的connected/local/glean/MFIB route contribution；这是显式差异，
   不以空route或假DPO代替。后续加入时必须位于callback之前并保持本节branch/error顺序；
9. 按registration order同步调用全部family callback，传实际pool index；
10. IPv6 delete在callback之后调用link disable；随后返回成功。

第8步只界定当前forwarding缺口，不授权缩窄public address operation的输入或错误合同。地址
owner、enable transition和callback顺序仍按VPP实现；未来加入route contribution不得改变本API、
错误类别或callback相对顺序。

### 8.3 IPv6 link-local与link引用

`ip6_add_del_interface_address`必须接受link-local address，不能把它作为undocumented precondition
排除。其VPP同义分支为：

- capability检查仍先执行；link-local prefix不是`/128`时返回`AddressLengthMismatch`；
- add调用private link set operation：link不存在时创建`Ip6Link`、使用请求地址、令`locks = 1`
  并增加一次`ip6_sw_interface_enable_disable`引用；link已存在时替换当前link-local address，
  不增加link lock；
- delete请求等于当前link-local时返回`AddressNotDeletable`；请求其他link-local时返回
  `AddressNotFoundForInterface`；link-local不能经该API删除，只能被新地址替换；
- link-local成功路径不写`IpLookupMain<Ipv6Addr>` address pool、不调用family address callback，
  因而tuntap也不会收到link-local同步事件；
- ordinary IPv6 add在family enable之后调用link enable。每次调用增加link lock；第一次创建
  link-local identity并额外增加一次family enable引用，已有link返回的VPP `VALUE_EXIST`不作为
  address error传播；
- ordinary IPv6 delete在family callback之后调用link disable。最后一个link lock释放时清除
  link-local identity并额外减少一次family enable引用；VPP `IP6_NOT_ENABLED`同样不作为address
  error传播。software-interface delete强制清理仍存活的link record。

VPP的link-local table route、MFIB enable、multicast adjacency和delegate通知与上述owner lifecycle
发生在同一`ip6_link`实现中，但Hammer当前没有这些consumer。本ADR不伪造它们，也不声称完成
完整IPv6 link forwarding；这是第11节记录的实现差异，不得反向改变本节已经确定的address
branch、引用计数或错误语义。

### 8.4 Error contract

public `IpInterfaceAddressError<A>`由IP plugin拥有，完整表达本slice中VPP public family address
entry points的六个稳定类别：

| Rust variant | VPP errno | 触发条件 | mutation/callback |
| --- | --- | --- | --- |
| `Unsupported { sw_if_index }` | `VNET_API_ERROR_UNSUPPORTED` | `local0`或VPP同义的不支持addressing interface | 无mutation，无callback |
| `AddressLengthMismatch { address, address_length }` | `VNET_API_ERROR_ADDRESS_LENGTH_MISMATCH` | ordinary add到达pool add后prefix为0/超过family位宽；IPv6 link-local add/delete非`/128` | 无mutation，无callback |
| `AddressInUse { address, conflicting_sw_if_index }` | `VNET_API_ERROR_ADDRESS_IN_USE` | IPv4同FIB prefix overlap，包括normal exact duplicate通常命中的pre-scan | 无mutation，无callback |
| `DuplicateInterfaceAddress { address, existing_sw_if_index }` | `VNET_API_ERROR_DUPLICATE_IF_ADDRESS` | IPv6同FIB prefix overlap或非stale exact duplicate；IPv4仅在到达exact-key branch时 | 无mutation，无callback |
| `AddressNotFoundForInterface { sw_if_index, address }` | `VNET_API_ERROR_ADDRESS_NOT_FOUND_FOR_INTERFACE` | ordinary delete key缺失/record属于另一interface，或删除非当前IPv6 link-local | 无mutation，无callback |
| `AddressNotDeletable { sw_if_index, address }` | `VNET_API_ERROR_ADDRESS_NOT_DELETABLE` | 试图删除当前IPv6 link-local | 无mutation，无callback |

error字段保存结构化interface/address/prefix/conflicting-interface事实，不保存display字符串。
不得增加`InvalidInterface`、`UnsupportedLinkLocal`、`AddressNotAssignable`、`Feature`、
`Callback`、`Io`、`Internal`、`Other`或message-only variant。`VALUE_EXIST`与`IP6_NOT_ENABLED`
只出现在VPP private link引用操作，public address entry point不传播它们；Hammer不把它们改造成
第七类address error。pool exhaustion遵循现有Hammer Main Heap不可恢复边界；FeatureMain/
lookup invariant失败panic。tuntap callback返回`void`且syscall warning-only，因此不会成为
address operation的returned error或rollback原因。

## 9. tuntap address synchronization

### 9.1 Dependency与owner state

`hammer-plugin-tuntap`增加对`hammer-plugin-ip`的显式crate依赖，并声明
`load_after=["ip"]`。它直接调用IP owner公开的family API；`hammer-service`、runtime
registration和`PluginMain`不保存IP-specific capability。

`TuntapMain`增加VPP同义的private state：

```rust
struct SubinterfaceAddress {
    sw_if_index: u32,
    address: IpAddr,
}

struct TuntapMain {
    // Existing fields remain.
    subinterface_addresses: Pool<SubinterfaceAddress>,
    subinterface_address_index_by_key: HashMap<(u32, IpAddr), u32>,
}
```

alias name严格使用`format!("{}:{}", tun_name, pool_index)`；pool slot释放后允许复用，与VPP
pool index语义相同。prefix length不进入key，因为VPP `subif_address_t` hash同样只使用family、
address与`sw_if_index`；IP owner已经拒绝同FIB exact duplicate。

这些字段只由main thread上的address callback访问，不被Data Worker读取，因此不增加mutex、
RwLock、atomic、thread-local或snapshot。`UnsafeCell`只在现有process-global Main需要从共享引用
执行main-thread owner mutation时使用，并沿用owner safety contract。

### 9.2 Registration

tuntap init在IP Mains初始化后调用：

```text
register_ip4_add_del_interface_address_callback(tuntap_ip4_add_del_interface_address)
register_ip6_add_del_interface_address_callback(tuntap_ip6_add_del_interface_address)
```

两个callback是concrete function，不通过一个接收`IpAddr`的public callback做runtime family
dispatch。它们可调用共享的private alias allocation/key helper，但Linux操作保持family-specific。

### 9.3 IPv4 callback

`tuntap_ip4_add_del_interface_address`严格执行：

1. `TuntapMain`未发布、`dev_tap_fd < 0`时return；未来normal-interface mode实现后也必须return；
2. 比较来源interface与`tuntap.sw_if_index`的IPv4 FIB；不相等则return；
3. 按`(sw_if_index, IpAddr::V4(address))`查找或分配pool slot；
4. 用pool index构造alias name；
5. 调用`ip4_sw_interface_enable_disable(main, tuntap.sw_if_index, !is_delete)`；
6. add依次尝试`SIOCSIFADDR`、按prefix生成VPP同义netmask后`SIOCSIFNETMASK`；
7. delete从hash/pool移除alias record；
8. 尝试`SIOCGIFFLAGS`，add设置、delete清除`IFF_UP|IFF_RUNNING`，再尝试`SIOCSIFFLAGS`。

每个ioctl失败只记录包含`io::Error` source与operation的warning，并继续。callback无返回错误，
不回滚pool/hash或IP enable引用。

### 9.4 IPv6 callback

`tuntap_ip6_add_del_interface_address`使用相同的disabled/FIB/key/alias/enable前半段，随后：

1. 创建`AF_INET6/SOCK_STREAM` socket；失败warning但不改变callback返回语义；
2. `SIOGIFINDEX`取得alias interface index；失败warning并继续；
3. 填入address、prefix length和interface index；
4. add尝试`SIOCSIFADDR`，delete尝试`SIOCDIFADDR`；失败warning；
5. 关闭有效socket；
6. delete最后从hash/pool移除alias record。

IPv6 path不执行IPv4的netmask或flags ioctl。socket/ioctl failure不产生
`TuntapAddressError`、不返回`Err`、不撤销第5步之前已经完成的enable transition，也不把
warning字符串提升为稳定error contract。

## 10. 同步与failure atomicity

- IP Mains、`Ip6LinkMain`、lookup/address pools、FIB mappings、enable counts与callback lists继续是
  owner-local process globals；不增加`Arc<Main>`、registry lookup或handle。
- callback registration只发生在startup单线程阶段；workers启动后callback vector不可变。
- live interface/FIB/address/Feature mutation由main thread发起。worker-visible字段在现有
  Worker Barrier内直接更新；不在barrier-owned值外再加lock、atomic pointer或snapshot。
- `FeatureMain::enable_feature/disable_feature`继续拥有其配置链的barrier与graph refork行为；IP
  不复制Feature config index或bitmap。
- address operation按第8节的VPP branch precedence完成所有会返回public error的检查后才执行
  pool/link mutation。进入第一次mutation后，IP-owned步骤必须不可失败；private IPv6 link
  status不提升为新的public address error，也不把callback warning当作transaction failure。
- tuntap alias pool/hash只供main thread控制面使用。其VPP-defined warning-only syscall发生在
  IP address state成功提交以后，不要求rollback，也不能替换primary IP error。
- software-interface delete先完成family address callbacks与mapping teardown，再由
  `InterfaceMain`释放slot。任何cleanup secondary error都不能把已存在的primary interface
  error改名；IP callback本身不返回recoverable error。

## 11. 三向语义差异

| 维度 | 当前 Hammer | 本 ADR 目标 | vendored VPP | 结论 |
| --- | --- | --- | --- | --- |
| address owner | `InterfaceMain`混合IPv4/IPv6 pool | ordinary地址由每个family Main内嵌`IpLookupMain<A>`拥有；link-local由独立`Ip6LinkMain`拥有 | `ip[46]_main_t.lookup_main`加`ip6_link.c` file-static owner | owner对齐；link-local不污染lookup pool或generic interface |
| address API | service add/remove/query | public concrete family mutation与public family callback registration | VPP公开`ip4_*`/`ip6_*`，IPv6内部委托`ip6_link` | 删除service facade；完整接受ordinary/link-local输入，不增加统一ip46入口 |
| address errors | duplicate add成功；missing delete=false | 六个typed VPP类别并保留family/branch优先级 | 六个concrete errno | 改为VPP语义；不做统一预校验或幂等化 |
| IPv6 link owner | 无 | independent link-local identity与locks；ordinary add/delete成对引用 | `ip6_links`、`ip6_link_enable/disable/set_local_address` | address可观察语义对齐；delegate/MFIB/mcast adjacency明确延期 |
| not-enabled Feature Node | 无 | 两个concrete Feature Nodes，共享private process function | 两个sibling nodes注册为family features | identity、Feature位置与行为对齐 |
| not-enabled选择 | 无 | input以RX interface启动arc，该interface compiled config决定是否含not-enabled；lookup为`last_in_arc` | 同 | per-interface对齐；arc本身不判断enable；multicast明确延期 |
| enable refcount | 无 | family `u8` count与transition | 同 | 对齐，包括IPv4/IPv6 underflow差异 |
| hardware L3 count | 无字段 | 不增加 | enable transition维护`l3_if_count` | 有界延期，无当前消费者 |
| FIB query | private helper且混淆Main缺失 | public IP query，mapping absence=`None` | `~0` | Rust类型化等价；MPLS延期 |
| interface callback context | 无`DataPlaneMain` | direct mutable borrow向下传递 | C从global取得`vlib_main` | Rust owner差异，语义同步 |
| existing interface startup | local0早于arc build | arc build后、worker前一次authoritative scan | feature已可用于callback | 有界lifecycle差异，无pending state |
| tuntap callback | 无 | 两个family callback与alias owner | 两个callback | 对齐 |
| tuntap syscall error | N/A | warning、continue、无rollback | warning、continue、void | 对齐，不发明error |
| generic dispatch | 多处`IpVersion` runtime match | `IpLookupMain<A>`直接泛型、const-generic family policy与direct borrows；nodes仍拆分 | C family implementations | Rust零成本复用且不增加无调用边界的Main trait |

## 12. 新类型/API与修改审批

根`AGENTS.md`要求非平凡VPP工作中的新类型/API先获明确批准。以下限定surface已随issue #337
获得批准；超出本表的surface仍需再次审批。

### 12.1 新增

| Item | Visibility/owner | Final responsibility |
| --- | --- | --- |
| `IpLookupMain<A>` | private, IP plugin | family ordinary address、unicast arc与local-next tables的真实owner |
| `IpInterfaceAddress<A>`及其key | private, IP plugin | VPP同义ordinary address pool/list/hash records |
| `Ip6LinkMain`、`Ip6Link` | private, IP plugin | 独立IPv6 link-local identity、生成/替换与lock lifecycle；不进入`IpLookupMain` |
| direct generic address methods与const-generic enable core | private, IP plugin | 只统一实际同构算法；接收owner字段的直接borrow，不增加Main trait/enum/ABI |
| `Ip4NotEnabledNode`、`Ip6NotEnabledNode` | public Feature Node identities, IP plugin | 独立VPP同名Graph Node并分别注册为family unicast feature，共享private drop core |
| `ip4_sw_interface_enable_disable`、`ip6_sw_interface_enable_disable` | public, IP plugin | family refcount与not-enabled transition；返回`()` |
| `fib_table_get_index_for_sw_if_index` | public, IP plugin | `IpVersion`选择family，`Option<u32>`表达VPP `~0` |
| `IpInterfaceAddressError<A>` | public, IP plugin | 第8.4节六个VPP address error类别，不含catch-all或link-private status |
| `ip4_add_del_interface_address`、`ip6_add_del_interface_address` | public, IP plugin | concrete family address validation、pool/link mutation、enable与callback；IPv6包含link-local branch |
| `IpInterfaceAddressCallback<A>` | public generic fn-pointer alias, IP plugin | family registration的零动态分派callback layout |
| 两个`register_ip[46]_add_del_interface_address_callback` | public, IP plugin | startup-only family callback registration |
| `ip_interface_feature_init` | private lifecycle callback, IP plugin | Feature Arc build后为既有live interfaces安装not-enabled |
| `InterfaceMain::software_interface_indices` | public narrow borrow-to-copy operation, service | 在main-thread Worker Barrier publication scope内供IP startup catch-up枚举authoritative live software-interface pool；不暴露pool或IP状态 |
| `SubinterfaceAddress`及TuntapMain pool/hash字段 | private, tuntap plugin | VPP alias identity与pool-index name |
| 两个`tuntap_ip[46]_add_del_interface_address` | private callbacks, tuntap plugin | family-specific Linux address同步，warning-only |

### 12.2 修改/删除

| Item | Change |
| --- | --- |
| `Ip4Main`/`Ip6Main` | 各自嵌入`IpLookupMain<concrete address>`，并持有family FIB mapping、enable counts、address callbacks |
| IP drop Feature Arcs | 各增加family not-enabled start node；drop与not-enabled调用同一private process function |
| `InterfaceCallback`与generic interface mutation APIs | 直接增加`&mut DataPlaneMain`参数并机械迁移所有调用者；不保留旧overload |
| `declare_interface_registration_image!` | 接受可选generic callback slices；service surface不出现IP类型 |
| IP plugin lifecycle | `ip_lookup_init`发布family Mains，`ip6_link_init`随后发布独立`Ip6LinkMain`；两者排在`interface_main_init`后、`net_main_init`前；增加feature-build后的catch-up |
| tuntap dependency/lifecycle | 直接依赖IP plugin、`load_after=["ip"]`并在startup注册两个address callbacks |
| Interface address state/API | 删除第4.1节列出的全部state/method/test和未使用的`InterfaceConfig.address` |
| ICMP source selection | 改用family IP owner operation；InterfaceMain只提供unnumbered topology |

### 12.3 明确不批准

- `Ip46Main`、`Ip46Address`、`ip46-not-enabled`或统一public ip46 node/API；
- service/runtime内的IP trait、IP callback carrier、IP Main reference或erased registry entry；
- `dyn` dispatch、boxed callbacks、closure-based owner access；
- 没有真实consumer的`IpFamilyMain`或其他Main abstraction trait；
- 地址兼容facade、pending interface list、ready atomic、packet-path enable检查；
- `TuntapAddressError`、syscall rollback、multicast占位arc、MPLS假分支；
- 新锁、thread-local、snapshot publication或第二套barrier completion。

## 13. 聚焦测试矩阵

测试只覆盖本ADR请求的IP能力，不顺带扩展FIB route、multicast、neighbor、Binary API、CLI或
特权tuntap packet I/O测试。tuntap本轮以plugin所有target成功编译作为验收。

| Test | Level | Required assertion | Evidence |
| --- | --- | --- | --- |
| `interface_enable_fib_and_address_operations_match_vpp` | IP owner/Feature integration | 两个concrete not-enabled features注册到各自unicast arc；两个RX interface的compiled chain分别可观察enabled interface结束于family lookup、disabled interface进入family not-enabled；family enable/disable切换配置；FIB mapping随interface create/delete；IPv4/IPv6 ordinary及link-local add/delete匹配第8.4节相关VPP错误类别 | V3-V11,V18 |
| `cargo check -p hammer-plugin-tuntap --all-targets` | compile gate | tuntap直接依赖IP，两个concrete callback、alias owner及Linux ioctl路径均可编译 | V12-V16 |

不得用读取`.rs`后`contains`symbol的source-text test证明node、owner或callback存在。node顺序通过真实
Feature Arc/graph执行验证；callback通过真实registration与observable owner state验证；Linux
行为只在已有privileged tuntap CI环境运行，本地agent不创建host TUN/TAP。

实现完成、review/format均结束且下一步就是commit时，才运行仓库规定的最终测试gate。

## 14. 实现顺序与完成判定

批准后的实现顺序固定为：

1. 修改generic interface callback签名与image参数，机械迁移现有调用者；
2. 建立`IpLookupMain<A>`与独立`Ip6LinkMain`，从service迁移地址owner/ICMP source selection；
3. 接入software-interface family FIB lifecycle和公开FIB query；
4. 注册两个not-enabled node/feature并完成startup catch-up；
5. 实现family enable/disable、IPv6 link lifecycle与完整address add/delete/callback contract；
6. 让tuntap显式依赖IP，增加alias state与两个family callbacks；
7. 只增加第13节测试，执行最终pre-commit gate后立即commit。

完成判定：

- `InterfaceMain`和`SwInterface`不再保存或暴露任何IP地址；
- `Ip4Main.lookup_main`与`Ip6Main.lookup_main`是两个独立`IpLookupMain<A>`实例；
- `Ip6LinkMain`独立拥有link-local identity/locks，link-local不进入`IpLookupMain`或tuntap callback；
- Graph中存在两个独立not-enabled Feature Nodes，且RX interface compiled config决定是否执行，
  对应family lookup保持`last_in_arc`；
- enable/disable引用计数与IPv4/IPv6 underflow语义逐项匹配VPP；
- FIB query不混淆Main lifecycle与mapping absence；
- public family address errors逐分支匹配第8.4节六类VPP contract；
- tuntap两个callback的filter、alias、enable与warning-only Linux顺序匹配VPP；
- 没有本ADR禁止的facade、dynamic dispatch、pending state、锁或自创error；
- 测试集合没有超出第13节。

## 15. Decision record 与 verdict

| ID | Final decision | Alignment |
| --- | --- | --- |
| D1 | ordinary地址owner从InterfaceMain迁入每个family Main内嵌的`IpLookupMain<A>`；IPv6 link-local由独立`Ip6LinkMain`拥有 | 对齐`ip_lookup_main_t`与`ip6_link.c` owner；解决用户指出的耦合 |
| D2 | `IpLookupMain<A>`、address records/callback/error直接泛型化，enable core用const generic；不增加`IpFamilyMain` | Rust零成本抽象只覆盖真实同构逻辑，public surface与Graph Node保持family分离 |
| D3 | input用RX interface选择compiled config；只有disabled interface配置含family not-enabled，lookup是`last_in_arc`；not-enabled复用drop function/arc且不新增packet error | 对齐VPP input、per-interface Feature config、registration、sibling与`ip_drop_or_punt`行为 |
| D4 | enable/disable返回`()`并保留IPv4 assert、IPv6 no-op差异 | 精确对齐VPP，不发明控制面error |
| D5 | FIB query以`Option<u32>`表达`~0`，Main缺失为lifecycle invariant | 类型化表达VPP absence，不隐藏bug |
| D6 | interface callback直接传递`&mut DataPlaneMain`；startup用authoritative scan catch up | 满足Hammer owner/barrier合同，无pending/mirror |
| D7 | public family address operation保留VPP检查顺序与六类typed error；IPv6纳入独立link owner、link-local early return和ordinary link引用 | 纠正当前Hammer幂等语义，不排除link-local或发明第七类error |
| D8 | tuntap直接依赖IP并注册两个family callbacks；Linux失败warning-only且不回滚 | 对齐VPP callback owner、顺序与错误语义 |
| D9 | multicast、MPLS、route contribution以及`ip6_link` delegate/MFIB/mcast-adjacency consumer显式延期 | 不用占位实现夸大本slice；不改变本ADR已覆盖的link owner/error/refcount contract |

**Design verdict: Aligned for the scoped IPv4/IPv6 interface-enable, FIB-query and
tuntap address-sync slice.** 该结论覆盖本文列出的unicast、
ordinary与IPv6 link-local address owner/error路径；它不宣称Hammer已经实现VPP完整IP/MFIB、
route或IPv6 link delegate/adjacency subsystem。

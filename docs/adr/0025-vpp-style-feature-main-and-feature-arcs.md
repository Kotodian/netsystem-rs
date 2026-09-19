# ADR-0025: FeatureMain、FeatureConfigMain 与 VPP Feature Arc 语义对齐

Status: accepted

Date: 2026-09-19

本文记录已批准的实现设计。它取代
ADR-0006 中 Feature Arc 的所有权、模块位置、初始化入口、控制 API receiver、
接口删除清理和 Feature Arc 验证清单，并取代 ADR-0002 中“由
`InterfaceMain` 发布 Feature Arc 变更”的表述。ADR-0006 的 IPv4/IPv6
local/receive/end-of-arc 节点、校验、协议表和 ICMP 边界继续有效；ADR-0002
的 Worker Barrier 与 Graph Refork 规则继续有效。

## 1. 范围与完成定义

本 ADR 解决三个问题：

1. 将当前嵌在 `InterfaceMain` 内的全部 Feature Arc 状态和操作迁移到独立的
   进程级 `FeatureMain`；
2. 显式恢复 VPP 的 `FeatureMain -> FeatureConfigMain -> ConfigMain` 所有权层次，
   不再把 per-arc feature 绑定和通用配置链编译折叠进一个 `FeatureArc`；
3. 对 vendored VPP 的 Feature Arc 机制做完整语义核对，逐项决定注册、排序、
   配置编译、启停、end-node、packet cursor、接口删除、缓存入口、同步和错误
   行为，而不是只移动一个字段。

范围内的 owner 和 surface：

- `hammer-service::feature::FeatureMain`；
- `hammer-infra::heap::Heap<T>` 为配置链补齐的通用 remove、复用和连续 run 访问；
- 当前 `hammer-service::interface::feature` 的注册、配置、查询和 packet-path
  API；
- `#[feature_arc]`、`#[feature]` 生成的 owner 参数和错误路径；
- `InterfaceMain` 的软件接口删除通知；
- IP 插件中所有 Feature Arc 注册、索引查询、启停、start/next 调用；
- `Buffer::current_config_index` 的既有 Feature Arc 用法。

本 ADR 不增加 VPP 的具体 arc 清单，不新增 Feature Binary API 或 CLI，不改变
IPv4/IPv6 packet policy，不改变 Buffer header 布局，也不允许 worker 运行后增加
或重排 Feature 注册。

“完整对齐”在本文中的定义是：vendored VPP `feature.h`、`feature.c`、
`registration.c` 和 `config.c` 中与 Feature Arc 机制有关的每项行为都被核对并
得到“采用”或“有约束的差异”结论。它不是逐字段复制 C 指针、全局裸回调或
CLI/API 表面。

## 2. 当前 Hammer 基线

当前实现已经具备 VPP 配置链的大部分核心语义，但 owner 错误：

- `crates/hammer-service/src/interface_model.rs:306-320` 把
  `feature: FeatureState` 放在 `InterfaceState`；
- `crates/hammer-service/src/interface/feature.rs:67-118` 持有 arc/feature
  registrations、已构建 arcs、共享 `u32` heap、配置 pool、引用计数、dense
  `config_index_by_sw_if_index`、feature count 和 bitmap；
- 当前 private `FeatureArc` 同时承担 arc metadata、VPP
  `vnet_feature_config_main_t` 的 interface-to-config 绑定以及
  `vnet_config_main_t` 的通用配置编译/去重职责，缺少明确的 per-arc
  `FeatureConfigMain` 与内部 `ConfigMain` 边界；
- 同文件 `120-691` 把注册、构建、启停、end-node、start/next 全部实现为
  `InterfaceMain` 方法；
- 同文件 `753-769` 清理一个 `sw_if_index` 的全部配置，而
  `interface_model.rs:689-725` 在硬件接口删除内部直接调用该私有状态；
- 同文件 `771-794` 的 `interface_feature_init` 经由 `NetMain -> InterfaceMain`
  构建 arcs；
- `crates/hammer-component-macros/src/lib.rs:1362-1404` 生成以
  `&InterfaceMain` 为 receiver 的注册方法；
- `crates/hammer-service/tests/interface_features.rs:5-109` 只覆盖了一个链的
  end-node、自环避免、配置读取和空链 fast path，未覆盖独立 owner、完整查询面、
  接口删除和 publication failure atomicity。

当前配置链值得保留：enabled occurrence 按 feature index 排序，可重复；完全相同
的链按 words 去重并引用计数；共享 heap extent 前一个 word 保存 pool back-pointer，
末尾把 end node 纳入去重 key；packet 只携带 `current_config_index`。这些行为与
VPP 的 `vnet_config_t`/`vnet_config_main_t` 语义相符，不应因为 owner 迁移而重写。

## 3. VPP 事实与证据账本

| ID | 来源 | 路径与 symbol | 已验证行为 | 设计约束 |
| --- | --- | --- | --- | --- |
| E1 | VPP | `third_party/vpp/src/vnet/feature/feature.h:14-108`, `vnet_feature_main_t`, `vnet_feature_config_main_t` | `feature_main` 独立持有 registrations/name indexes、`feature_config_mains`、共享 heap、per-arc feature nodes、interface bitmap/count；每个 `vnet_feature_config_main_t` 明确嵌入一个 `vnet_config_main_t` 并另持有 dense interface config-index vector | Feature 状态不能继续由 `InterfaceMain` 持有；per-arc config binding 与 generic config machinery 不能折叠进 arc metadata |
| E2 | VPP | `feature.h:113-172`, `VNET_FEATURE_ARC_INIT`, `VNET_FEATURE_INIT`, `VNET_FEATURE_ARC_ORDER` | 声明先进入 `feature_main` registration inventories，随后由 startup init 统一构建 | Hammer 的注册方法必须以 `FeatureMain` 为 owner，arc 构建只发生在 `feature_arc_init` |
| E3 | VPP | `feature.c:40-185`, `vnet_feature_init` | init 分配 compact `u8` arc index，按 arc 分组 feature/constraint，初始化每个 config main，保存 name/index 和排序结果 | Hammer 保留 compact index、startup-only `feature_arc_init` 和 name lookup；不引入额外 install 状态 |
| E4 | VPP | `registration.c:111-356`, `vnet_feature_arc_init` | `runs_before`/`runs_after`、`last_in_arc` 和可选 bulk order 形成偏序；拓扑排序后分配 compact feature index；随后初始化 start/feature/end graph facts | 排序与 graph 构建属于 `FeatureMain` startup lifecycle |
| E5 | VPP | `feature.h:62-66`, `vnet_feature_config_main_t`; `config.h:11-68`, `vnet_config_feature_t`, `vnet_config_t`, `vnet_config_main_t`; `feature.c:125-145`; `registration.c:315-338` | 每个已构建 arc 恰有一个 `vnet_feature_config_main_t`；其 `config_index_by_sw_if_index` 把 interface 绑定到 compiled config，而嵌入的 `vnet_config_main_t` 独立持有 start nodes、default/per-config end nodes、feature-index-to-node、config pool/hash、heap及pool lookup/back-pointer关系；Feature Arc init 把 shared heap接到每个 config main后再初始化 | Hammer 必须有 private `FeatureConfigMain { config_main, config_index_by_sw_if_index }` 和 private `ConfigMain`；不得继续让 `FeatureArc` 兼任这两层，也不得公开 config-main wrapper |
| E6 | VPP | `config.c:30-136`, `add_next`, `find_config_with_features` | 多 start node 必须得到相同 next；config string 依次包含 next 和 opaque config；end node 进入 hash key；相同链共享并引用计数 | 保留现有连续 `u32` heap、dedupe、refcount 和 start-slot 一致性检查 |
| E7 | VPP | `config.c:304-429`, `vnet_config_add_feature`, `vnet_config_del_feature` | add 可增加重复 occurrence，按 feature index 排序；del 只删除一个 index+config 精确匹配；未找到是 no-op | Hammer 继续保留 occurrence multiplicity 和 exact-delete |
| E8 | VPP | `feature.c:242-311`, `vnet_feature_enable_disable_with_index` | 每 interface 的 config index 被替换；feature count 和 presence bitmap 同步更新 | 三项 publication 必须在一个不可分割 owner mutation 中完成 |
| E9 | VPP | `feature.c:314-452`, `vnet_feature_is_enabled`, `vnet_feature_get_end_node`, `vnet_feature_modify_end_node`, `vnet_feature_reset_end_node` | 查询 enabled；读取、修改、恢复 per-interface end node；end-node 变化复用同一配置机制 | Hammer 补齐 end-node getter，保留 modify/reset failure atomicity |
| E10 | VPP | `feature.h:220-323`, count/presence/config-index accessors and arc start/next | hot path 可读 count/presence/config index；普通 start 按 `sw_if_index` 选择配置；cached config index 可直接启动；next 顺序消费 shared heap | Hammer 补齐 count/config-index/cached-start surface，继续使用 Buffer cursor |
| E11 | VPP | `feature.c:698-741`, `vnet_feature_add_del_sw_interface` | software interface 删除通过 HIGH priority callback 清 bitmap/count/config index，并释放配置 | `InterfaceMain` 只发通用删除通知，`FeatureMain` 自己清理 feature state |
| E12 | VPP | `feature_api.c:21-63`, feature enable/disable handler | 外部入口先校验 `sw_if_index`，再解析 feature owner callback，最后调用通用 Feature Arc mutation | Hammer 的控制入口仍负责外部校验；FeatureMain 不成为 Binary API owner |
| E13 | VPP | `feature.c:10-36`, `vnet_feature_register`; scoped callers in `adj/adj.c`, `dev/dev.c`, `ipsec/ipsec_tun.c`, `plugins/tap/tap.c` | VPP 有通用 update observer，现有调用者用它刷新 adjacency/device/tunnel caches | Hammer 当前没有这种 cache，且仓库规则禁止 service 保存 plugin-owned callback/reference |
| E14 | VPP | scoped search `rg "VNET_FEATURE_ARC_ORDER\\s*\\(" third_party/vpp/src` | vendored tree 除 `feature.h` 的宏定义/variant 外没有 bulk-order declaration | 当前不新增无调用者的第三套注册 API；所有实际 VPP declarations 可由 per-feature constraints 表达 |
| E15 | VPP | scoped search `rg "enable_disable_cb\\s*=" third_party/vpp/src` | vendored tree没有 registration initializer 设置 `enable_disable_cb`；CLI/API 仍保留调用分支 | Hammer 不把 plugin policy callback 存入 FeatureMain；owner control method 显式组合操作 |
| H1 | Hammer | `interface/feature.rs:67-118`, `FeatureState` and private records | 当前全部 feature state 已集中但嵌套在 interface owner | 状态整体搬迁，不建立双 owner 或 mirror |
| H2 | Hammer | `interface/feature.rs:346-437`, enable/disable/is/end methods | numeric control API、重复 occurrence、精确删除和 end override 已存在 | receiver 和模块迁移；不增加兼容 facade |
| H3 | Hammer | `interface/feature.rs:466-650`, `replace_feature_chain` | candidate、graph edges、heap、dedupe、publish 已有 failure-atomic 设计，但内部使用 caller-operation closure | 保留算法，改为 owner 的窄 domain operation，生产代码不保留 closure-mediated state access |
| H4 | Hammer | `interface/feature.rs:652-690`, packet methods | ordinary start、next、const-word config read 已存在并为 allocation-free | 迁移到 `FeatureMain` 并补齐 VPP cached-index/query surface |
| H5 | Hammer | `interface_model.rs:20-36`, `InterfaceCallbackRegistration`; `:170-192`, `InterfaceRegistrationImage`; `:506-534`, `consume_registration_image`; `:536-579`, `register_hardware_interface`; `:689-727`, `delete_hardware_interface_with_file_cleanup`; `:969-1039`, callback dispatch; scoped search `rg "consume_registration_image" crates` | callback image、按 priority 排序和 add/delete dispatch API 已存在，但 `consume_registration_image` 当前没有 caller；hardware-interface create/delete path均未调用 software-interface callback，delete 直接改 `state.feature` 后释放 software-interface slot | 不新增另一套 callback registry；`interface_main_init` 必须消费 built-in image，create/delete必须接入既有 generic callback path，delete callback必须先于slot释放 |
| H6 | Repository contract | root `AGENTS.md`, synchronization, plugin ownership and error rules | barrier-owned worker-visible state不能再套 lock/snapshot；service 不能保存 plugin-specific callback/reference；生产代码禁止 caller-supplied state closure；缺失已初始化 process owner是programmer invariant | VPP C callback/pointer shape必须用 Hammer owner operations 表达；不得为了Feature interface校验增加 `RuntimeError -> FeatureError` 翻译 |
| H7 | Hammer | `crates/hammer-infra/src/heap.rs:1-72`, `Heap<T>`; `crates/hammer-infra/tests/heap.rs` | 现有generic heap以稳定`u32` offset标识连续run，提供tail `alloc`和单元素`get`；它当前明确grow-only，没有remove/reuse或连续run的读写borrow | Feature配置必须复用并在`hammer-infra`通用扩展该类型；不能在FeatureMain中继续维护`Vec<u32> + free_config_ranges`私有heap实现，也不增加可恢复容量错误 |

vendored VPP 没有 Feature Arc 专用的完整单元测试目录；`third_party/vpp/test`
只在具体插件测试中观察启停。本文测试矩阵因此主要从 E3-E11 的实现行为推导，
而不是声称复制了上游测试。

## 4. 决策

### 4.1 独立 FeatureMain

`hammer-service` 增加公开 owner type `hammer_service::feature::FeatureMain` 和私有
process cell。`FeatureMain` 直接拥有领域字段，不保留没有独立语义的 `FeatureState`
中间层。其结构形状为：

```rust
pub struct FeatureMain {
    arc_registrations: UnsafeCell<Vec<FeatureArcRegistration>>,
    feature_registrations: UnsafeCell<Vec<FeatureRegistration>>,
    arc_index_by_name: UnsafeCell<BTreeMap<&'static str, u8>>,
    feature_index_by_name: UnsafeCell<Vec<BTreeMap<&'static str, u32>>>,
    feature_config_mains: UnsafeCell<Vec<FeatureConfigMain>>,
    shared_feature_config_heap: UnsafeCell<Heap<u32>>,
    feature_nodes_by_arc: UnsafeCell<Vec<Box<[NodeId]>>>,
    feature_count_by_sw_if_index: UnsafeCell<Vec<Vec<i16>>>,
    sw_if_index_has_features: UnsafeCell<Vec<Bitmap>>,
}

static FEATURE_MAIN: OnceLock<FeatureMain> = OnceLock::new();
```

现有 `FeatureArcRegistration`、`FeatureRegistration` 从
`hammer-service::interface::feature` 迁移到 `hammer-service::feature`；新增
`FeatureConfigMain`、`ConfigMain`、`ConfigEntry` 和 `ConfigFeature`。后两者分别直接
对应 VPP `vnet_config_t` 和 `vnet_config_feature_t`；删除无VPP对应物的
`FeatureChain`/`FeatureOccurrence` 命名。这些全部继续是 private implementation types；
两个 config main 是内部所有权边界，不是对外借用 wrapper。`FeatureMain::global()`
返回进程生命周期的
`&'static FeatureMain`。不增加 `Arc<FeatureMain>`、handle、registry capability、
trait object 或 thread-local selector。

`FeatureMain` 是 process-global Feature authority，不嵌入 `NetMain`，也不由
`InterfaceMain` 持有。`InterfaceState.feature` 删除；全部 Feature API 从
`impl InterfaceMain` 移到 `impl FeatureMain`。`FeatureMain` 不缓存
`&InterfaceMain`、`&NodeMain` 或其内部 pointer；需要验证软件接口时通过现有
`NetMain::global().expect("Feature mutation requires initialized NetMain")`
取得当次 `InterfaceMain` borrow，需要 graph 时使用调用者传入的 main-thread
`DataPlaneMain`/`NodeMain` borrow。初始化顺序已建立 NetMain 存在性；缺失它是 lifecycle
programmer invariant，不新增 `RuntimeError -> FeatureError` 转换。

各字段的 `UnsafeCell` 只表达 `FeatureMain` 的 owner-local publication，不代表多个
authority：startup 单线程写；运行时写发生在所有 Data Workers 已确认的 Worker Barrier
内；worker 在 barrier release 之间只读。owner method只取得完成当前 operation 所需的
互不重叠字段，不能返回其中的 borrow。实现必须为 `Send`/`Sync` safety comment明确这些
条件。不得重新引入 `FeatureState`/`Inner` wrapper，也不得增加 `Mutex`、`RwLock`、
atomic pointer、snapshot 或第二套完成协议。

### 4.2 初始化、注册和 arc 构建

初始化顺序为：

```text
interface_main_init
  -> net_main_init
  -> feature_main_init

all graph nodes materialized
  -> plugin feature registration callbacks
  -> feature_arc_init
  -> start_workers
```

`interface_main_init` 在发布 `InterfaceMain` 前消费 service 自有的 built-in
`InterfaceRegistrationImage`。Feature 模块向该 image 提供一个 HIGH-priority
software-interface callback registration；复用既有排序和 dispatch，不增加 Feature
专用 callback list。`register_hardware_interface` 在 software-interface slot 完整构造后
调用 generic create callbacks；Feature callback 对 `is_create == true` 必须立即返回
`Ok(())`，不能先取得 `FeatureMain`，因为 `net_main_init` 创建 `local0` 早于
`feature_main_init`。

`feature_main_init` 只创建并发布空的 `FeatureMain`。`feature_arc_init` 取代
`interface_feature_init`，仍是 main-loop-enter function，因为 Hammer 的 graph
nodes 在 init functions 之后才 materialize；它在全部 plugin feature registration
之后、`start_workers` 之前运行。该生命周期差异不改变 VPP 的关键边界：所有静态
nodes 和 declarations 已可见，workers 尚未启动。arc 排序、`FeatureConfigMain` 构造和
graph next 建立都只在这个既有 startup callback 内完成；不增加公开或私有的
`install_feature_arcs` API，也不维护 installed/closed 状态。

`#[feature_arc]` 和 `#[feature]` 保留现有声明语法以及真实 Graph Node type path，
但生成方法的 owner 参数和错误路径改为：

```rust
pub fn register_feature_arc(
    features: &FeatureMain,
    nodes: &NodeMain,
) -> Result<u8, FeatureError>;

pub fn register_feature(
    features: &FeatureMain,
    nodes: &NodeMain,
) -> Result<(), FeatureError>;
```

arc index 仍在 `register_feature_arc` 时按 registration sequence 分配 compact `u8`
并保持不变。VPP 在 `vnet_feature_init` 中分配 index；Hammer 提前到同一 startup
窗口，是为了让 concrete plugin Main 在一次 registration callback 内保存 index，
而不在 FeatureMain 中缓存 plugin-owned index pointer。两者对 worker 可见的最终
index、排序和生命周期相同；index 不持久化，也不是 ABI。

注册函数只允许由既有 startup registration callbacks 调用。若 worker 启动后仍尝试
注册 arc、feature 或 constraint，说明调用者破坏了 runtime lifecycle，是本地断言或
panic，而不是可恢复的 `RegistrationClosed`。runtime plugin loading 不能重排既有
index；无需为这个不可能状态在 `FeatureMain` 中保存布尔值。

### 4.3 排序和 graph 构建

`feature_arc_init` 对每个 arc 执行以下完整合同：

1. arc 至少有一个 start node，至少有一个 feature；所有 `NodeId` 必须已存在；
2. feature 的 `runs_before`/`runs_after` 只解析同一 arc 内的 feature name；
3. `last_in_arc` 自动获得所有其它 feature 到它的 before edge；
4. 缺失 constraint target、重复 arc/feature/node、cycle 和不一致的 start next slot
   都是 typed startup error；
5. topological order 的零基位置就是 compact `u32 feature_index`；
6. 排序后的最后一个 feature node 是 default end node；
7. start nodes、feature nodes、name indexes 和 per-arc configuration owner 只在完整
   候选构造成功后发布；失败时既有 registration inventories 保持可诊断状态，不存在
   installed 标记需要回滚。

VPP 对部分缺失 constraint/name 只告警后继续，而 Hammer 把 startup plugin
declaration 当作可定位、可修复的配置错误并拒绝启动。这是有意的错误模型差异：
Hammer 不在缺了一段 ordering constraint 时运行一个不同的 packet graph。

不新增 `FeatureOrderRegistration` 或第三个 proc macro。E14 证明 vendored tree 没有
实际 `VNET_FEATURE_ARC_ORDER` declaration；现有 per-feature constraints 加
`last_in_arc` 已表达本仓库参照源的全部实际排序。将一个零调用者 C macro 复制成
Rust public API 不属于语义对齐。

### 4.4 Per-arc 配置与共享 heap

不保留第二个 `FeatureArc` 容器。arc registration、arc-name index、per-arc
feature-name index和排序结果直接由`FeatureMain`持有；全部per-arc配置字段进入与arc
index一一对应的`FeatureConfigMain`。最终 private config state明确分为两层：

```rust
struct FeatureConfigMain {
    config_main: ConfigMain,
    config_index_by_sw_if_index: Vec<u32>,
}

struct ConfigMain {
    start_nodes: Box<[NodeId]>,
    default_end_node: NodeId,
    feature_node_by_index: Box<[NodeId]>,
    config_pool: Pool<ConfigEntry>,
    config_by_words: HashMap<Box<[u32]>, u32>,
}
```

- `FeatureMain` 持有 registrations/name indexes、与 arc index
  一一对应的 `Vec<FeatureConfigMain>`、跨 arc 的
  `shared_feature_config_heap: Heap<u32>`、排序后的 per-arc feature nodes、per-arc
  `feature_count_by_sw_if_index` 以及 `sw_if_index_has_features`；
- 每个已构建 arc 在同一 compact arc index 上恰有一个 `FeatureConfigMain`。
  它拥有该arc全部配置状态：private `ConfigMain` 与dense
  `config_index_by_sw_if_index`，后者以 `u32::MAX` 表示该 interface 尚无 compiled
  config entry；
- private `ConfigMain` 是 VPP `vnet_config_main_t` 的 Rust 语义对应层，持有 start
  nodes、default end node、feature-index-to-node table、`Pool<ConfigEntry>`、config
  words hash，以及 config index 到 pool entry 的 lookup/back-pointer 合同；每个
  `ConfigEntry` 持有有序 `ConfigFeature`、per-config end node、heap offset 和 reference
  count；
- `ConfigMain` 不知道 `sw_if_index`、presence bitmap 或 feature count；
- feature count 来自本次 enable/disable 后的 occurrence 数量，不从去重后共享的
  `ConfigEntry.features` 反推；这对应 VPP `feature.c:262-285` 在
  `vnet_config_add_feature`/`vnet_config_del_feature` 之外独立更新 count；
  `FeatureConfigMain` 不拥有第二份 heap；`FeatureMain` 的窄 domain operation 同时借用
  目标 config main 与 shared heap 完成编译、替换和释放，不接受 caller closure，也不
  保存指向 heap storage 的裸指针。

这种拆分对应 E1/E5：VPP 的 `vnet_feature_config_main_t` 负责 interface 到 config 的
绑定，嵌入的 `vnet_config_main_t` 负责通用链编译，而 `vnet_feature_main_t` 负责跨 arc
inventory、presence/count 和共享 heap。`FeatureConfigMain` 与`ConfigMain`均不公开
borrow/getter；公开 API仍只在`FeatureMain`上。

因此“config字段进入FeatureConfigMain”包括start nodes、default/per-config end nodes、
feature-index-to-node、config pool/hash/back-pointer和dense interface config index；其中前
一组位于它内嵌的`ConfigMain`。只有VPP明确放在顶层、且跨arc共享或按arc并列的
`shared_feature_config_heap`、sorted feature nodes、presence bitmap和feature count保留在
`FeatureMain`，不把这些字段下沉到单个`FeatureConfigMain`。

packet-visible layout 保持：

```text
[config pool index]      control-only back-pointer
[start -> first next]    first packet-visible word
[feature-0 config words]
[feature-0 -> next]
...
[last -> end next]
[end node id]            hash/dedupe key only
```

如果最后一个 enabled occurrence 自身就是 end node，不建立 `end -> end` edge，也不
追加 self-next。add 保留重复 occurrence，del 只删除一个完全相同的
`(feature_index, config words)`。相同 words+end config 在对应 `ConfigMain` 内共享一个
pool value；heap extent 前置 word 保存该 config main 的 config-pool back-pointer，最后
一个引用释放时由 `FeatureMain` 调用 generic heap `remove(offset)` 回收；run长度、free
extent的索引、
复用与合并由 `Heap<u32>` 自己拥有。不同 arc 即使 words相同也不共享pool entry，因为
start/feature node table 与 pool index 的 owner 是各自的 `ConfigMain`。

现有 `Heap<T>` 还不足以执行这条生命周期，因此先在 `hammer-infra` 做一次通用扩展：

- 保留现有 `alloc`/`get` 与已分配offset在释放前稳定的合同；
- `alloc` 优先复用已释放 run，否则扩展 element space；`u32` offset/length overflow或
  backing allocation 无法满足属于进程内存基础设施不变量，直接 assert/panic，不返回
  `HeapError`，Feature 层也不增加 exhausted error；
- 增加按 allocation offset remove 并复用/coalesce free runs的操作；run长度只由Heap
  记录，调用者不重复传递；未知offset或重复remove是`Heap`自身的programmer invariant；
- 增加受边界检查的连续run immutable/mutable borrow，用于一次写入compiled words、
  dedupe key读取和packet cursor读取；borrow不跨FeatureMain operation或barrier边界。

这些是 `Heap<T>` 的通用 VPP-style element-space 能力，不包含 Feature、arc、node 或
config概念。Feature模块删除现有 `free_config_ranges` 和手写 extent allocator；不得在
`Heap<u32>` 外再维护一份free-list或allocation handle mirror。

Feature 配置仍是显式 `u32` words。VPP 用 byte count 后按 `u32` round-up；Hammer
要求 plugin 在 owner 边界显式编码/解码 words，从而不把任意 Rust layout、padding
或引用放进共享 heap。这个差异不改变 next/config/cursor 顺序。

### 4.5 控制面 API

以下现有 API 只迁移 receiver 和 module，不保留 `InterfaceMain` forwarding methods：

```rust
impl FeatureMain {
    pub fn register_feature_arc(/* existing arguments */) -> Result<u8, FeatureError>;
    pub fn register_feature(/* existing arguments */) -> Result<(), FeatureError>;
    pub fn feature_arc_index(&self, name: &str) -> Option<u8>;
    pub fn feature_index(&self, arc_index: u8, name: &str) -> Option<u32>;
    pub fn enable_feature(/* existing arguments */) -> Result<(), FeatureError>;
    pub fn disable_feature(/* existing arguments */) -> Result<(), FeatureError>;
    pub fn is_feature_enabled(/* existing arguments */) -> Result<bool, FeatureError>;
    pub fn modify_feature_arc_end(/* existing arguments */) -> Result<(), FeatureError>;
    pub fn reset_feature_arc_end(/* existing arguments */) -> Result<(), FeatureError>;
}
```

为覆盖 E9-E10 中当前缺少的 VPP 语义，新增以下窄 API：

```rust
impl FeatureMain {
    pub fn feature_count(
        &self,
        arc_index: u8,
        sw_if_index: u32,
    ) -> Result<u32, FeatureError>;

    pub fn has_features(&self, arc_index: u8, sw_if_index: u32) -> bool;

    pub fn feature_config_index(
        &self,
        arc_index: u8,
        sw_if_index: u32,
    ) -> Result<Option<u32>, FeatureError>;

    pub fn feature_arc_end_node(
        &self,
        arc_index: u8,
        sw_if_index: u32,
    ) -> Result<NodeId, FeatureError>;
}
```

`feature_count` 对从未扩展到该 interface 的 dense row 返回 `0`；
`feature_config_index` 对从未配置或已经删除的 interface 返回 `None`。一旦某个 interface
产生过 compiled config，删除最后一个 feature 后也与 VPP `vnet_config_del_feature`
一致，保留 count 为零、end 为 default 的 compiled config，因此仍返回该 config index；
`has_features` 此时为 `false`，普通 start 继续走调用者的 default next，而 cached-index
start 仍可使用这份配置。`feature_arc_end_node` 在没有 override 时返回 default end。
`has_features` 是 packet/cache fast check：调用者提供 startup-resolved arc index，越界是
programmer invariant；interface bitmap 越界则为 `false`。

name-based enable wrapper不新增。`feature_arc_index` + `feature_index` + numeric
enable/disable 已无损表达 VPP 的 name path，重复 API 只会增加错误翻译和测试面。

`enable_feature`、`disable_feature` 和 end-node mutation 继续在 owner 内校验 arc、
feature、`sw_if_index` 对应的 live `SwInterface`、end graph node和完整 candidate。
`FeatureMain` 只当次从必须已初始化的 `NetMain` 借用 `InterfaceMain` 验证 liveness，
不保存 interface reference。若这个 owner 不存在就断言 lifecycle invariant，不把
`RuntimeError` 包装成 `FeatureError`。外部 Binary API/CLI 若未来增加，仍需先做自身
schema/handle 校验并在真正的 ingress seam 翻译 `FeatureError`。

### 4.6 Packet-path API

普通 packet API 迁移到 `FeatureMain`，签名和 cursor 语义保持：

```rust
impl FeatureMain {
    #[inline(always)]
    pub fn start_feature_arc(
        &self,
        arc_index: u8,
        sw_if_index: u32,
        buffer: &mut Buffer,
        default_next: u16,
    ) -> u16;

    #[inline(always)]
    pub fn next_feature(&self, buffer: &mut Buffer) -> u16;

    #[inline(always)]
    pub fn next_feature_with_config<const N: usize>(
        &self,
        buffer: &mut Buffer,
    ) -> ([u32; N], u16);
}
```

补齐 VPP `vnet_feature_arc_start_w_cfg_index` 的能力：

```rust
impl FeatureMain {
    #[inline(always)]
    pub fn start_feature_arc_at_config(
        &self,
        arc_index: u8,
        config_index: u32,
        buffer: &mut Buffer,
    ) -> u16;
}
```

该方法只接受此前从同一 arc 的 `FeatureConfigMain::config_index_by_sw_if_index`
经 `feature_config_index` 获得、并在对应 owner transaction 中保持有效的 index。它从
`FeatureMain` 的 shared heap 读取 first next，将 cursor 推进一个 word，不做
bitmap/interface lookup。错误 arc、跨 arc 或 stale config index 是 cache owner 违反
publication/lifetime 合同的 programmer bug；hot path 断言，不返回 control-plane
`Result`。

不新增 `start_feature_arc_with_config`。E10 的 scoped search 证明 vendored tree 没有
该 helper 的实际 caller；现有语义是在 feature node 内通过
`next_feature_with_config::<N>` 读取该 occurrence 的 config。也不公开内部
`FeatureConfigMain`/`ConfigMain` borrow，避免让 packet code 持有可被
下一次 barrier mutation失效的引用。

packet path 只读取 `FeatureMain` 的 bitmap、dense index 和 shared heap。它不经过
`NetMain`/`InterfaceMain`，不查 name/map，不分配，不加锁，不修改引用计数。arc index
只在 start 时需要；后续只依赖 `Buffer::current_config_index`。

### 4.7 软件接口删除

`InterfaceMain` 不再直接访问 Feature private state。`interface_main_init` 消费的
built-in `InterfaceRegistrationImage` 安装 Feature 模块的 HIGH-priority
software-interface callback；hardware-interface create/delete 均通过现有 generic
software-interface callback dispatcher，行为与 E11 一致：

- create 在查询 `FeatureMain::global()` 之前就是 no-op，因此 `local0` 创建不依赖尚未
  执行的 `feature_main_init`；
- delete 取得已发布的 `FeatureMain`，清除每个 arc 的 bitmap bit、count 和对应
  `FeatureConfigMain::config_index_by_sw_if_index`；
- 每个旧 config entry 释放一个 reference，零引用 entry 回收；
- 清理完成后 `InterfaceMain` 才能释放/复用 `sw_if_index`。

接口删除已经要求 main thread 持有 Worker Barrier；`InterfaceMain` 在 software-interface
slot 仍有效时调用 delete callbacks，Feature callback 断言 barrier 前置条件，不自行进入
第二个 barrier。callback 成功后删除流程才释放 slot并允许 index复用。清理在已验证存在
的 interface 上是 infallible owner cleanup；破损 back-pointer/count 是 programmer
invariant，不转换成 `InterfaceError`。

这要求实现同时修正当前 `delete_hardware_interface*` 绕过通用 callback list 的路径。
FeatureMain 不反向拥有 interface pool，InterfaceMain 也不重新获得 Feature fields。

### 4.8 不复制 VPP 的裸 callback registries

本文有意不复制两类 VPP callback：

- `vnet_feature_registration_t.enable_disable_cb`；
- `vnet_feature_register` 的 process-global update observer list。

原因不是忽略 E13/E15，而是 Hammer 的 owner contract 更强：plugin-specific function
pointer、reference、state 和 policy 只能保存在 owning plugin。当前 Hammer 也没有
adjacency/device/tunnel feature-config cache 需要 observer invalidation。Feature owner 的
control method先更新自己的 policy，再调用 `FeatureMain`；未来某个 concrete owner 若
缓存 config index，应在自己的同一 barrier transaction 中更新 Feature Arc 和 cache，
并使用 `feature_config_index`/`start_feature_arc_at_config`，不得向 service 注册 erased
callback。

同理，VPP 的 feature CLI/show/API 是 FeatureMain 的调用者，不是其所有权的一部分。
Hammer 若增加相应 Binary API 或 diagnostics，必须由单独 ADR 定义 schema、owner 和
client boundary；本文不预留 message、handler 或 callback carrier。

### 4.9 Publication、failure atomicity 与 closure 清理

live mutation 的顺序保持并收紧为：

1. 在 barrier 外验证全部 identity、liveness、count、storage size、end node 和 candidate
   occurrence list；
2. 生成完整 edge plan 和 config words，尚不触及 worker-visible state；
3. 实际变更才进入 `worker_thread_barrier_sync!`；startup 或 no-op 不进入；
4. 在目标 `ConfigMain` 中分配不可达的 candidate pool slot，并从 FeatureMain shared
   heap分配 extent；
5. `NodeMain::add_node_next_slots` 完整成功后写 next slots；失败则 owner operation 回收
   candidate，旧 graph/config publication 不变；
6. 在同一个 owner mutation 中完成 `ConfigMain` dedupe/publication，并原子式替换
   `FeatureConfigMain` interface config index以及 FeatureMain count/bitmap；
7. 最后释放旧 config entry；实际新增 graph edge只请求一次 coalesced Graph Refork。

当前 `replace_feature_chain` 中的局部 `publish` closure 删除。`FeatureMain` 用一个私有、
窄命名的 domain operation 执行已准备好的 publication；它不接受 `Fn`/`FnMut`/
`FnOnce`，也不通过 closure 出借 state。

`FeatureError` 随 owner 移到 `hammer_service::feature`。只保留调用者确实能够修正的
startup declaration、control input和graph mutation错误。heap容量、offset范围和内部
索引耗尽都不是可恢复控制面错误，不定义 `HeapError`、`StorageExhausted` 或任何同义
variant。错误分类如下：

| 类别 | 例子 | owner/recovery |
| --- | --- | --- |
| startup declaration error | duplicate、missing target/node、cycle、empty arc | `FeatureMain`; startup caller报告并拒绝启动 |
| recoverable control error | invalid arc/feature/interface/end input | `FeatureMain`; caller可修正请求并重试，旧publication不变 |
| graph boundary error | missing node、next count overflow、start-slot mismatch | `FeatureMain` 在 runtime seam 一次翻译为具体 variant |
| programmer/infrastructure invariant | late registration、published cursor越界、heap back-pointer损坏、stale cached config、heap offset/length overflow、worker发起 mutation | owner assertion/panic；不得继续处理错误 graph |

不得增加 catch-all `Internal(String)`、`Invariant(String)` 或把 source 格式化成字符串。

### 4.10 Layout、alignment 与 inline

本迁移不改变任何 FFI、DSO ABI、Buffer header 或 SVM layout。`FeatureMain` 及 private
control structs 使用普通 Rust layout，不加 `repr(C)` 或 speculative cache alignment。
shared heap 从 Feature 自建的 `Vec<u32>` extent allocator 改为
`hammer_infra::heap::Heap<u32>`，但packet cursor仍是相同`u32` offset，config words保持
四字节对齐；这不改变Buffer header、packet-visible words或跨DSO layout。next slot在
发布前验证可表示为`u16`。

只对四个 tiny packet operations 保留/增加 `#[inline(always)]`：ordinary start、
cached-index start、next 和 const-word next。依据是 VPP 对应 helper 使用
`static_always_inline`（E10），且 Hammer 需要把 const `N`、bounds 和 cursor arithmetic
折叠进 node loop。实现验收必须用 release codegen 或现有 packet microbenchmark证明没有
引入 call、allocation、map lookup 或额外 global lookup；控制 API 不加 inline attribute。

## 5. 三方语义差异

| 维度 | 当前 Hammer | 本 ADR 后 Hammer | VPP 证据 | 决策与影响 |
| --- | --- | --- | --- | --- |
| owner | `InterfaceState.feature`，其中`FeatureArc`混合三层职责 | 独立process-global `FeatureMain`；不保留`FeatureArc`聚合，全部per-arc config进入`FeatureConfigMain -> ConfigMain` | E1, E5 | 对齐；InterfaceMain不再是feature owner，registration inventory与config owner分离 |
| module/API receiver | `interface::feature`, `impl InterfaceMain` | `service::feature`, `impl FeatureMain` | E1-E3 | breaking move，无 re-export/forwarder |
| lifecycle | interface init后由 `interface_feature_init` 构建；interface callback image未消费 | `interface_main_init` 先注册built-in callbacks，`feature_main_init` 发布owner，`feature_arc_init` 一次构建 | E2-E4, H5 | 对齐声明收集后统一init，并接通既有interface lifecycle；不增加install状态/API |
| arc index | registration 时分配 | 保持 registration 时稳定分配 | E3 | 有意 timing 差异；避免保存 plugin pointer，最终 compact index相同 |
| feature ordering | before/after + last | 保留，完整 typed validation | E4 | 对齐结果；VPP warning分支改为 startup error |
| bulk order | 无 | 无 | E14 | 有意不复制零调用者 API；实际 vendored declarations全部可表达 |
| per-arc config | private `FeatureArc` 同时持有dense interface index、graph facts、pool/hash和config记录 | 每个arc一个private `FeatureConfigMain`，其内嵌private `ConfigMain`；pool entry使用与VPP直接对应的`ConfigEntry`/`ConfigFeature` | E1, E5-E7 | 对齐VPP config ownership；不增加`FeatureChain`概念或公开config wrapper |
| add/del | duplicate + exact delete | 保持 | E7-E8 | 对齐 |
| end node | modify/reset，无 getter | add getter，保留 modify/reset | E9 | 补齐 |
| packet start/next | ordinary start + next | 增加 presence/count/config-index/cached start | E10 | 补齐实际 cache/start语义 |
| interface add/delete | create/delete绕过software callback，delete直改private state | `interface_main_init`消费built-in image；create先dispatch no-op；delete在slot释放前用generic HIGH callback进入FeatureMain cleanup | E11, H5 | 对齐owner和生命周期，复用既有callback surface |
| interface validation | owner内验证 live interface | FeatureMain 当次借用 InterfaceMain验证；缺失NetMain断言lifecycle invariant | E12, H6 | Hammer安全增强；不缓存reference，不新增跨owner错误转换 |
| feature policy callback | 无 | 仍无 | E15, H6 | 有意差异；plugin owner显式组合 |
| update observers | 无 | 仍无 | E13, H6 | 有意差异；禁止 erased/plugin callback registry |
| synchronization | InterfaceMain owner内部 barrier | FeatureMain owner内部 barrier | H3, H6 | owner迁移；无新 primitive |
| config heap owner | Feature私有`Vec<u32> + free_config_ranges` | FeatureMain直接持有扩展后的`Heap<u32>`；free-list/reuse归infra | E5-E7, H7 | 使用仓库既有VPP-style primitive；删除Feature本地allocator |
| layout/ABI | `u32` word heap offset + Buffer cursor | 存储owner改变，packet-visible offset/words不变 | E5-E10, H7 | 无ABI或packet layout migration |

## 6. 稳定决策记录

| ID | 最终决策 | 对齐结论 | 理由 |
| --- | --- | --- | --- |
| D1 | Feature Arc 的唯一 owner 是独立 `FeatureMain` | Aligned | E1, H1 |
| D2 | FeatureMain 由 owner-local `OnceLock` 发布，不嵌入 NetMain/InterfaceMain | Aligned Rust expression | E1, H6 |
| D3 | 注册仍由 concrete plugin 的startup callback发起，`feature_arc_init`统一在workers前构建；late registration是lifecycle invariant | Aligned | E2-E4 |
| D4 | per-feature constraints + `last_in_arc` 是唯一 ordering surface | Intentional divergence | E4, E14；不复制零调用者 bulk macro |
| D5 | 每个已构建arc有一个private `FeatureConfigMain`，它拥有dense interface index并内嵌private `ConfigMain`承载全部per-arc配置；不保留`FeatureArc`聚合；FeatureMain只持有VPP顶层inventory/shared heap/per-arc nodes/count/bitmap | Aligned | E1, E5-E8, H1-H4 |
| D6 | 增加 count、presence、config-index、end getter 和 cached-index start | Aligned | E9-E10 |
| D7 | `interface_main_init`消费built-in callback image；create callback先no-op，delete通过generic HIGH callback在slot释放前触发FeatureMain cleanup | Aligned | E11, H5 |
| D8 | live mutation由 FeatureMain内部 barrier发布并保持 failure atomic | Aligned Hammer publication | E8, H3, H6 |
| D9 | 不保存 Feature policy/update callbacks | Intentional divergence | E13, E15, H6 |
| D10 | config仍是显式 `u32` words，Buffer layout不变 | Intentional Rust representation | E5-E10, H4 |
| D11 | 缺失 ordering/node declaration 是 typed startup error，不是 warning | Intentional safety divergence | E3-E4, H6 |
| D12 | 不新增 Feature Binary API/CLI/diagnostic owner | Aligned boundary | E12；这些只是 callers |
| D13 | FeatureMain直接持有`Heap<u32>`；remove/reuse/run borrow作为generic infra能力补入Heap，Feature不保留free-list或run长度mirror | Aligned Rust infrastructure | E5-E7, H7；仓库要求复用hammer-infra primitive |

## 7. 变更清单

### 7.1 新增

| 类型/API | 目标位置 | 最终行为 | 为什么现有 surface 不足 | 验证 |
| --- | --- | --- | --- | --- |
| `Heap::remove(offset)` | `crates/hammer-infra/src/heap.rs` | Heap按内部记录的run长度drop元素并coalesce；无效offset是owner invariant | 现有Heap无remove path，config entry有引用归零生命周期 | remove/reuse/coalesce tests |
| `Heap::{get_slice,get_slice_mut}` | 同上 | 对一个连续run做bounds-checked直接borrow | 单元素`get`不足以安装/哈希compiled config words | bounds/read/write tests |
| `FeatureMain` | `crates/hammer-service/src/feature.rs` | 唯一 process-global Feature owner，直接持有全部领域字段 | `InterfaceMain` 是错误 owner，`FeatureState`也没有独立领域角色 | owner/lifecycle integration |
| private `FeatureConfigMain` | 同上 | 每个已构建arc一个；拥有全部per-arc config state，组合`ConfigMain`与dense interface config-index table | 当前`FeatureArc`折叠了E1/E5的独立per-arc绑定层 | per-arc cardinality/dense-index unit test |
| private `ConfigMain`、`ConfigEntry`、`ConfigFeature` | 同上 | 持有通用start/feature/end graph facts、config pool/hash、per-config end和pool back-pointer关系 | 当前`FeatureArc`折叠了VPP generic config owner；`FeatureChain`命名没有VPP对应物 | config compile/share/release unit tests |
| `FeatureMain::global` | 同上 | 返回已初始化的进程 owner | Feature API 不应再经 `NetMain -> InterfaceMain` | init order/global access |
| `feature_main_init` | 同上 | init phase 发布空 owner | 当前 owner 随 InterfaceMain 隐式创建 | duplicate/init-order test |
| `feature_arc_init` | 同上 | main-loop-enter中直接构建全部arcs，且是唯一构建入口 | 当前入口名称和 owner均绑定 interface | startup lifecycle test |
| `feature_count` | 同上 | 读取 per-arc/interface occurrence count | 当前只能内部读 count | E10 query test |
| `has_features` | 同上 | bitmap fast check | 当前只隐藏在 ordinary start | no-feature/cache fast-path test |
| `feature_config_index` | 同上 | 返回 compiled config index或 `None`；最后一个feature删除后保留VPP empty/default config | cached start没有受控 index来源 | empty/end-only/config query test |
| `feature_arc_end_node` | 同上 | 返回 override或 default end | 当前只有 modify/reset | default/override/reset test |
| `start_feature_arc_at_config` | 同上 | 从已发布 config index启动并推进 cursor | 当前无法表达 E10 的 cached-index path | cached start graph test |
| Feature software-interface callback | Feature module + service built-in interface image | create立即no-op；HIGH priority删除清理 | 当前 InterfaceMain 直接访问 Feature private state，且既有image未消费 | init/create/delete/reuse test |

### 7.2 修改/迁移

| 类型/API/调用者 | 当前 -> 目标 | 修改内容 | 兼容性 | 验证 |
| --- | --- | --- | --- | --- |
| 现有 Feature private records | `interface/feature.rs` -> `feature.rs` | registrations/indexes直接迁入FeatureMain；`FeatureArc`的全部配置字段进入两个private config owner | private move/split | config behavior suite |
| `Heap<T>` | `hammer-infra/src/heap.rs` | 从grow-only allocation side扩展为支持remove/reuse的稳定offset element space；现有不remove的caller行为不变 | Feature需要回收config run，仓库禁止本地one-off allocator | infra heap suite + feature config suite |
| `FeatureError`其余variants | `interface::feature` -> `feature` | owner-local path迁移，其余variant contract保持 | source-breaking path | concrete variant tests |
| 现有 13 个 Feature methods | `impl InterfaceMain` -> `impl FeatureMain` | 参数语义不变，interface validation改为当次 borrow | source-breaking receiver | workspace compile + behavior |
| `#[feature_arc]`, `#[feature]` expansion | component macros | owner参数和 error path改为 FeatureMain | 所有生成调用者同批迁移 | macro compile tests |
| `hammer-service` runtime registration image | `src/lib.rs` | 增加feature init并把main-loop-enter symbol改为Feature owner入口 | startup order change | real lifecycle test |
| built-in `InterfaceRegistrationImage` / `interface_main_init` | `interface_model.rs` + Feature module | image包含Feature HIGH callback并在InterfaceMain发布前消费、排序 | 当前image定义/consumer存在但未接线 | callback registration/order test |
| `InterfaceMain` create/delete path | `interface_model.rs` | software slot完整创建后dispatch create；delete在slot释放前dispatch generic callbacks | callback lifecycle behavior change | local0 init + delete/reuse test |
| IP plugin Feature callsites | `input.rs`, `local.rs`, `punt.rs` and owner init | 从 `net.interface_main()`改为 `FeatureMain` direct borrow | source-breaking | IP graph tests |
| service feature tests | `tests/interface_features.rs` -> feature-owned tests | fixture和 assertions改用 FeatureMain并扩充矩阵 | test ownership move | focused test target |
| ADR-0002/ADR-0006/`CONTEXT.md` | docs | implementation获批时记录新 owner和 supersession | 文档同步 | review audit |

### 7.3 删除

| 删除项 | 位置 | 替代 | 验证 |
| --- | --- | --- | --- |
| `InterfaceState.feature` | `interface_model.rs` | `FeatureMain`直接拥有的字段 | compile/API audit |
| `FeatureState` | current feature module | `FeatureMain`直接字段与owner methods | compile/type ownership audit |
| installed `FeatureArc` | current feature module | registration/name indexes归FeatureMain；全部per-arc config归`FeatureConfigMain`/`ConfigMain` | compile/type ownership audit |
| `free_config_ranges`、`free_extent`和Feature私有extent allocator | current feature module | generic `Heap<u32>` allocation/remove | heap reuse + config retirement tests |
| `hammer_service::interface::feature` public path | module export | `hammer_service::feature` | all-target compile；无兼容 re-export |
| `InterfaceMain::{register_feature_arc,...,next_feature_with_config}` 全部 Feature methods | `interface/feature.rs` | 同名 `FeatureMain` methods | caller closure audit |
| `interface_feature_init` | current feature module | `feature_arc_init` | lifecycle ordering test |
| `install_feature_arcs`、installed/closed状态及对应错误 | current feature module | `feature_arc_init`直接构建；late registration断言lifecycle invariant | API/variant cleanup audit |
| interface delete中的 `state.feature.remove_interface` | `interface_model.rs` | generic callback -> FeatureMain cleanup | interface deletion test |
| `replace_feature_chain` 的局部 `publish` closure | current feature module | private owner domain operation | production closure audit + failure tests |

没有 compatibility alias、deprecated forwarding method、双 publication path、配置迁移
flag、序列化格式或跨进程 ABI 变更。

## 8. 实现顺序

实现必须按一个 coherent slice 完成，不能留下双 owner：

1. 先在`hammer-infra::heap::Heap<T>`实现获批的非fallible allocation reuse、run borrow、
   remove/coalescing，并保持现有grow-only caller行为；
2. 引入并初始化 `FeatureMain`，把现有字段直接移入owner并删除`FeatureState`，不增加
   第二份状态；
3. 删除`FeatureArc`聚合，把registration/name indexes放到FeatureMain，把全部
   per-arc config字段放到每arc一个`FeatureConfigMain`及其内嵌`ConfigMain`；不保留
   兼容的第二套配置owner；
4. 把所有 existing methods 和 macro expansion改为 FeatureMain receiver；
5. 一次迁移 service/IP/tests 的全部 callers，删除 InterfaceMain forwarding surface；
6. 让 `interface_main_init` 消费built-in image，接通software-interface create/delete
   callbacks并删除 direct private access；
7. 增加 E9-E10 的 query/cached-start methods；
8. 删除 closure、旧 module/path/init symbol，更新 ADR/CONTEXT；
9. 完成全文本 caller/definition cleanup audit后，才进入 repository规定的最终 pre-commit
   test gate。

定义、exports、macros、registrations、callers、tests 和 docs 必须同批闭合。编译错误
不是增加 wrapper/alias/registry 的授权。

## 9. 测试矩阵

| Test | 层级与 setup | 必须断言 | 决策 | VPP 证据 |
| --- | --- | --- | --- | --- |
| `heap_reuses_released_runs` | infra unit，不涉及Feature | run读写边界；remove后优先复用；相邻free run合并；未释放offset保持稳定；invalid/double remove与offset overflow触发invariant | D13 | E5-E7, H7 |
| `feature_main_owns_arc_lifecycle` | service integration，真实 init/main-loop-enter | FeatureMain在NetMain/InterfaceMain之外独立发布；built-in callbacks在local0前注册且create不要求FeatureMain；registration callbacks先于唯一的`feature_arc_init`；late registration触发lifecycle invariant | D1-D3, D7 | E1-E4, E11 |
| `feature_arc_orders_and_indexes_features` | service unit/integration，多 start/features | compact arc/feature indices、before/after/last顺序、start slots一致 | D3-D4 | E3-E6 |
| `feature_arc_rejects_invalid_declarations` | service startup fixture | duplicate、missing target/node、cycle、empty arc逐一匹配具体 variant；失败不发布部分config owner或graph edges | D4, D11 | E3-E4 |
| `feature_config_main_is_dense_per_arc` | service private unit，安装两个arcs并使用稀疏interface indices | compact arc index上恰有一个FeatureConfigMain；每个内嵌独立ConfigMain；dense table扩展到最高interface且中间/未配置项为`u32::MAX`；config fields不留在registration inventory | D5 | E1, E5 |
| `feature_config_add_delete_and_share` | service integration | duplicate occurrence、exact single delete、absent delete no-op、ConfigMain内dedupe/refcount、Heap extent复用、end进入key；不同arc不共享pool entry | D5, D13 | E5-E8 |
| `feature_queries_match_published_chain` | service integration | count/presence/index/end在default、enabled、end-only override、reset后相互一致 | D5-D6 | E8-E10 |
| `feature_packet_cursor_traverses_chain` | graph behavior | ordinary start按interface选择；config words顺序；每node推进一次；最终到end；无feature不改cursor | D5-D6, D10 | E6, E10 |
| `feature_cached_config_starts_same_chain` | graph behavior，先取config index | cached start与ordinary start选择同一first next/cursor；跨arc/stale index触发invariant test | D6 | E10 |
| `feature_end_node_avoids_self_edge` | graph behavior | enabled end feature不建self edge；modify/get/reset一致 | D5-D6 | E6, E9 |
| `feature_interface_delete_releases_all_arcs` | interface integration，删除并复用pool slot | callbacks在slot释放前运行；所有arc bitmap/count/index清空；chain最后引用释放；复用不继承 | D7 | E11 |
| `feature_mutation_is_failure_atomic` | runtime/service integration，注入graph失败 | old index/count/bitmap/heap/refcount/graph不变；成功新增edges只触发一次refork | D8, D13 | E6-E8, H3, H7 |
| `feature_live_mutation_uses_one_barrier` | runtime workers | startup/no-op/read不入barrier；实际变更一次；nested transaction复用recursion | D8 | H6 |
| `feature_macros_register_with_feature_main` | proc-macro compile + real plugin init | generated methods只接受FeatureMain；真实nodes/name constraints安装 | D1-D4 | E2-E4 |
| `feature_packet_path_cost` | release benchmark/codegen | 无allocation、lock、map/name lookup、refcount、InterfaceMain/NetMain访问；tiny methods内联 | D6, D10 | E10 |
| `feature_layout_is_unchanged` | compile-time layout assertions | Buffer header/current config offset不变；Heap words四字节且offset为`u32`；无Feature state进入NetworkOpaque | D10, D13 | E5-E10, H7 |

不得用读取 `.rs` 后 `contains`/regex 的测试证明 owner 或 API 删除；删除闭包和旧 symbols
用 review-time `rg` cleanup audit，行为/类型通过编译和真实 lifecycle tests 证明。

最终 implementation gate（不是本 ADR authoring turn）至少包括：

```text
cargo fmt --all -- --check
git diff --check
cargo test -p hammer-infra --test heap
cargo test -p hammer-service --test feature_arcs
cargo test -p hammer-component-macros
cargo test -p hammer-plugin-ip
cargo clippy -p hammer-infra -p hammer-service -p hammer-component-macros -p hammer-plugin-ip --all-targets
```

具体 package/test target 以实现时 workspace 的真实名称为准；TUN/TCP lab 仍只在 CI。

## 10. 新类型/API 审批

根 `AGENTS.md` 要求非平凡 VPP 工作中的每个新 type/API 在实现前获得明确批准。
用户在本 ADR 评审后明确批准以下实现，并进一步否决 install state、fallible heap
allocation 和 exhausted errors：

| 待批准项 | 最终结果 | 现有 surface 为什么不能满足 | 不采用的替代 |
| --- | --- | --- | --- |
| `Heap::remove(offset)` | Heap内部拥有run长度、drop和coalesce生命周期 | 现有Heap没有remove；config entry最后引用必须回收 | C式`dealloc(offset,len)`、FeatureMain的`free_config_ranges`、永不释放config |
| `Heap::{get_slice,get_slice_mut}` | bounds-checked连续run borrow | 现有单元素`get`不能直接安装/读取compiled words | 暴露内部Vec、逐word public mutation helper |
| `FeatureMain` + `global` + private process cell | 建立直接持有领域字段的唯一正确owner，无FeatureState | `InterfaceMain` ownership与E1冲突；继续转发会保留双边界 | 嵌入NetMain、FeatureState/Inner、registry capability、Arc handle |
| private `FeatureConfigMain` | 每个已构建arc唯一拥有dense interface config index并内嵌ConfigMain | 当前FeatureArc混合registration与config，不能表达E1/E5 | 继续使用FeatureArc、公开config wrapper |
| private `ConfigMain`、`ConfigEntry`、`ConfigFeature` | 在FeatureConfigMain内拥有通用graph/config pool/hash/end/back-pointer合同，并直接对应VPP三个config类型 | 直接把generic config字段平铺进FeatureConfigMain会丢失VPP config owner边界 | 公开ConfigMain、保留FeatureChain命名、Feature-specific heap/pool helper |
| `feature_main_init` / `feature_arc_init` | 分离owner发布和startup arc构建；后者是唯一构建入口 | 当前 init symbol绑定错误 owner | lazy init、额外install API、worker后构建 |
| `feature_count` | VPP count query | count当前完全private | 暴露FeatureConfigMain borrow |
| `has_features` | bitmap fast query | ordinary start不能供cache owner预判 | 暴露bitmap/reference |
| `feature_config_index` | 受控获取compiled index | 当前无cached-start index来源 | 返回private config entry handle/wrapper |
| `feature_arc_end_node` | 完整end-node read/modify/reset | 当前只有写/reset | 暴露private config entry |
| `start_feature_arc_at_config` | VPP cached-index start语义 | ordinary start强制bitmap+dense lookup | feature-specific packet helper |

现有 methods 的 receiver/module迁移不是新增能力，但属于 source-breaking API change，
也必须与上表作为一个整体批准。没有其它待批准新 type、trait、error family、wrapper、
registry、sync primitive 或 dependency。

## 11. 开放项、结论与审查 verdict

设计层没有待源码核实的开放问题：bulk constraints、enable callback、update observer、
cached start、interface deletion和VPP测试覆盖都已做 scoped search并在本文明确处置。

当前结论分三类：

- **仓库事实**：Feature state和API当前属于InterfaceMain；配置链、shared heap、Buffer
  cursor已实现；已有generic Heap但缺少释放/复用；interface image未消费且delete path
  直接访问private state；测试面不完整（H1-H5, H7）。
- **用户要求**：Feature相关状态/操作迁到FeatureMain；显式增加FeatureConfigMain并把
  per-arc config字段放进去；FeatureMain不保留FeatureState；复用已有Heap；Feature Arc
  完整对照VPP；先写ADR且不改代码。
- **设计推断**：Rust不复制C pointer/callback surface；bulk-order macro无实际语义需求；
  cached config能力通过窄numeric API表达。每项推断都由E/H证据和仓库contract约束，
  不留作实现者自由选择。

**Design verdict: Aligned and approved.** 本 ADR 给出的目标 ownership、configuration、
packet cursor、interface lifecycle和publication语义与vendored VPP对齐，所有有意差异
均已记录理由和验证。实现必须严格保持无installed状态、无install API、无fallible heap
allocation和无exhausted错误的最终约束。

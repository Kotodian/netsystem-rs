# ADR-0047: TUN vhost-net 多队列与 host netlink 配置

Status: implemented (source review only; build and runtime validation intentionally not run)

Date: 2026-10-01

## 1. 决策范围

保留当前 `hammer-plugin-tuntap` 的 DSO 名和 `[plugin.tuntap]` TOML 入口，避免把设备改造混成配置迁移；设备能力则收敛为 **L3 TUN**。不实现 TAP/L2、MAC/bridge、host gateway/default route、vhost-user、GRO 或 CLI。GSO/CSUM 作为可选 TUN offload，默认关闭；禁用时带相应 offload flags 的包按 node error 丢弃。保留 **vhost-net**，因为 VPP tap 插件的 TUN 分支正是通过它把 virtqueue 的 RX 包送入 `ip4-input`/`ip6-input`；去掉 vhost 后不能复用 `tap_input_node`/`tap_device_class` 的收发算法。

此决策替代 ADR-0027/0030 对 tuntap 数据路径的单 fd、逐包 `readv/writev`、`tuntap-rx`/`tuntap-tx` 实现选择；ADR-0045 的单 worker 测试配置是历史记录，不是新多队列验收。现有 `hammer-service::interface` 的 interface、RX/TX queue 和动态 output/tx pair 仍是唯一通用 owner，IP plugin 仍拥有 IP 地址与 input node。TUN 插件只持有 TUN/vhost 描述符、vring 和队列私有状态。

VPP 源码依据：

| 源码 | 必须保留的语义 | 本 ADR 的裁剪 |
| --- | --- | --- |
| `third_party/vpp/src/plugins/tap/tap.c:430-651` | `IFF_TUN | IFF_NO_PI | IFF_VNET_HDR`、`IFF_MULTI_QUEUE` 能力检查、每 RX queue 一个 TUN fd、非阻塞 fd | 仅 TUN；不生成 TAP 分支；默认不设置持久化 |
| `tap.c:663-715,821-936` | 每 queue pair 的 vhost fd、features/memory table、RX=vring 0、TX=vring 1、call/kick/backend | 保留 vhost-net，删除 vhost-user/TAP-only 选择 |
| `tap.c:749-810`；`third_party/vpp/src/vnet/devices/netlink.c:28-113,199-353` | host 地址、link up、MTU 经 NETLINK_ROUTE 请求并等 ACK | 用 Rust netlink crate；不调用 `netlink_add_ip[46]_route` |
| `tap.c:154-216,1002-1045`；`third_party/vpp/src/vnet/interface/rx_queue.c:54-81,146-156` | RX queue 注册、File call fd 到 queue interrupt、TX queue 到执行线程、TUN P2P interface | thread 0 不跑 dataplane，故自动 TX 数按 Data Worker 数而不是 VPP 全部 `vlib_main` 数 |
| `third_party/vpp/src/plugins/tap/rx_node.c:30-87,191-423` | used ring 批量提取、链式 Buffer、IPv4/IPv6 next、frame 末 refill、RX error counter | 只保留 TUN/IP 分支；不保留 Ethernet 分支 |
| `third_party/vpp/src/plugins/tap/tx_node.c:61-88,220-424` | TX used ring 回收、Buffer chain 间接描述符、一次发布一批、no-slot drop | 只保留 TUN 分支；禁用 GSO/CSUM offload 时沿用其 drop 语义 |
| `third_party/vpp/src/plugins/tap/internal.h:41-129,209-255` | queue cacheline 分隔、vring、每 queue counters、interface 到 queue 映射 | 不复制未启用的 TAP/GRO 字段 |

## 2. TOML 与初始化

保持现有 `enabled/name/mtu/admin_up/ip4_address`。`ip4_address` 是 **Hammer 侧** interface 地址，仍调用 IP plugin；`host_ip4_address`/`host_ip6_address` 是 **Linux TUN 侧** 地址，只由 netlink 配置，不能把同一 CIDR 配在两侧。`num_rx_queues` 省略为 1；`num_tx_queues` 省略为 Data Worker 数，实际 TX 数取 `max(configured, data_worker_count)`，不把控制线程算进去。有效 RX 或 TX queue 数超过 1 时要求 Linux `TUNGETFEATURES` 有 `IFF_MULTI_QUEUE`；两个都为 1 才是单队列。ring size 默认为 VPP 的 256，须是非零、2 的幂且不大于 32768。TUN MTU 范围按 VPP `tap.c:27-30` 的 TUN packet bounds 验证，且必须检查 Buffer headroom 能容纳 virtio header 与 `TUN_DATA_OFFSET`。TX 自动扩容使每个执行 worker 至少独占一个 TX vring，避免热路径共享队列锁；VPP 的显式单 pair 多线程共享特例不纳入本次性能方案。

```toml
plugins = ["tuntap", "iperf3"]

[cpu]
main-core = 0
corelist-workers = [10, 11]

[plugin.tuntap]
enabled = true
name = "hammer0"
mtu = 1500
admin_up = true
ip4_address = "198.18.0.1/30"
host_ip4_address = "198.18.0.2/30"
num_rx_queues = 2
num_tx_queues = 2
rx_ring_size = 256
tx_ring_size = 256
```

不增加 `host_ip4_gw`、`host_ip6_gw`、route、bridge、MAC、TAP mode 或 vhost mode 配置。Linux 给 host CIDR 自动创建的 connected route 是地址配置的内核副作用，不是插件安装的 gateway/default route。`admin_up` 是 Hammer interface 管理状态；host link 必须在 netlink ACK 后单独置 up，二者不混用。`mtu` 同时设置 host link MTU 和 Hammer interface MTU，失败不得发布半成品 interface。内核不支持 vhost-net、必需 virtio feature 或请求的 multi-queue 时启动失败，不悄悄退回旧 `readv/writev`。

```rust
// plugin/tuntap；VPP tap.h:32-77、tap.c:478-482,749-810。
#[derive(serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TunConfig {
    enabled: bool,
    name: String,
    mtu: u32,
    admin_up: bool,
    ip4_address: Option<ipnet::Ipv4Net>,       // Hammer 侧
    host_ip4_address: Option<ipnet::Ipv4Net>,  // Linux 侧
    host_ip6_address: Option<ipnet::Ipv6Net>,  // Linux 侧
    num_rx_queues: u16,
    num_tx_queues: Option<u16>,
    rx_ring_size: u16,
    tx_ring_size: u16,
    gso: bool,
    csum_offload: bool,
}

// VPP tap.c:1408-1445：先建立进程级 Main 与 vhost memory table。
impl TunMain {
    fn init() -> RuntimeResult<&'static Self>;
    fn global() -> &'static Self;

    // VPP tap.c:430-482,544-651,821-1045：配置阶段才创建设备。
    fn create_if(&self, config: TunConfig, runtime: &mut DataPlaneMain)
        -> RuntimeResult<()>;
}
```

**`init` 和设备创建分阶段。** VPP 的 `tap_init` (`tap.c:1408-1445`) 初始化进程级 `tap_main` 与覆盖 Buffer physmem 的 vhost memory table，不创建 TUN；`tap_create_if` (`tap.c:430-1045`) 才打开 fd、建 vring、配置 host 并注册 interface。Hammer 的 `#[init_function(name = "tun_init", runs_after = ["interface_main_init"])]` 调用 `TunMain::init()`，只发布进程级 Main 和 memory table；它不读取 TOML、不打开 `/dev/net/tun`。`#[config_function(section = "plugin.tuntap")]` 在配置启用后调用 `TunMain::global().create_if(config, runtime)`。插件加载但 `enabled = false` 时，Main 已初始化但没有设备实例；创建失败不发布半成品实例。

**DeviceClass 注册走现有 image。** `#[derive(DeviceClass)]` 与 `#[derive(HwClass)]` 把声明放入插件的 `declare_interface_registration_image!()`；`interface_main_init` 遍历插件的 `HAMMER_INTERFACE_REGISTRATION_IMAGE` 并调用 `InterfaceMain::consume_registration_image` (`interface_model.rs:1559-1591`)，先安装 class/name index。`tun_init` 的 `runs_after` 明确依赖该阶段。`create_if` 使用 `device_class_index("tuntap")`、`hw_class_index("tun")` 调用 `register_interface`；`DeviceClass.tx_function = tun_intfc_tx` 因而由 service 创建动态 `<interface>-tx` node，不手工注册第二个 TX node。`TunInputNode` 由 graph registration 在 graph materialization 注册为 Interrupt；配置阶段固化 `NodeId` 和 IP4/IP6/drop next slot，File callback 只用已安装的 index。旧 `tuntap_input_init` main-loop-enter hook 随迁移删除。来源：VPP `tap.c:52-56,1002-1045,1399-1445`、`rx_node.c:412-423`；Hammer `interface_model.rs:1559-1591`、`main_loop.rs:152-170`。

```rust
// plugin/tuntap；VPP tap.c:52-56,1399-1409、rx_node.c:412-423。
hammer_service::declare_interface_registration_image!();

#[derive(hammer_component_macros::DeviceClass)]
#[device_class(
    name = "tuntap",
    format_device_name = format_tun_interface_name,
    tx_function = tun_intfc_tx
)]
struct TunDeviceClass;

#[derive(hammer_component_macros::HwClass)]
#[hw_class(name = "tun", flags = HwClassFlags::P2P)]
struct TunHwClass;

// VPP tap_init；Hammer normal init 在 graph materialization 之前。
#[hammer_component_macros::init_function(
    name = "tun_init", runs_after = ["interface_main_init"]
)]
fn tun_init(runtime: &mut DataPlaneMain) -> RuntimeResult<()>;

// Hammer 当前 config ABI；VPP tap_create_if 的设备创建时机。
#[hammer_component_macros::config_function(
    name = "tuntap_config", section = "plugin.tuntap"
)]
fn tuntap_config(config: TunConfig, runtime: &mut DataPlaneMain)
    -> RuntimeResult<()>;

hammer_component_macros::declare_plugin!(
    name = "tuntap",
    load_after = ["ip"],
    init_functions = [__INIT_FN_TUN_INIT],
    config_functions = [__CONFIG_FN_TUNTAP_CONFIG],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [__INIT_FN_TUNTAP_EXIT],
    worker_init_functions = [],
    graph_nodes = [__TUNTAP_GRAPH_NODE_TUN_INPUT_NODE],
    node_functions = [],
    process_nodes = [],
);
```

具体 interface 创建不新增 helper 或 public service API：`TunMain::create_if` 直接调用已有 `InterfaceMain::register_interface`，用 `device_class_index("tuntap")` 与 `hw_class_index("tun")`，再调用已有 MTU/flag/queue 注册。动态 `<interface>-output`/`<interface>-tx` pair 由 service 的 `register_interface` 生成，`tx_function = tun_intfc_tx` 连接设备函数；`tun-input` 是独立 RX sibling node，不把 TX 函数误注册成另一个 static TX node。`register_interface` 返回 hw index 后，从现有 `hardware_interface(hw_if_index).tx_node_index` 取得动态 TX node，调用现有 `runtime.register_node_errors(tx_node, &TUN_TX_ERROR_DESCRIPTORS)`；不为设备类错误发明第二个宏属性。exit hook 保留现有生命周期入口，但改为多队列的停止、used ring 回收和 descriptor 关闭；旧单 fd 退出算法废弃。

初始化顺序固定：`tun_init` 建立覆盖所有 Buffer physmem 映射的 vhost memory table 并发布空的 `TunMain`；`tuntap_config` 验证 TOML 与 worker/Buffer 映射 -> 创建 TUN fd 并查询 feature -> 用相同 ifname 和 `IFF_MULTI_QUEUE` 附着其余 fd -> 建立每 queue pair 的 vhost fd、eventfd 和 ring -> netlink 配置 host 地址、MTU、link up 且逐项等 ACK -> `VHOST_SET_FEATURES/MEM_TABLE/VRING_*/BACKEND` -> 注册 Hammer interface、service RX/TX queue 和 File call fd -> 预填 RX ring -> 添加 Hammer 侧 IP 地址 -> 发布设备实例 -> 允许 worker 调度。VPP 同样先配置 host link 再安装 vring (`tap.c:749-936`)。必需 features 是 VPP `tap.c:39-41` 的 `MRG_RXBUF`、`VERSION_1`、`INDIRECT_DESC`；只启用能在 Hammer Buffer 上正确处理的集合。VPP `tap.c:442-482,615-651,663-715,749-810,821-1045,1408-1445` 是顺序来源。默认不设 `TUNSETPERSIST`，失败关闭最后一个 fd 即撤销新建的 host link；若未来支持 attach-existing/persist，必须另设计外部状态的恢复规则。若 interface/queue/File 已注册后发生失败，按 owner 逆序撤销 File 与 interface（后者拥有 queue）及已添加的 IP 地址；已预填 RX ring 的 Buffer 在尚存的 `DataPlaneMain` 上归还，`OwnedFd`/ring owner 只自动释放各自的内核资源，不发布部分构造的设备实例。

## 3. owner 与多队列状态

一个 RX queue 归一个 Data Worker；`InterfaceMain::register_rx_queue(hw, queue_id, worker, mode)` 建 queue，File 注册后用 `set_rx_queue_file_index` 关联。每个 call fd 的 `File::polling_thread_index` 选同一个 worker，`File::private_data` 保存 service RX queue index。read callback 只取走 eventfd 计数、在 service 的该 worker RX pending bitmap 标记 queue，并使已注册的 TUN input node interrupt pending；不在 callback 中收包、分配 Buffer 或每次 `node_by_name`。input node 每轮只遍历本 worker poll vector；按升序 queue id 与取模分配关系直接索引本 worker queue，并核对 id，不做线性查找。VPP 来源为 `tap.c:142-184`、`internal.h:181-191`、`rx_queue.c:146-156,260-284`、`rx_node.c:394-423`。

TX queue 由现有 `InterfaceMain::register_tx_queue` 和 `assign_tx_queue_to_worker` 记录；每个 Data Worker 至少独占一个 queue。**service output 选择 queue，并将 `queue_id` 写入 TX Frame scalar；TUN 的 device TX 函数只按 scalar 访问本 worker 所拥有的 queue，不得自行轮转。** service 在 queue 分配时预建 worker/hardware interface 的 power-of-two `Vec<TxFrame>` 查找表；一条 queue 直接取首项，多条 queue 调硬件类 `tx_hash(&Buffer)` 再用 hash mask 查表。TUN 硬件类提供 VPP `vnet/hash/crc32_5tuple.c:compute_ip4_key/compute_ip6_key` 对应的 L3 5-tuple hash；service 不解释 IP 字段。按 queue id 分帧，不能让不同 queue 的 Buffer 共用一个 scalar。TUN queue id 升序注册并按 id 对 Data Worker 数取模分配，所以设备 TX 可由 `queue_id / worker_count` 直接索引本 worker 的 queue，再断言 id 一致；不在线性扫描中查找。来源：VPP `interface/runtime.c:185-241`、`interface_output.c:344-374,381-522`、`tap/internal.h:187-191`。多个 TX vring 可以按 VPP `tap.c:848-936` 映射到同一 TUN fd；每个 vring 仍只被一个 Hammer worker 修改。thread 0 没有 RX/TX queue，也不驱动 TUN node。

```rust
// hammer-service::interface；VPP interface.h:515,717-738、interface/runtime.c:185-241。
struct HwClass {
    tx_hash: Option<fn(&Buffer) -> u32>,
    // 其余现有 class 字段不变。
}
struct InterfaceMain {
    tx_lookups: Vec<UnsafeCell<Vec<Vec<TxFrame>>>>, // worker -> hw -> hash table
    // 其余现有 interface 状态不变。
}
impl InterfaceMain {
    fn assign_tx_queue_to_worker(&self, queue: u32, worker: DataWorkerId)
        -> InterfaceResult<()>;
    fn tx_frame_for_packet(&self, worker: DataWorkerId, hw: u32,
                           buffer: &Buffer) -> Option<TxFrame>;
}
// plugin/tuntap；VPP tap.c:55、vnet/hash/crc32_5tuple.c:22-72。
#[inline(always)]
fn tun_ip_flow_hash(buffer: &Buffer) -> u32;
```

```rust
// plugin/tuntap；VPP internal.h:41-129, tap.c:154-216。
#[repr(C)]
struct TunRxQueue {
    cacheline0: CacheLineAlignMark,
    // ring 拥有稳定、cacheline-aligned 的 desc/avail/used 存储；
    // buffers[desc_id] 是 BufferIndex，不是 payload 副本。
    ring: Virtqueue,
    buffers: Vec<Option<u32>>,
    kick: OwnedFd,
    call: OwnedFd,
    file_index: Option<u32>,
    last_used: u16,
    desc_next: u16,
    desc_in_use: u16,
    queue_id: u16,
    cacheline1: CacheLineAlignMark,
    queue_index: Option<u32>,
}

#[repr(C)]
struct TunTxQueue {
    cacheline0: CacheLineAlignMark,
    ring: Virtqueue,
    buffers: Vec<Option<u32>>,
    kick: OwnedFd,
    last_used: u16,
    desc_in_use: u16,
    free_head: u16,
    cacheline1: CacheLineAlignMark,
    queue_id: u16,
    queue_index: Option<u32>,
}

// VPP tap_main_t/tap_if_t 见 internal.h:107-183；Main 先于设备实例发布。
struct TunMain {
    memory: MemoryTable,
    interface: std::cell::UnsafeCell<Option<TunInterface>>,
}

// 当前 TOML 仅支持一台设备；不为此发明第二套 interface 索引。
struct TunInterface {
    hw_if_index: u32,
    sw_if_index: u32,
    input_node: NodeId,
    next: TunRxNext,
    tun_fds: Vec<OwnedFd>,
    vhost_fds: Vec<OwnedFd>,
    workers: Vec<TunWorker>,
}

#[repr(C)]
struct TunWorker {
    cacheline0: CacheLineAlignMark,
    state: std::cell::RefCell<TunWorkerState>,
}
struct TunWorkerState {
    rx: Vec<TunRxQueue>,
    tx: Vec<TunTxQueue>,
}

// 私有 ABI owner；其构造一次性分配 Linux vring 要求的对齐内存，并把
// desc/avail/used 区间固定到关闭为止。VPP tap.c:303-405。
struct Virtqueue {
    descriptors: AlignedVring,
    available: AlignedVring,
    used: AlignedVring,
}

// Linux virtio ring descriptor 的固定 ABI 布局；VPP virtio_net.h/vhost.h。
#[repr(C)]
struct VirtioDescriptor {
    addr: u64,
    len: u32,
    flags: u16,
    next: u16,
}

// 私有稳定对齐 allocation；Box 通过全局 Main Heap 分配，多留至多 63 字节
// 以取得 64-byte aligned 的 flexible-array 区间，由 Rust Drop 释放。
struct AlignedVring {
    storage: Box<[u8]>,
    aligned_offset: usize,
    length: usize,
}
```

`AlignedVring` 不是观察另一个 owner 的 borrow wrapper，而是该队列唯一拥有的、满足 Linux flexible-array ABI 对齐与固定地址要求的分配。`Box` 仍走 process-global Main Heap；固定 offset 只在 plugin 内暴露受控 slice/volatile 操作，不引入第二个 allocator，也不手工释放。若没有可核验布局的 crate，plugin 内 `#[repr(C)]` 定义仅覆盖使用到的 Linux UAPI struct，并加 size/offset 断言，不能把 vhost 或 virtqueue 抽象放进 service。vring 内存地址由 plugin 固定到关闭为止；Rust 中 kernel 共享的 index 以 acquire/release 原子访问，descriptor/used entry 在对应 index acquire 之后读，在 avail index release 之前写。VPP `rx_node.c:200-206,61-80`、`tx_node.c:69-79,341-351` 是排序依据。`TunMain` 的 `Sync` 仅由“每个 worker 只借其固定下标槽，main 只在 worker 启动前/停止后写”的私有 unsafe 契约证明；`RefCell` 的重复可变借用仍在本 worker 内报编程错误。call fd 注册 File 时复制一个 `OwnedFd`，所以 queue 的 eventfd 和 File 所拥有的 poll fd 不会双重关闭。main thread 仅在启动前或停止所有 worker 的 barrier 内修改注册关系，不在 packet path 加 `Mutex/RwLock/SpinLock`。`CacheLineAlignMark` 分开 queue 热字段、queue identity 和 worker 槽；不把整个 queue 放进一个共享锁。

**内存表前置条件。** VPP `tap_init` 用一个 `vlib_physmem_main_t` 区间 (`tap.c:1412-1435`)。Hammer 的 Buffer arena 可有多个 NUMA `PhysmemMap`，不能照抄一个 base/size。`VHOST_SET_MEM_TABLE` 必须覆盖每个可能由 vring descriptor 引用的 Buffer backing 区间，`guest_phys_addr` 与 `userspace_addr` 采用稳定的相同 VA 映射；数量、页对齐、无重叠和内核接受情况在发布前验证。`PhysmemMain` 已拥有每个 map 的 `base()/size()`，但当前没有公开迭代；需要一个返回 `&PhysmemMap` 的借用迭代方法，不复制或包装整个映射列表。Buffer pool 数量和 backing 在 worker 启动前固定；未来动态扩容必须先重新设计 vhost 内存表更新的 barrier/停队列时机。

```rust
// hammer-infra::physmem；VPP tap.c:1412-1435 的内存区间枚举。
impl PhysmemMain {
    pub fn maps(&self) -> impl Iterator<Item = &PhysmemMap>;
}
```

## 4. RX node

替换旧 `tuntap-rx` 算法为 TUN-only `tun-input`，仍声明 `sibling_of = DeviceInputNode`，初态 Interrupt。每 queue 的 call fd readiness 触发该 worker queue pending；node 对 pending queue 批量执行 VPP `tap_device_input_one_inline` 的 TUN 分支：acquire `used.idx`，最多取 `DEFAULT_BUFFER_FRAME_CAPACITY` 个完整包，先借用首段 Buffer 中的 virtio header，确认完整多段包已到，再按 `num_buffers` 直接链接 Buffer 并设置各段长度；Buffer index 直接写 driver Frame 的 vector 槽，不经中间数组或 payload copy。在 head 写 RX interface、L3 offset/总长，按 IP 首字节映射 IPv4/IPv6/drop；`next_indices` 是容量 256 的初始化数组，按 `i < n_rx, i += 8` 原地做非对齐 `u16x8` 读写，末组访问的填充槽不会入图。批量 enqueue next，最后按 64 个 descriptor 一批 refill 并 release `avail.idx`，需要时 kick。包/字节统计本轮不引入；保留 VPP 的 RX node error counter。未消费的 used entries 下一轮仍可调度，避免只靠下一次 edge-triggered call 才继续。VPP 来源：`rx_node.c:30-87,89-188,191-285,285-423`；Hammer 不复制 Ethernet 分支。

```rust
// plugin/tuntap；VPP rx_node.c:191-285,285-423。
#[hammer_component_macros::graph_node(
    graph = tuntap, init = register_tun_input, role = driver, name = "tun-input",
    sibling_of = hammer_service::device::DeviceInputNode
)]
struct TunInputNode;

// VPP VLIB_REGISTER_NODE(tap_input_node) 初态 INTERRUPT，rx_node.c:412-423。
fn register_tun_input(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_driver(TunInputNode::new())?;
    runtime.nodes().set_node_state(node, NodeState::Interrupt)?;
    runtime.register_node_errors(node, &TUN_RX_ERROR_DESCRIPTORS)?;
    Ok(node)
}

impl Node for TunInputNode {
    fn process(runtime: &mut DataPlaneMain,
               node: &mut NodeRuntime, frame: &mut Frame) -> usize;
}

// 对应 tap_rx_dequeue 的 TUN 分支：返回完整包数，头数组和 next 数组写入调用者的
// 固定 frame 容量 slice；没有私有 payload Vec。VPP rx_node.c:191-285。
#[inline(always)]
fn tun_rx_dequeue(runtime: &mut DataPlaneMain, queue: &mut TunRxQueue,
                  buffers: &mut [u32], nexts: &mut [u16], rx_bytes: &mut u64) -> usize;

// 对应 tap_refill_vring；一次发布已准备好的 descriptor，不暴露手工 unlock。
// VPP rx_node.c:30-87。
#[inline(always)]
fn tun_refill_vring(runtime: &mut DataPlaneMain,
                     queue: &mut TunRxQueue, node: NodeId);
```

`tun_rx_dequeue` 不负责 IP parse、TCP、host netlink 或 File readiness；`tun-input` 只做 IP version next 与 device-input feature 起点，IP/TCP 仍由各自插件节点处理。TUN virtio header 占用 payload 前的 headroom，L3 起点按 VPP `TUN_DATA_OFFSET` 处理；不能把 header 作为 IP 首字节。未知版本走 drop next，不生成控制面 `Result`。RX ring 空时正常返回 0；Buffer 不足只增加本 node 的 `BufferAlloc` counter，满 ring 增加 `FullRxQueue` informational counter；counter 在一批末汇总，不逐包写日志。VPP `internal.h:23-27`、`rx_node.c:17-27,48-57,210-214`。

## 5. TX 设备函数与回收

动态 `<interface>-tx` 保持 service `DeviceClass` 的输出边界；TUN plugin 的 device TX 函数从 Frame scalar 指定的、当前 worker 独占的 TX queue 取 Buffer heads。先 acquire used index，回收已完成 descriptor 和整条 Buffer chain；然后按 frame 预算从 free descriptor list 取槽：单 Buffer 直接引用 Buffer payload 前的 virtio-net header，链式 Buffer 使用 VPP 的 indirect descriptor（描述符存放于一个额外 Buffer，payload 不复制），把 head/index 存于 `queue.buffers[desc_id]`；一批末 release `avail.idx` 并必要时 kick。剩余无槽 packet 的 Buffer 必须释放且计入 `NoFreeSlots`；已发布 descriptor 的 Buffer 只能在 **used completion 后** 释放，不能在 TX 函数返回时像旧 `tuntap_tx` 一样全部 free。VPP `interface.h:722-738`、`interface_output.c:381-522`、`tx_node.c:61-88,220-424`。

```rust
// hammer-service::interface；VPP interface.h:vnet_hw_if_tx_frame_t。
#[repr(C)]
struct TxFrame {
    queue_id: u32,
    hints: u16,
    shared_queue: u8,
    reserved: u8,
}

// hammer-runtime；VPP interface_output.c:enqueue_one_to_tx_node。
// Scalar 不同且旧 Frame 非空时必须封口并分配新 Frame。
impl DataPlaneMain {
    fn enqueue_to_next_with_scalar<N, S>(
        &mut self, node: &mut NodeRuntime, frame: &mut Frame,
        nexts: &[N], scalars: &[S],
    ) where N: NodeNext, S: Copy + Eq + FromBytes + IntoBytes + Immutable + KnownLayout;
}

// plugin/tuntap；VPP tx_node.c:376-391：队列身份只从 Frame scalar 取得。
fn tun_intfc_tx(runtime: &mut DataPlaneMain,
                node: &mut NodeRuntime, frame: &mut Frame) -> usize;
```

```rust
// plugin/tuntap；VPP tx_node.c:61-88,220-424。
#[inline(always)]
fn tun_free_used_device_desc(runtime: &mut DataPlaneMain, queue: &mut TunTxQueue);

// 返回已发布或按具体 drop reason 已消费的 head 数；剩余由调用者计 NoFreeSlots。
// VPP tap_if_tx_inline，tx_node.c:220-374。
#[inline(always)]
fn tun_if_tx(runtime: &mut DataPlaneMain, queue: &mut TunTxQueue,
             buffers: &[u32], gso_enabled: bool, csum_offload_enabled: bool,
             drops: &mut [u64; 5]) -> usize;

```

`gso`/`csum_offload` TOML 默认 false。启用时分别按 VPP `set_gso_offsets`/`set_checksum_offsets` (`tx_node.c:110-219`) 填 virtio header，并由 TUN ioctl 协商；禁用时带相应 flags 的包按 `GsoPacketDrop`/`CsumOffloadPacketDrop` 分类，而不是把错误 checksum 送往 host。TX 函数先 `free_used`，发布一批，若仍有包且 vring 满，最多再重试两次；最后集中写各 node error counter 并释放未入队 Buffer。TUN queue 非 shared，不取 VPP 的 shared-queue spinlock；本次也不做 GRO。TX `NoFreeSlots`、`TruncPacket`、`IndirectDescAllocFailed` 只属于本 node；不把 packet drop 包装成 `RuntimeError`。TX used descriptor 按 VPP 在下一次 TX dispatch 回收，不新增 completion node；关闭时停 backend 后回收剩余 used/未消费 descriptor，才释放 ring 与 Buffer。空闲期被 TX queue 持有的 Buffer 是有界在途所有权，不是丢失的 Buffer。

## 6. host netlink、错误与回收

仅主线程控制面引入三个具体 crate：`netlink-sys = "0.9"`（同步 NETLINK_ROUTE socket）、`netlink-packet-core = "0.9"`（NetlinkMessage/header/payload 与序列/ACK 解析）、`netlink-packet-route = "0.33"`（结构化 RTM_GETLINK/NEWLINK/NEWADDR 消息）。它们只进入 `hammer-plugin-tuntap` 的 Cargo 依赖，不放进 service/runtime；实施时锁定兼容的实际 patch 版本。此处不选 async `rtnetlink`，因为现有后置 `config_function` 是同步回调，不应为了配置 host link 再创建独立 Tokio runtime。以 TUN 返回的真实 ifname 查询 ifindex，按 VPP `tap.c:749-810` 配 IPv4/IPv6 host 地址、link up、MTU；每次请求消费匹配 sequence 的 ACK/`NLMSG_ERROR`，不能把 send 成功当配置成功。没有 gateway/route API，也不以字符串拼装 netlink 消息。不要对当前进程 `setns`；host namespace 留待独立设计。`TUNSETIFF`、vhost ioctl、eventfd 属于 Linux 设备 UAPI，不替换为 netlink。

```rust
// plugin/tuntap；VPP tap.c:749-810 与 vnet/devices/netlink.c:28-113,199-353。
impl TunMain {
    // config 生命周期内依次执行；失败时设备实例尚未发布。
    fn configure_host_link(config: &TunConfig, ifindex: u32)
        -> Result<(), TunConfigError>;
}

// plugin/tuntap 的启动错误；没有 VPP 数字 retval 或宽泛字符串 variant。
// VPP tap.c:522-574,663-810,821-936 的 error/goto cleanup 入口。
#[derive(Debug, thiserror::Error)]
enum TunConfigError {
    #[error("invalid TUN queue count: RX {rx}, TX {tx}")]
    QueueCount { rx: u16, tx: u16 },
    #[error("invalid {queue} ring size {size}")]
    RingSize { queue: &'static str, size: u16 },
    #[error("invalid TUN MTU {mtu}")]
    Mtu { mtu: u32 },
    #[error("open /dev/net/tun")]
    OpenTun { #[source] source: std::io::Error },
    #[error("query TUN capabilities")]
    TunFeatureQuery { #[source] source: std::io::Error },
    #[error("TUN multi-queue is unavailable for {requested} queues")]
    MultiQueueUnsupported { requested: u16 },
    #[error("attach TUN queue {queue_id}")]
    TunAttach { queue_id: u16, #[source] source: std::io::Error },
    #[error("open vhost-net queue {queue_id}")]
    OpenVhost { queue_id: u16, #[source] source: std::io::Error },
    #[error("create queue {queue_id} eventfd")]
    EventFd { queue_id: u16, #[source] source: std::io::Error },
    #[error("vhost-net is missing required features {missing:#x}")]
    VhostFeatureUnsupported { missing: u64 },
    #[error("install vhost-net memory table")]
    VhostMemoryTable { #[source] source: std::io::Error },
    #[error("install vhost-net vring {queue_id}")]
    VhostVring { queue_id: u16, #[source] source: std::io::Error },
    #[error("bind vhost-net backend for queue {queue_id}")]
    VhostBackend { queue_id: u16, #[source] source: std::io::Error },
    #[error("configure host link {ifindex}")]
    HostLink { ifindex: u32, #[source] source: std::io::Error },
    #[error("configure {address} on host link {ifindex}")]
    HostAddress { ifindex: u32, address: ipnet::IpNet,
                  #[source] source: std::io::Error },
    #[error("configure MTU on host link {ifindex}")]
    HostMtu { ifindex: u32, #[source] source: std::io::Error },
}

// plugin/tuntap node-local counters；VPP rx_node.c:17-27、
// internal.h:209-255、tx_node.c:354-424。无数值协议码。
enum TunRxError { BufferAlloc, FullRxQueue }
enum TunTxError {
    NoFreeSlots, TruncPacket, IndirectDescAllocFailed,
    GsoPacketDrop, CsumOffloadPacketDrop,
}
```

无效 `num_*_queues`/ring size/MTU 是 `TunConfigError` 的可恢复配置错误，检查必须在创建 fd 前完成；真实设备操作的失败保留 `io::Error` source，netlink 内核 ACK errno 也转成原始 `io::Error` source。`FileMain::add`、service interface 注册和 IP 地址添加的失败保持各自 owner 的现有 typed error，在 plugin 配置边界翻译一次；不能吞掉或打印后返回成功。已经提交到 graph 的 Buffer ownership 状态错误是本模块 bug，应断言并由 worker 边界收口。VPP `tap.c:1048-1061` 在 error 分支释放未发布的 queue/fd；Hammer 依靠 `OwnedFd`、ring owner 的 Drop 回收未发布内核资源，Buffer 则由持有 `DataPlaneMain` 的启动/关闭边界明确归还；已发布关闭必须先停止 worker queue、取消 File、停止 vhost backend、确认 used ring/Buffer 归还，再释放 rings 与 fd。不能先释放 physmem 或 descriptor 内存。

## 7. 前置 API、隔离契约与验收

已批准并接入的通用 API 是 `PhysmemMain::maps()`、service 的 RX poll vector、`HwClass::tx_hash` 和 worker TX lookup、core/runtime 的 Frame scalar 布局和 fanout；它们不含 TUN/vhost 类型。继续复用 `File::private_data`、`set_polling_thread_index`、`InterfaceMain::register_{rx,tx}_queue`、`assign_tx_queue_to_worker`、`DataPlaneMain` Buffer/Frame API；不加第二套通用 Device Main。plugin 私有的 `TunRxQueue/TunTxQueue/TunConfigError` 是 vhost UAPI 的具体状态，不能放进 service/runtime。本 ADR 不授权顺手扩展 TCP、IP、Session 或 io_uring。

层隔离：runtime File 只通知指定 worker 的 queue readiness；service 管 interface/queue 身份、worker 分配、动态 TX 边；plugin/tuntap 管 Linux fd、vhost virtqueue 和 TUN node；IP plugin 管 Hammer 侧地址、IP next 后的解析与转发。TUN node 不做 host netlink、IP lookup、TCP；service 不存 plugin 指针/状态，runtime 不认 TUN queue。thread 0 不运行 dataplane，所有 queue 热字段由 owner worker写，kernel-shared vring 仅用已说明的 acquire/release 与 UnsafeCell/volatile 访问；无跨 worker packet-path 锁。

实现记录：TUN 的 RX/TX ring 存储使用 `UnsafeCell<u8>`，worker 在发布 `avail.idx` 前写 descriptor/available entry，内核在更新 `used.idx` 后由 worker acquire 读取 used entry；`used.flags` 的通知判定也使用 acquire。RX 和 TX queue 在创建时按 ID 升序、按 worker 数取模分配，两个 node 均按 `queue_id / worker_count` 直接取本 worker 的 queue 并核对身份。rewrite 与 interface-TX DPO 仍进入 service 的 interface output，再由该层按 hash 生成 TX Frame scalar。静态源码核对已完成；以下构建、运行、内存与性能验收按用户要求没有执行，不能据此声称运行正确或性能改善。

实施验收（按本轮要求，**不执行**编译、测试或 CI 命令）：

1. 结构检查：不再有单 fd `readv/writev` RX/TX 路径、`IFF_TAP`/Ethernet next、host gateway/default route、旧 `tuntap-rx/tuntap-tx` 并行实现；TOML 未改成 CLI。
2. 控制面：1/2/多 RX queue 创建和关闭；能力缺失、ring 非法、vhost setup、netlink ACK 错误均不发布半成品 interface；host 仅有指定 CIDR 的 connected route，无插件添加的默认路由。
3. 数据面：每 RX queue 分布到正确 worker；IPv4/IPv6 包经 `tun-input -> ip[46]-input -> local/lookup -> TCP`；frame 满后续处理不丢唤醒；链式 RX/TX 的 Buffer 只在正确完成点归还；连续 TX 和空闲后的关闭均无泄漏。
4. 同步与性能：TSAN/地址检查覆盖 ring publish/consume；以 1 和多 worker 对比 packet/s、cycles/packet、syscalls/packet、RX full/no-slot counters、重传；不能只凭启用 vhost 声称吞吐提升。

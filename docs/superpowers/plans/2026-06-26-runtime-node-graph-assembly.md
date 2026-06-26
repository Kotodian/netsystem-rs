# Runtime Node-Graph Assembly + Dataplane/Network Config Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

## 目标

两件事：
1. **Node-graph 组装移到 runtime**：把 `service.rs` 内联命令式注册搬成 `hammer-runtime` 的 `NodeRegistry` + `PacketGraphAssembler`，节点+边在代码里静态注册（对齐 VPP `VLIB_REGISTER_NODE`），不是 TOML 配置。
2. **Dataplane / network 配置**：新增 `[worker]`（线程模型 + CPU/调度/NUMA）和 `[network]`（tcp/ip/session/interface）配置节，按 cfg feature 分平台，复用现成 crate，不自研平台代码。

**Main thread 不跑数据包**：纯控制面（tokio control thread）。所有包处理在 worker threads（对齐 VPP main/worker 分离，对齐 Hammer 现状 `DataRuntime`）。

## VPP 调研结论（语义参考，非 1:1 API）

- `VLIB_REGISTER_NODE`：`__attribute__((constructor))` 静态登记节点进全局链表。节点 + `.next_nodes = { [slot] = "name" }` 边声明都在**代码**里，非用户配置。
- `vlib_node_main_init`：init 阶段把名字解析成索引。
- `[cpu]` startup.conf：`main-core`/`corelist-workers`（手动）或 `workers`+`skip-cores`（自动）+ `relative`（容器亲和子集）+ `scheduler-policy`/`scheduler-priority`。
- `[buffers]`：`data-size`（buffer 数据区字节）+ `buffers-per-numa`（每 NUMA 节点缓冲数）。
- NUMA 感知：每个 worker 用本地 NUMA node 的 buffer。
- `vlib_frame_queue_main_init`：per-worker handoff 队列，`vlib_buffer_enqueue_to_thread` 跨 worker 投递。
- Feature arc（`VNET_FEATURE_ARC_INIT`/`VNET_FEATURE_INIT`）：代码静态声明，用户 config 只 `vnet_feature_enable_disable` 启用/禁用——**Hammer 已有 `FeatureArcControl`/`Feature<A>`，不动**。

## macOS/iOS 平台特性调研

- **无 CPU 亲和**：`THREAD_AFFINITY_POLICY` 在 Apple Silicon 返回 `KERN_NOT_SUPPORTED`。XNU 不支持 thread→core 绑定。
- **无 NUMA**：无公开 API，内核自动管理 cache/memory 局部性。
- **无实时调度**：`SCHED_FIFO/RR` 用户态不可用。
- **有 QoS**：`pthread_set_qos_class_self_np(qos_class, rel_pri)`，4 档 `userInteractive`/`userInitiated`/`utility`/`background`，XNU 据此选 P/E core + 频率。这是 Apple Silicon 对应"调度策略"的能力。

## 库选型（能用现成库就不自研，零额外 C 依赖）

| 能力 | 库 | 说明 |
|---|---|---|
| CPU 数 | `num_cpus` | `core_affinity` 间接依赖 |
| CPU 亲和 | `core_affinity` | workspace dep；Linux 生效，macOS no-op 但可编译 |
| 调度策略（Linux） | `thread-priority` | workspace dep，`linux-platform` feature |
| QoS（macOS/iOS） | `libc::pthread_set_qos_class_self_np` | 已有 `libc`，直接调系统 API |
| NUMA node 探测 | `libc::syscall(SYS_getcpu)` | 已有 `libc`，无 C 依赖 |
| NUMA 内存绑定 | `libc::syscall(SYS_mbind)` | 已有 `libc`，无 C 依赖 |

**结论**：只新增 `core_affinity` + `thread-priority` 两个纯 Rust crate。NUMA/QoS 全靠 `libc` syscall / 系统 API，不 link libnuma/hwloc，不自研平台封装。

## 平台 cfg feature 分层

`hammer-core`（config schema）+ `hammer-runtime`（worker 调度）加 feature：

- `linux-platform`（默认在 Linux 编译）：暴露 `[worker.cpu]`（亲和）+ `[worker.scheduler]`（policy/priority）+ `[worker.numa]`。
- `apple-platform`（默认在 macOS/iOS 编译）：暴露 `[worker.scheduler]`（qos）。不暴露 cpu/numa。
- 两者皆无（其它平台）：只暴露 `worker.count`/`stack_size`/`buffer`/`handoff`/`app_ring` + `network.*`。

## 配置 schema（无 Config/Raw 后缀，单层 Deserialize）

```toml
[worker]
count = 2                              # service.rs:63  DATA_WORKER_THREADS
stack_size = "2MB"                      # service.rs:67  DATA_WORKER_STACK_SIZE
max_blocking_threads = 4               # service.rs:68  DATA_MAX_BLOCKING_THREADS
idle_slice = "1ms"                      # spawn.rs:137   DATA_WORKER_IDLE_SLICE

[worker.buffer]
slot_bytes = 2048                       # spawn.rs:135   DATA_BUFFER_SLOT_CAPACITY  (VPP data-size)
slots_per_numa = 4096                   # spawn.rs:136   DATA_BUFFER_SLOTS           (VPP buffers-per-numa；非 Linux = 总量)
frame_capacity = 256                    # buffer.rs:29   DEFAULT_BUFFER_FRAME_CAPACITY
frame_pool_size = 64                    # buffer.rs:30   DEFAULT_BUFFER_FRAME_POOL_SIZE

[worker.handoff]
queue_capacity = 1024                   # handoff.rs:56  DataPlaneHandoff::new(workers, queue_capacity)

[worker.app_ring]
capacity = 256                          # service.rs:417 AppContext::with_ring_capacity(.., 256)

# --- linux-platform only ---
[worker.cpu]
main_core = 0                           # VPP main-core（main thread 核，不跑包）
worker_cores = [1, 2]                   # VPP corelist-workers（手动）
# 或自动：
skip_cores = 1                          # VPP skip-cores
relative = false                        # VPP relative

[worker.scheduler]                      # linux: policy/priority
policy = "other"                        # other|batch|idle|fifo|rr  (VPP scheduler-policy)
priority = 0                            # VPP scheduler-priority (仅 fifo/rr)

[worker.numa]
enabled = false                         # true 时按 numa_node_of_cpu(worker_core) 分配 buffer

# --- apple-platform only ---
[worker.scheduler]                      # apple: qos
qos = "userInteractive"                 # userInteractive|userInitiated|utility|background

[network.tcp]
mss = 1440                              # output.rs:10    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN
receive_window = 65535                  # connection.rs   DEFAULT_TCP_WINDOW
congestion = "bbr"                      # 目前仅 bbr
time_wait = "60s"                       # connection.rs   TCP_TIME_WAIT_TICKS=6000 (100ms tick)
paws_idle = "24h"                       # connection.rs   TCP_PAWS_IDLE

[network.tcp.retransmit]               # connection.rs:28-30
initial = "50ms"
min = "50ms"
max = "60s"

[network.tcp.keepalive]                # connection.rs:46-60
idle = "75s"
probe_interval = "75s"
probe_limit = 8

[network.ip.reassembly]                # reassembly.rs:33-35
timeout = "100ms"
max_reassemblies = 1024
max_fragments_per_reassembly = 64

[network.session]
timer_tick = "10ms"                     # runtime.rs:21   DEFAULT_SESSION_TIMER_TICK
pool_capacity = 1024                    # runtime.rs:22   DEFAULT_SESSION_POOL_CAPACITY
ready_queue_capacity = 1024             # ready.rs:3      DEFAULT_READY_QUEUE_CAPACITY
app_session_capacity = 1024             # app.rs:13       DEFAULT_APP_SESSION_CAPACITY
ooo_capacity = 8                        # runtime.rs:55   DEFAULT_OOO_CAPACITY

[network.session.buffer]               # app.rs:56       DataPlaneBuffers::with_buffer_capacity(2048, 1)
slot_bytes = 2048
slots = 1

[[network.interface]]
name = "tun0"
driver = "tun"                          # 目前仅 tun
mtu = { l3 = 9000, ip4 = 9000, ip6 = 9000, mpls = 9000 }  # interface.rs InterfaceMtu
```

## 类型（无 Config/Raw 后缀）

`hammer-core/src/config/network.rs`：
- `Network { tcp: Tcp, ip: Ip, session: Session, interface: Vec<Interface> }`
- `Tcp { mss, receive_window: u32, congestion: CongestionController, time_wait, paws_idle: Duration, retransmit: Retransmit, keepalive: Keepalive }`
- `Retransmit { initial, min, max: Duration }`
- `Keepalive { idle, probe_interval: Duration, probe_limit: u8 }`
- `Ip { reassembly: Reassembly }`
- `Reassembly { timeout: Duration, max_reassemblies, max_fragments_per_reassembly: usize }`
- `Session { timer_tick: Duration, pool_capacity, ready_queue_capacity, app_session_capacity, ooo_capacity: usize, buffer: SessionBuffer }`
- `SessionBuffer { slot_bytes, slots: usize }`
- `Interface { name: String, driver: InterfaceDriver, mtu: InterfaceMtu }`
- `InterfaceMtu { l3, ip4, ip6, mpls: u32 }`
- `enum InterfaceDriver { Tun }`
- `enum CongestionController { Bbr }`

`hammer-core/src/config/worker.rs`：
- `Worker { count: usize, stack_size: usize, max_blocking_threads: usize, idle_slice: Duration, buffer: WorkerBuffer, handoff: WorkerHandoff, app_ring: WorkerAppRing, cpu: WorkerCpu, scheduler: WorkerScheduler, numa: WorkerNuma }`
  - `cpu`/`numa` 仅 `linux-platform` feature；apple 的 `scheduler` 用同名字段不同形状 → 用 `#[cfg]` 分支或 enum。
- `WorkerBuffer { slot_bytes, slots_per_numa, frame_capacity, frame_pool_size: usize }`
- `WorkerHandoff { queue_capacity: usize }`
- `WorkerAppRing { capacity: usize }`
- `WorkerCpu { main_core: Option<usize>, worker_cores: Vec<usize>, skip_cores: usize, relative: bool }`（linux）
- `WorkerScheduler`：linux = `{ policy: SchedulerPolicy, priority: i32 }`；apple = `{ qos: QosClass }`。用 cfg 分两个 struct 或 enum。
- `enum SchedulerPolicy { Other, Batch, Idle, Fifo, Rr }`
- `enum QosClass { UserInteractive, UserInitiated, Utility, Background }`
- `WorkerNuma { enabled: bool }`（linux）

## 文件责任图

- `crates/hammer-core/src/config/worker.rs`（新）+ `network.rs`（新）+ `mod.rs` 接入 `RawConfig`/`Options`。
- `crates/hammer-runtime/src/graph/`（已落地 T3/T4）：`registry.rs`/`assembler.rs`/`mod.rs`。
- `crates/hammer-runtime/src/numa.rs`（新，linux）：`getcpu`/`mbind` 薄封装；apple/其它平台空 stub。
- `crates/hammer-runtime/src/spawn.rs`：`DataRuntime::new` 接 `Worker` 配置（亲和/调度/NUMA/buffer）。
- `crates/hammer-service/src/service.rs`：`install_service_packet_graph_on_workers` 改用 `PacketGraphAssembler` + `NodeRegistry<ServiceGraphDeps>`；`DATA_WORKER_THREADS` 等常量改从 `Options.worker` 取。
- `crates/hammer-component-macros/src/lib.rs`：加 `#[node_factory]` 属性宏派生纯边节点 builder fn（T8 简化）。
- `Cargo.toml`（workspace）：加 `core-affinity`/`thread-priority`。

## 不变量与约束

- main thread 不跑包；worker threads 跑包（现状不变，配置固化）。
- 节点图在代码静态注册（宏 + `NodeRegistry`），不是 TOML。config 只调参数（buffer 大小、timer、MTU），不声明节点/边。
- feature arc 不动（`FeatureArcControl`/`Feature<A>` 保留）。
- adapter 注册表面只加了 `try_register_descriptor`（T2 已完成）；不再动。
- assembler 拓扑序单趟装配（T1 证伪两阶段：adapter 注册时校验 `initial_nexts` NodeId 已注册 + `len==next_count`，占位不可行）。
- 无 `dyn`/`Any`/`NodeFactory` trait：`NodeBuilder<D> = fn(&NodeCtx<'_, D>) -> CoreResult<NodeId>`，`D` 泛型。
- NUMA 实现纯 `libc` syscall，不 link libnuma/hwloc；apple/其它平台空 stub。
- 配置结构体无 `Config`/`Raw` 后缀，单层 `Deserialize`。
- 不破坏现有 8 个预存失败（2026-06-25 plan 记录），不试图在本计划修它们。
- AGENTS.md：新结构在 `hammer-runtime`/`hammer-core` 层，非业务名；复用 `FeatureArcControl` 不重造；VPP 是语义参考非 1:1 API；无 `_value` 式下划线前缀。

## Todos

- [x] **T1 adapter: 确认 node_by_name + set_node_next_slot 已公开（契约测试）** — 已完成。`crates/hammer-adapter/tests/node_runtime.rs` 加 `node_by_name_returns_registered_id_and_none_for_unknown` + `set_node_next_slot_redirects_existing_next_slot`。关键发现：`register_descriptor`（`node.rs:751-754`）强制 `initial_nexts.len()==next_count`，占位 `NodeId::new(0)` 被拒（`validate_node` out of bounds）→ 证伪两阶段名解析，确立拓扑序装配。

- [x] **T2 adapter: 公开 try_register_descriptor** — 已完成。`NodeRuntime::try_register_descriptor(kind, descriptor) -> CoreResult<NodeId>`（`node.rs`），契约测试 `try_register_descriptor_registers_erased_descriptor` + `try_register_descriptor_rejects_next_count_mismatch`。

- [x] **T3 runtime: NodeRegistry + NodeCtx（泛型 D，无 any/dyn）** — 已完成。`crates/hammer-runtime/src/graph/registry.rs`：`NodeCtx<'a, D>` + `NodeBuilder<D> = fn(&NodeCtx<'_, D>) -> CoreResult<NodeId>` + `NodeRegistry<D>`。`D` 是 service 依赖袋，graph 层不命名其字段。

- [x] **T4 runtime: PacketGraphAssembler 拓扑序装配** — 已完成。`graph/assembler.rs`：`GraphSpec { nodes: Vec<&'static str> }` + `PacketGraphAssembler::assemble_on(runtime, worker_id, deps)` 单趟拓扑序，builder 用 `ctx.node(name)` 解析前序节点边。测试 `assembler_registers_nodes_in_dependency_order` 通过。

- [x] **T5 core: network.rs（tcp/ip/session/interface，无 Config 后缀）** — 已完成。新建 `crates/hammer-core/src/config/network.rs`：`RawNetwork`/`RawTcp`/`RawRetransmit`/`RawKeepalive`/`RawIp`/`RawReassembly`/`RawSession`/`RawSessionBuffer`/`RawInterface`/`RawInterfaceMtu`/`RawCongestionController` + `Network`/`Tcp`/`Retransmit`/`Keepalive`/`Ip`/`Reassembly`/`Session`/`SessionBuffer`/`Interface`/`InterfaceDriver`/`InterfaceMtu`/`CongestionController`（无 Config 后缀）。默认值全部对齐 `hammer-service` 生产常量（MSS=1440、window=65535、RTO 50ms/50ms/60s、time_wait=600s、paws_idle=24h、keepalive 75s/75s/8、reassembly 100ms/1024/64、session 10ms/1024/1024/1024/8、buffer 2048/1、MTU 9000）。`mod.rs` 接入 `RawConfig.network` + `Options.network` + `build_options` 解构/构造。8 个单元测试通过（含 zero-mss、min>initial、duplicate/empty interface、unknown driver 拒绝 + 默认值对齐生产常量）。`cargo test -p hammer-core --lib` 56 passed。

- [ ] **T6 core: worker.rs（cfg 分平台）** — 新建 `crates/hammer-core/src/config/worker.rs`，`linux-platform`/`apple-platform` feature 分支。接入 `RawConfig.worker` + `Options.worker`。加 `hammer-core` Cargo features `linux-platform`/`apple-platform`（用 `#[cfg(target_os)]` 自动启用）。测试 `config_parse_worker_count`/`config_parse_worker_linux_cpu`（linux feature）/`config_parse_worker_apple_qos`（apple feature）。跑 `cargo test -p hammer-core --lib`。

- [ ] **T7 runtime: NUMA + 亲和 + 调度接入** — 新建 `crates/hammer-runtime/src/numa.rs`（`libc::syscall(SYS_getcpu)` 探测 + `SYS_mbind` 内存绑定，linux；其它平台 stub）。改 `spawn.rs` `DataRuntime::new` 接 `Worker` 配置：`core_affinity::set_for_current` 绑核（linux）、`thread-priority` 设调度（linux）、`libc::pthread_set_qos_class_self_np` 设 QoS（apple）、NUMA buffer 分配（linux + `numa.enabled`）。加 workspace dep `core-affinity`/`thread-priority`。测试 `worker_pinned_to_core`（linux）/`worker_qos_set`（apple）。跑 `cargo test -p hammer-runtime --lib`。

- [ ] **T8 service: 节点 builder + assembler 接入 + 宏简化** — `hammer-component-macros` 加 `#[node_factory]` 属性宏：对 `#[node]` 纯边节点自动生成 `fn build_<node>(ctx: &NodeCtx<'_, D>) -> CoreResult<NodeId>`（用 `ctx.node("dep-name")` 解析边 + `try_register_internal`）。`hammer-service` 定义 `ServiceGraphDeps { tcp_control, ... }`，为 9 个生产节点注册 builder 到 `NodeRegistry<ServiceGraphDeps>`，`install_service_packet_graph_on_workers` 改用 `PacketGraphAssembler`。复杂节点（TcpListenNode/TcpInputNode 需 control/queue）手写 builder。测试 `default_node_registry_contains_*` + `service_lifecycle_assembles_via_runtime_assembler`。跑 `cargo test -p hammer-service --lib` + `--test service_lifecycle`。

- [ ] **T9 verify: 全量回归 + clippy** — `cargo test --workspace`、`cargo clippy -p hammer-runtime -p hammer-adapter -p hammer-service -p hammer-core --lib --tests`、`cargo test -p hammer-service --release --lib transport::tcp`。记录结果到末尾"执行结果"节。不修预存失败，仅确认无新回归。

## 验证命令

```bash
cargo test -p hammer-core --lib
cargo test -p hammer-runtime --lib
cargo test -p hammer-service --lib
cargo test --workspace
cargo clippy -p hammer-runtime -p hammer-adapter -p hammer-service -p hammer-core --lib --tests -- -D warnings
cargo test -p hammer-service --release --lib transport::tcp
```

## Self-Review

- **Spec coverage:** 节点名键反查（T1）、类型擦除注册入口（T2）、节点 builder inventory（T3）、拓扑序 assembler（T4）、network 配置（T5）、worker 配置 + cfg 分平台（T6）、NUMA/亲和/调度接入（T7）、节点 builder 注册 + 宏简化 + assembler 接入（T8）、全量回归（T9）。覆盖"node graph 组装在 runtime + dataplane/network 配置 + NUMA + main 不跑包"全部诉求。
- **VPP 对齐：** 节点+边代码静态注册（`VLIB_REGISTER_NODE`/`next_node_names`）、worker 线程模型（`[cpu]`）、buffer（`[buffers]` data-size/buffers-per-numa）、NUMA 感知、handoff 队列。feature arc 不动。语义参考非 1:1 API。
- **平台差异：** cfg feature 分 linux/apple/其它，macOS/iOS 无 cpu/numa（XNU 不支持），用 QoS 替代 scheduler-policy。库选型纯 Rust + libc syscall，零 C 依赖。
- **AGENTS.md 合规：** 新结构在 hammer-runtime/hammer-core 层；复用 FeatureArcControl；无 dyn/Any；无 _value；VPP 语义参考。

## 执行交接

T1-T4 已完成（见 todos）。T5 起按本 plan 执行。配置结构体无 Config/Raw 后缀；node graph 是代码静态注册非 TOML；NUMA 纯 libc syscall；main thread 不跑包。

# ADR-0023: Node Stats 的注册与采集（`/sys/node/*` 与 `/nodes/<name>/*`）

Status: proposed — 设计草案，尚未批准实现（本文只覆盖 node runtime stats：条目用
`#[derive(Stats)]` 注册、值由 collector 采集、`cpu_time_now` 落在 `hammer-infra`；
不含 buffer/mem/session 家族，也不含 `clear` 命令面）
Date: 2026-09-17

> **前置依赖：本文按 ADR-0021 第四版的机制写。**采集者表是进程私有的
> `Vec<Box<dyn Collector>>`（`Collector { entry_index(&self); collect(&self, &DirectoryEntry) }`）、
> 登记是 `StatsMain::register_collector(&mut self, impl Collector + 'static)`、轮次
> `StatsMain::collect(&self)` 不取锁、值写走条目级 `DirectoryEntry::set_simple_counter_cell`、
> 结构变更走 D10 的段锁 guard。这些在 ADR-0021 里仍是 proposed（尚未实现）；
> **ADR-0021 的 M1/M2 未落地前不得实施本文的 M1–M4**。本文不重复论证 ADR-0021 已给出的
> 机制决定，只在需要**新增** surface 的地方标注"需批准"。
>
> **与 ADR-0019 的关系：**ADR-0019 D4.3 把"node calls/vectors/clocks/suspends 的真实 owner
> 更新点，再导出 `/sys/node/*`、名字与 `/nodes/<name>/*` symlink"列为 M4，并在 §9 第 10 条
> 要求运行期结构变更发生在 thread zero（或先由 `WorkerBarrier` 停住 worker）。本文就是那条
> M4 的设计；ADR-0019 §6.6.1 里"`per_node_counters` 控制 node collector 的目录注册和周期采样"
> 在本文落地。
>
> **旧 ADR-0022（node + buffer 合写）已删除**，本文不复活它的进程级 node 计数块、
> `publish_*` 家族步骤或 `MainLoopRate` 之类自造类型。buffer 家族见 ADR-0022。

## 1. 范围与结论

**用户要求：**设计 node stats：把 `cpu_time_now` 引入 `hammer-infra`，参考 VPP
`vlib_node_stats_t` 与 `update_node_counters`；先出设计，不写代码，不执行测试，不启动 daemon。

**已核实的 VPP 事实：**

- **三级计数**：每线程 `vlib_node_runtime_t` 的 32 位 `clocks/calls/vectors_since_last_overflow`
  由派发点直接累加（`node.h:482-503`、`main.c:546-577`，溢出时回滚并同步），64 位
  `vlib_node_stats_t` 只在同步点增加（`node.h:226-233`、`main.c:469-483`），采集点把
  "`stats_total - stats_last_clear`"写进目录单元格（`collector.c:105-123`，V1–V5、V14）。
- **目录契约 = 五个固定条目 + 每节点四条别名**：`/sys/node/names`（名字向量，元素 = 节点下标）
  与 `/sys/node/{clocks,vectors,calls,suspends}`（简单计数向量：**行 = 线程，列 = 节点**），
  加 `/nodes/<node-name>/<counter>` 别名（`collector.c:18-27,79-89`、`stat_client.c:222-241,271-278`，
  V10–V13、V19、V22）。
- **这五条条目受开关控制，并且是在 collector process 启动时建的**：`if (sm->node_counters_enabled)`
  才建 `/sys/node/names` 与四个计数向量（`collector.c:154-176`，V16）；开关来自
  `per-node-counters on|off`（`init.c:154-159`，V24）。
- **名字与别名在采集点的 diff 里重建**（`collector.c:40-93`，V12、V13）：VPP 的节点向量可能
  换名换序，所以它每轮比对名字、只在变化时取段锁、删旧条目再建 name vector 与别名。
- **采集不做 barrier 同步**：`vlib_node_get_nodes(..., barrier_sync = 0)`，源码注释写明
  "Barrier sync across stats scraping. Otherwise, the counts will be grossly inaccurate."
  （`node.c:667-726`，V17）——VPP 接受"统计值读到的是迟一拍、跨项不同步的快照"。
- **`suspends` 属于 process 节点**：`p->n_suspends += 1`（`main.c:1272`、`:1376`，V9），
  只在 first main 同步（`main.c:499-528`，V6）。`max_clock`/`max_clock_n` 只有 `show node`
  消费（`node_cli.c:373-392`，V25），**不在目录里**。
- **`clocks` 的单位是裸 CPU 计数器**：`clib_cpu_time_now()`（`time.h:44-56` x86_64 的 `rdtsc`、
  `:94-103` aarch64 的 `cntvct_el0`，V28）没有 fence、不换算；只有 `show node time` 才乘
  `seconds_per_clock`（`node_cli.c:390-392`、`time.h:20-24,188-208`，V28）。

**已核实的 Hammer 事实：**

- **图里今天没有任何 node 计数**：`node_counters` 在 `crates/` 只命中
  `StatsConfig.per_node_counters`、`StatsMain::init` 的参数与 `StatsSegment::node_counters_enabled()`
  ——开关存在但**没有任何消费者**；`/sys/node`、`/nodes/` 路径在整个 workspace 里没有命中（H8、H10、H18）。
- **派发点只有一处**：`DataPlaneMain::dispatch_node`（`node.rs:2190-2205`），被
  `run_ready_function_nodes` 的两个循环调用（`node.rs:2105`、`:2165`），节点函数的返回值就是
  VPP 的 `n_vectors`（处理的包数）（H2）。
- **节点身份在 worker 启动前冻结**：`start_workers` 用 `main.nodes().node_count()` 给
  `DataPlaneHandoff` 冻结节点容量（`start_workers.rs:20-24`、`handoff.rs:172-179`，H5）；
  图发布只允许"同槽同名"的增量（`inherit_worker_state` 断言名字/role/error 布局不变，
  `node.rs:522-548`，H3）；`rebuild_graph*` 今天只有测试调用者。**因此 VPP 那套"名字 diff +
  段锁重建"在 Hammer 里是断言，不是每轮的循环。**
- **每线程发布值已有既定形状**：`WorkerThread` 把 `main_loop_count`/`loops_per_second` 放在
  各自的 cacheline 上，注释写明"只有拥有者线程写、只有轮次读，所以 `Relaxed` 足够"
  （`worker_thread.rs:26-46,88-110`，H6）；轮次的采集者通过 `ThreadMain` 按 thread index 读它们
  （`thread_main.rs:325-352`、`config/stats.rs:145-183`，H7、H8）。
- **启动顺序**：`run_init_functions`（stats 登记）→ config → **图物化** → main-loop-enter
  （`start_workers` 发射 worker）→ barrier → `start_processes`（collector process）（`main_loop.rs:41-53`，H12）。
  也就是说：**stats 登记时图还是空的，节点数/名字只有 main-loop-enter 之后才是事实**——这正是 VPP
  把五条条目放到 collector process 里的原因（V16）。
- **机制侧已有全部原语**：`add_name_vector`/`set_name`（会像 `vec_validate` 一样增长）、
  `add_simple_counter`/`validate`、`add_symlink`、条目级 `set_simple_counter_cell`
  （`segment.rs:232-292,314-330,428-438,471-556,569-620`，H10；写入口名见 ADR-0021 D9.2）。
- **`NodeId` 可按 slot 枚举**（`NodeId::new(slot)`、`NodeId::slot()`，`hammer-core/src/graph/id.rs:5-16`），
  `NodeMain::node_count`/`node_name` 提供图事实（`node.rs:1558,1713-1720`，H4）。
- **`hammer-infra` 没有 CPU 时间戳原语**（H17）；runtime 的时间窗口今天用 `std::time::Instant`
  （`data_plane/worker.rs:41-57`，H14）。

**结论（拟议，均需批准）：**

- **D0 目录契约**：`/sys/node/names`（`NameVector`）+ `/sys/node/{clocks,vectors,calls,suspends}`
  （`CounterVectorSimple`，行 = 线程、列 = 节点）+ `/nodes/<name>/<counter>`（`Symlink`，指向第
  `<node slot>` 列），逐条对应 V10–V13、V19、V22；只在 `per_node_counters = true` 时存在（V16、V24）。
- **D1 计数存储 = 每个线程图里的每节点四个 `Relaxed` 原子格**（`NodeMain` 的计数器行，
  槽 = `NodeId::slot()`），在 `start_workers` 按冻结的节点容量随图建好；线程描述符上只放
  指向该行的句柄（VPP `vlib_worker_thread_t.vlib_main`），不放假存储。归 VPP
  `vlib_node_runtime_t` 的每线程计数器（V3）；**不复制 VPP 的 32 位累加 + 溢出同步**
  （理由见 D1.4）。
- **D2 `cpu_time_now()` 落在 `hammer-infra::time`**（VPP `clib_cpu_time_now`，V28）：x86_64 用
  std 稳定 intrinsic `core::arch::x86_64::_rdtsc`、aarch64 用一条 `core::arch::asm!("mrs …,
  cntvct_el0")`（std 的 aarch64 稳定模块没有计数器 intrinsic），**不引第三方 crate**（候选对照
  见 D2：`quanta` 在 ARM 上走 stdlib fallback、`tick_counter` 零依赖但 API 是 benchmark 用的
  `start/stop`），返回裸计数器；`clocks` 列因此是 ticks，不做秒换算（V25 的 `seconds_per_clock`
  需要 VPP 那套校准，Hammer 没有该消费者）。
- **D3 声明**：五条条目用**一条 `#[derive(Stats)]` 声明**描述，由本模块的
  `#[stats_collect_registration]` 项在开关打开时 install（不再进 image 自动装，因为开关要能
  决定条目是否存在）；形状（行/列）不在 install 时建立——它是图事实。
- **D4 图事实的发布**：节点数/名字/别名/形状在 main-loop-enter 的**一步**里发布（V16 + V13 的
  第一次 diff；Hammer 的节点身份冻结，所以只发布一次，不需要每轮 diff）。
- **D5 采集**：一个 `NodeCounterCollector` 类型 + `counter: NodeCounter` 字段，四个实例
  （每计数器一条登记），写 `row = 线程`、`column = 节点`（V14 的单元格写）。
- **D6 `suspends`**：来自 process 节点的挂起点（thread zero 行，V9、V6）；`calls/vectors/clocks`
  对 process 节点恒为 0（Hammer 的 process 节点是 Tokio future，没有派发点）——这是 D7 里
  明写的差异。
- **D7 有意差异与不做项**：见 D7、D8（`max_clock`、`clear` 基线、process 节点三计数、barrier 采样）。
- **D9 stats client**：客户端分层与 ADR-0021 D7 相同；`/sys/node/*` 与 `/nodes/*` 的语义属于家族
  provider `NodeStatsProvider`（`const PREFIX = "/sys/node"`、`type Report = Option<NodeStats>`）；
  开关关闭时家族整体不存在 → `Ok(None)`（absence 是普通成功结果，不是错误）；typed 路径只把五个基
  条目各读一次、按槽号投影，不逐节点读 `4N` 条 symlink。读取规则见 D9，候选签名见 6.5。

## 2. VPP 源码证据

| 编号 | 位置 | 事实 |
| --- | --- | --- |
| V1 | `third_party/vpp/src/vlib/node.h:226-233` | `vlib_node_stats_t { u64 calls, vectors, clocks, suspends; u64 max_clock; u64 max_clock_n; }`，注释 "Total calls, clock ticks and vector elements processed for this node"。 |
| V2 | `src/vlib/node.h:280-287` | `vlib_node_t` 上两个 `vlib_node_stats_t`：`stats_total` 与 `stats_last_clear`；注释 "Current values are always stats_total - stats_last_clear"。 |
| V3 | `src/vlib/node.h:473-503` | `vlib_node_runtime_t`（每线程）的 32 位 `clocks_since_last_overflow`、`calls_since_last_overflow`、`vectors_since_last_overflow`、`max_clock`、`max_clock_n`。 |
| V4 | `src/vlib/main.c:546-577` | `vlib_node_runtime_update_stats(vm, node, n_calls, n_vectors, n_clocks)`：三个 32 位字段累加、更新 `max_clock`/`max_clock_n`，溢出（`ca1 < ca0` 等）时回滚并调 `vlib_node_runtime_sync_stats`。 |
| V5 | `src/vlib/main.c:469-483` | `vlib_node_runtime_sync_stats_node`：`stats_total.X += n_X + r->X_since_last_overflow`，随后把三个 32 位字段清零；`max_clock`/`max_clock_n` 直接覆盖。 |
| V6 | `src/vlib/main.c:499-528` | `vlib_process_sync_stats` 额外做 `stats_total.suspends += p->n_suspends` 并清零；`vlib_node_sync_stats` 对 `VLIB_NODE_TYPE_PROCESS` 只在 first main 同步（"Nothing to do for PROCESS nodes except in main thread"）。 |
| V7 | `src/vlib/main.c:832-835,899-916` | `dispatch_node(..., u64 last_time_stamp)`：节点返回后 `t = clib_cpu_time_now()`，随后 `vlib_node_runtime_update_stats(vm, node, 1, n, t - last_time_stamp)`，函数返回 `t` 作为下一次派发的 `last_time_stamp`。 |
| V8 | `src/vlib/main.c:1466-1472,1519` | 每轮主循环开始取一次时间戳（main 线程用 `clib_time.last_cpu_time`，worker 用 `clib_cpu_time_now()`），循环里在派发前再刷新一次；派发之间用返回值串联。 |
| V9 | `src/vlib/main.c:1272,1376` | process 每次进入挂起：`p->n_suspends += 1`。 |
| V10 | `src/vlib/stats/collector.c:9-16` | `enum { NODE_CLOCKS, NODE_VECTORS, NODE_CALLS, NODE_SUSPENDS, N_NODE_COUNTERS }`。 |
| V11 | `src/vlib/stats/collector.c:18-27` | `node_counters[] = { [NODE_CLOCKS] = {"clocks"}, … }`，每项带 `entry_index` 与 `name`。 |
| V12 | `src/vlib/stats/collector.c:38-57` | `update_node_counters`：`vlib_node_get_nodes(0, ~0, 1 include_stats, 0 barrier_sync, &node_dups, &stat_vms)`；`node_data[]` 与节点名 `vec_is_equal` 比对，变化的下标进 bitmap。 |
| V13 | `src/vlib/stats/collector.c:58-93` | 名字变化时：`vlib_stats_segment_lock()` → 释放旧名字与四条 symlink →（第二个循环，注释说明节点下标可能换位）`vlib_stats_set_string_vector(&node_names, n->index, "%v", n->name)` → 逐计数器 `vlib_stats_validate(entry, last_thread, n_nodes - 1)` 与 `vlib_stats_add_symlink(entry, n->index, "/nodes/%U/%s", name, counter_name)`。 |
| V14 | `src/vlib/stats/collector.c:95-127` | 单元格写：对每个线程 `j`、每个节点 `i`，`counters[j][n->index] = n->stats_total.X - n->stats_last_clear.X`（四个计数器各一次）。 |
| V15 | `src/vlib/stats/collector.c:132-151` | `do_stat_segment_updates`：**先** `if (sm->node_counters_enabled) update_node_counters (sm);`，**再**遍历 `sm->collectors` 调 `c->fn(&data)`，最后 heartbeat。node 计数不是注册表里的一行。 |
| V16 | `src/vlib/stats/collector.c:154-180` | `stat_segment_collector_process`：`node_counters_enabled` 时在**循环之前**建 `node_names = vlib_stats_add_string_vector("/sys/node/names")` 与四个 `vlib_stats_add_counter_vector("/sys/node/%s", name)`；随后设 boottime，循环 `do_stat_segment_updates` + `vlib_process_suspend(update_interval)`。 |
| V17 | `src/vlib/node.c:667-726` | `vlib_node_get_nodes(..., include_stats, barrier_sync, ...)`：`barrier_sync` 时才取 `vlib_worker_thread_barrier_sync`；注释 "Barrier sync across stats scraping. Otherwise, the counts will be grossly inaccurate."；`include_stats` 时对每个线程的每个节点调 `vlib_node_sync_stats`。 |
| V18 | `src/vlib/stats/stats.c:472-524` | `vlib_stats_validate(index, idx0, idx1)`：外层向量扩到 `idx0`、每行扩到 `idx1`；**只在会扩容时**取段锁，并在锁内 `clib_mem_set_heap(sm->heap)` 分配。 |
| V19 | `src/vlib/stats/stats.c:528-562` | `vlib_stats_add_symlink(entry_index, vector_index, fmt, ...)`：建 `STAT_DIR_TYPE_SYMLINK` 条目，`index1 = entry_index`、`index2 = vector_index`；名字已存在则返回 `~0`。 |
| V20 | `src/vlib/stats/stats.c:320-364` | `vlib_stats_set_string_vector`：`vec_validate(e->string_vector, vector_index)` 增长名字向量，取段锁，`vec_reset_length` 后格式化写入。 |
| V21 | `src/vlib/stats/stats.c:375-380` | `vlib_stats_add_counter_vector` = 新建 `STAT_DIR_TYPE_COUNTER_VECTOR_SIMPLE` 条目。 |
| V22 | `src/vpp-api/client/stat_client.c:210-241,271-278` | `copy_data`：简单计数向量按 `index2` 复制"该列跨所有线程的值"；`SYMLINK` 解引用到 `index1` 条目 + `index2` 列。 |
| V23 | `src/vlib/stats/init.c:13-48,123-131` | `vector_rate_collector_fn` 用 `counters[0]`（外层只有一行）写 per-thread 值；`vlib_stats_init` 里建 `/sys/vector_rate_per_worker`、`/sys/loops_per_worker`、`validate(entry, 0, n_threads)` 并 `vlib_stats_register_collector_fn`。 |
| V24 | `src/vlib/stats/init.c:154-159` | `per-node-counters on|off` → `sm->node_counters_enabled`。 |
| V25 | `src/vlib/node_cli.c:373-392` | `show node` 用 `stats_total - stats_last_clear` 的四个差值与 `max_clock`/`max_clock_n`；`use_time` 时 `l *= 1e9 * vm->clib_time.seconds_per_clock`。 |
| V26 | `src/vlib/node_cli.c:628-645` | `clear node counters`：`vlib_worker_thread_barrier_sync` → 逐节点 `vlib_node_sync_stats` → `stats_last_clear = stats_total` → `max_clock = 0` → `vlib_stats_set_timestamp(STAT_COUNTER_LAST_STATS_CLEAR, …)`。 |
| V27 | `src/vlib/threads.c:975-1010` | 图 refork 时按 index 复制 `stats_total`/`stats_last_clear`；新节点克隆清零。 |
| V28 | `src/vppinfra/time.h:20-24,44-56,94-103,188-208` | `clib_cpu_time_now()`：x86_64 `rdtsc`、aarch64 `mrs cntvct_el0`，无 fence、不换算；`clib_time_now_internal` 累加差值后乘 `seconds_per_clock` 才是秒。 |
| V29 | `src/vlib/stats/stats.h:28-31` | 段预建的固定槽只有 `/sys/last_stats_clear`、`/sys/heartbeat`、`/sys/boottime`——**没有时钟频率条目**，所以目录里读不到"ticks → 秒"的换算系数。 |

## 3. Hammer 现状与差异

| 编号 | 位置 | 事实 |
| --- | --- | --- |
| H1 | `crates/hammer-runtime/src/node.rs:114-160` | `NodeRuntime { words: [u64; 4], cached_next_index, flags }`：每节点的 runtime data，worker 拥有（`runtime_data`），发布时按槽保留。 |
| H2 | `crates/hammer-runtime/src/node.rs:507-517,2105,2165,2190-2205` | 派发只有一处：`NodeRuntimeSlot::dispatch` 被 `DataPlaneMain::dispatch_node` 调用，后者是 `run_ready_function_nodes` 两个循环的唯一派发点；节点函数返回值 = 处理的包数。 |
| H3 | `crates/hammer-runtime/src/node.rs:522-548` | `inherit_worker_state`：断言新图"只增不减"、同槽名字/role/error 布局不变，并保留 worker 自己的 `runtime_data`/`node_states`/`input_main_loops_per_call`。节点身份（槽 → 名字）因此是冻结事实。 |
| H4 | `crates/hammer-runtime/src/node.rs:620-637,726-772,1558,1713-1720` | 槽在 `push_node_slot` 时先占位（名字 `None`），注册时填名字；`node_count()` 与 `node_name(NodeId) -> Option<&'static str>` 是图事实的只读面。 |
| H5 | `crates/hammer-runtime/src/start_workers.rs:20-24`；`crates/hammer-runtime/src/handoff.rs:168-179` | `DataPlaneHandoff::with_node_capacity(worker_count, queue_capacity, main.nodes().node_count())`：节点容量在 worker 发射前冻结，同一点可用于建计数器行。 |
| H6 | `crates/hammer-runtime/src/worker_thread.rs:26-46,88-110` | `WorkerThread` 是 `#[repr(C)]` + cacheline 标记 + 编译期 offset 断言；`main_loop_count`/`loops_per_second` 各占一条 cacheline，访问器注释写明"只有拥有者线程写、只有轮次读 → `Relaxed`"。 |
| H7 | `crates/hammer-runtime/src/thread_main.rs:56-64,325-352,355-390` | `ThreadMain` 按 thread index 存 `WorkerThread`；`worker_main_loop_count`/`worker_loops_per_second` 是轮次读每线程发布值的唯一入口；`register_thread` 只追加非图线程（今天无调用者）。 |
| H8 | `crates/hammer-runtime/src/config/stats.rs:40-51,76-104,106-133,145-183,186-260` | `StatsConfig.per_node_counters` 已存在且**无消费者**；`Sys` 已声明 `/sys/main_loop_count_per_worker`、`/sys/loops_per_worker`（行 0、列 = worker），`collect_worker_main_loop` 是"读 `ThreadMain` 发布值 → 写单元格"的现成样例；`statseg-collector-process` 今天只做 boottime + `StatsMain::collect()` + sleep；四条统计登记（两个堆、main loop、worker 数）走 image。 |
| H9 | `crates/hammer-stats/src/lib.rs:31-70` | 今天的采集者行是 `CollectorRegistration { collect: fn(DirectoryIndex, u32), entry_index, vector_index }`，表是 `SpinLock<Vec<Collector>>`；轮次在段锁里跑。ADR-0021 第四版把它改成 `Box<dyn Collector>` + 无锁轮次 + 条目级写。 |
| H10 | `crates/hammer-stats/src/segment.rs:177-179,232-292,314-330,428-438,471-556,569-620` | 机制原语齐备：`node_counters_enabled()`、`add_simple_counter`/`validate`、`add_name_vector`、`add_symlink(target, column, name)`、`set_name`（会像 `vec_validate` 一样增长名字向量）、`set_simple_counter`。 |
| H11 | `crates/hammer-stats/src/protocol.rs:207-217`；`crates/hammer-stats/src/lib.rs:20-24` | `DirectoryType { ScalarIndex, CounterVectorSimple, … , NameVector, … }`；`Counter`/简单计数向量每格是 `u64`。 |
| H12 | `crates/hammer-runtime/src/main_loop.rs:41-53` | 启动顺序：`run_init_functions`（stats 登记）→ config → 图物化 → main-loop-enter（`start_workers`）→ barrier → `start_processes`（collector process）。**stats 登记早于图物化。** |
| H13 | `crates/hammer-runtime/src/main_loop.rs:75-141` | worker 主循环的固定步序（barrier → file → 调度 → `run_ready_nodes` → … → `increment_main_loop_count`）。 |
| H14 | `crates/hammer-runtime/src/data_plane/worker.rs:33-58`；`crates/hammer-runtime/src/data_plane/main.rs:40-60` | `increment_main_loop_count` + 200µs 阻尼窗口是"热路径写发布值"的现有实现（用 `std::time::Instant`）。 |
| H15 | `crates/hammer-runtime/src/process.rs:97-127,173-190` | process 节点的挂起点：`process_wait`（定时挂起）与 `take_process_events`（事件等待）把节点放进 `suspended_processes`；process 节点归 thread zero 的 `NodeMain`。 |
| H16 | `crates/hammer-runtime/src/lib.rs:14-41` | registration image 的各列表；`main_loop_enter_functions = [start_workers::__INIT_FN_START_WORKERS]`，`stats_registrations` 今天五条。 |
| H17 | `crates/hammer-infra/src/`（无 `time.rs`；`rg 'cpu_time\|rdtsc' crates/hammer-infra/src` 无命中） | infra 今天没有 CPU 时间戳原语；`hammer-runtime` 的时间窗口用 `std::time::Instant`。 |
| H18 | `rg '"/sys/node"\|"/nodes/"' crates/`（仅 `hammer-stats/src/mem.rs:49` 用 `add_symlink` 建 `/mem/*` 别名） | 今天没有任何 node 条目、名字向量或 `/nodes/*` 别名。 |

## 4. 拟议决定

### D0 目录契约与开关

| 目录名 | 类型（`DirectoryType`） | 形状 | 取值（VPP 出处） |
| --- | --- | --- | --- |
| `/sys/node/names` | `NameVector` | 元素 = 节点槽 | 节点名（VPP `n->name`，V13、V16） |
| `/sys/node/clocks` | `CounterVectorSimple` | 行 = 线程，列 = 节点槽 | 累计 CPU ticks（V1、V3、V14） |
| `/sys/node/vectors` | `CounterVectorSimple` | 同上 | 累计处理的包数（VPP `n_vectors`，V7、V14） |
| `/sys/node/calls` | `CounterVectorSimple` | 同上 | 累计派发次数（V4、V14） |
| `/sys/node/suspends` | `CounterVectorSimple` | 同上 | process 节点累计挂起次数（V6、V9、V14） |
| `/nodes/<node-name>/{clocks,vectors,calls,suspends}` | `Symlink` | 指向对应计数条目的第 `<node slot>` 列 | 该节点跨所有线程的值（V13、V19、V22） |

- **行 = 线程**，集合 = thread zero + Data Workers（`0..=worker_count`）。`register_thread`
  附加的 runtime 线程没有节点图，不占行——VPP 的 `vlib_node_get_nodes` 同样跳过没有
  `vlib_main_t` 的线程（H7、V17）。
- **列 = 节点槽**（`NodeId::slot()`），名字向量也按同一槽号索引（V13：`set_string_vector(&node_names, n->index, …)`）。
- **别名**让客户端不必知道槽号：`/nodes/<name>/clocks` 解引用到 `/sys/node/clocks` 的第
  `<slot>` 列，读出来是"该节点每个线程一个值"（V22）。
- **开关**：五条条目与四条别名只在 `per_node_counters = true` 时存在（V16、V24）；关闭时
  `/sys/node/*` 与 `/nodes/*` 在目录里完全不存在（不是零值条目），轮次也没有 node 家族的工作。
  这符合 ADR-0019 §6.6.1"控制 node collector 的目录注册和周期采样"。开关的现有载体是
  `StatsConfig.per_node_counters`（`config/stats.rs:51`）与 `StatsSegment::node_counters_enabled()`
  （`segment.rs:177-179`）——两者今天都没有消费者（H8、H10）。
- **客户端语义**：`/sys/node/names` + 四个向量是"原始表"，`/nodes/*` 是"按节点名取一列"的便利面；
  两条路径的值来自同一次投影（同一个采集者的同一格），不承诺跨条目/跨线程的同一瞬间快照（D7 第 2 条）。

### D1 每线程每节点计数器：位置、形状与更新点

**D1.1 位置与形状：计数器是本线程 node 行里的每节点一格（VPP `vlib_node_runtime_t`，
V3）；线程描述符上只放句柄（VPP `vlib_worker_thread_t.vlib_main` 指向该线程 main 的那一格）。**

```rust
// crates/hammer-runtime/src/node_stats.rs

/// 一个节点的四个累计计数：VPP `vlib_node_runtime_t` 的三个
/// `calls/vectors/clocks_since_last_overflow`（V3）加 `vlib_node_stats_t.suspends`（V1）。
///
/// 拥有该线程的执行体写 `calls`/`vectors`/`clocks`，thread zero 额外写 `suspends`；
/// 只有轮次读。四个格子因此各自是一个 `Relaxed` 原子——与 `WorkerThread::main_loop_count`
/// 同一条纪律（H6）。VPP 在同一位置是无同步的裸读写，并明确接受它的不精确（V17）。
pub(crate) struct NodeCounters {
    calls: AtomicU64,
    vectors: AtomicU64,
    clocks: AtomicU64,
    suspends: AtomicU64,
}

impl NodeCounters {
    /// 建一个线程的行：槽 = 节点（`NodeId::slot()`），与 node 向量同长、同一冻结点（H5）。
    pub(crate) fn row(node_capacity: usize) -> Arc<[NodeCounters]>;

    /// VPP `vlib_node_runtime_update_stats`（V4）里三行的 Hammer 形式：
    /// 一次派发 = `calls + 1`、`vectors += n`、`clocks += t - last_time_stamp`（V7）。
    #[inline(always)]
    pub(crate) fn update_dispatch(&self, vectors: u64, clocks: u64);

    /// VPP `p->n_suspends += 1`（V9）的落点；只有 thread zero 调（H15）。
    #[inline]
    pub(crate) fn add_suspend(&self);

    /// 读一格（轮次用；`Relaxed`，跨格不承诺一致快照）。
    #[inline(always)]
    pub(crate) fn value(&self, counter: NodeCounter) -> u64;
}
```

```rust
// crates/hammer-runtime/src/node.rs（图侧：计数器就是本线程 node main 的状态）

pub struct NodeMain {
    …
    /// 本线程每个节点槽的四个计数：VPP 该线程 `node_main` 里那一排
    /// `vlib_node_runtime_t` 的计数器字段。发射前建好、之后不再增长（H3）。
    node_counters: Arc<[NodeCounters]>,
}

impl NodeMain {
    /// 装本线程的行（发射前一次，见 D1.1 的时机）。
    pub(crate) fn install_node_counters(&mut self, row: Arc<[NodeCounters]>);

    #[inline(always)]
    pub(crate) fn node_counters(&self, node: NodeId) -> &NodeCounters;
}
```

```rust
// crates/hammer-runtime/src/worker_thread.rs（只加句柄，不加存储）

pub struct WorkerThread {
    …
    /// 本线程计数器行的句柄：VPP `vlib_worker_thread_t.vlib_main` 指向该线程
    /// `vlib_main_t` 的那一格。**计数器存储不在这里**（在 D1.1 的图里），这里只是轮次
    /// 跨线程读的入口，安装形状与 `barrier`/`join_handle` 相同。
    node_counters: OnceLock<Arc<[NodeCounters]>>,
}

impl WorkerThread {
    pub(crate) fn install_node_counters(&self, row: Arc<[NodeCounters]>);
    /// 轮次用；没有图的 runtime 线程没有这一格（D0 的行集合只含 thread zero + Data Worker）。
    pub(crate) fn node_counters(&self) -> Option<&[NodeCounters]>;
}
```

- **为什么不把存储放在线程描述符上**：VPP 把每线程计数器放在该线程自己的 node main
  （`vlib_node_runtime_t`，V3），`vlib_worker_thread_t` 上只有指向那个 main 的**指针**。
  把 `OnceLock<Box<[NodeCounters]>>` 当存储挂在描述符上，既不是 VPP 的位置，也把
  "节点自己的计数"写成了"线程的计数器数组"。
- **为什么行是共享句柄（`Arc`）**：轮次在 thread zero 上读别的线程的行；VPP 靠 raw 指针，
  Rust 里对应的是"发射前建好、交给线程"的共享句柄——仓库里同一形状已有
  `DataPlaneHandoff`（`Arc<DataPlaneHandoffInner>`）。行按线程各一份，**不跨线程复用**：
  worker 的图是克隆出来的（`worker_parts`，`node.rs:1076` 的 `Clone for NodeMain`），所以
  每一份克隆在发射前装自己那一行；refork（`inherit_worker_state`，H3）只改图的事实，不碰这行。
- **建与装的时机**：`start_workers` 读一次冻结的节点容量（`main.nodes().node_count()`，H5）：
  thread zero 的行装进自己的图，每个 worker 的行装进该 worker 的图克隆与它的描述符。全部在
  `launch` 之前完成，发射之后没有安装步骤，也没有"按节点数现装的 `Box<[NodeCounters]>`"。

**D1.2 更新点：`DataPlaneMain::dispatch_node`，一次派发写一次本线程图里的格子。**

```rust
// crates/hammer-runtime/src/data_plane/main.rs（新增字段）

/// 下一次派发测量 clocks 的起点：VPP `dispatch_node` 的 `last_time_stamp` 参数（V7），
/// 同一个值在主循环里是那个 `cpu_time_now` 局部变量（V8）。构造时取一次，
/// 每轮主循环在派发前刷新一次，其余时间由派发点推进。
pub(crate) last_time_stamp: u64,
```

```rust
// crates/hammer-runtime/src/node.rs（DataPlaneMain::dispatch_node 的收尾）

// VPP `dispatch_node`（V7）：节点返回后取一次时间戳，用它和本次派发起点之差更新
// 三个计数，并把这次的值留给下一次派发。写的是本线程图里那一格——VPP 同样是
// `vlib_node_runtime_update_stats (vm, node, …)` 写线程 main 的 node runtime。
let dispatch_end = cpu_time_now();
self.nodes
    .node_counters(node)
    .update_dispatch(count as u64, dispatch_end - self.last_time_stamp);
self.last_time_stamp = dispatch_end;
```

- `count` 就是节点函数的返回值，即 VPP 的 `n_vectors`（H2、V7）。
- **主循环刷新**：worker 主循环在派发步之前刷新一次 `last_time_stamp`（V8 的同位，
  `main_loop.rs:122-130` 的两个 `run_ready_nodes` 之前）；构造时初始化一次，避免 thread zero 在图重建等
  循环外派发时把"从进程启动到现在"记进 `clocks`。
- `clocks` 的语义因此与 VPP 逐字一致：**本轮第一次派发从"主循环刷新点"起算，其余派发从
  上一次派发的结束点起算**——它包含派发之间的主循环工作（文件轮询、handoff、调度），
  这是 VPP 的既有口径（V7、V8），不是"Hammer 累计的纯节点执行时间"。

**D1.3 轮次的读路径：线程描述符里的句柄，不取锁、不借 worker 的 `RefCell`。**

```rust
// crates/hammer-runtime/src/thread_main.rs

impl ThreadMain {
    /// 线程 `thread_index` 的 node 计数器行；`0..=worker_count()` 之外没有图，返回 `None`。
    pub(crate) fn thread_node_counters(&self, thread_index: u32) -> Option<&[NodeCounters]>;
}
```

轮次按 `0..=worker_count()` 取每一行、按 `NodeId::slot()` 读格子——与 VPP
`update_node_counters` 遍历各线程 `vlib_main` 的 `node_main.nodes`（V12、V14）同一形状。

**D1.4 为什么不复制 VPP 的"32 位累加 + 溢出同步"（有意差异，需批准）。**
VPP 的两级结构（32 位 per-thread → 64 位 `stats_total`，V3–V5）服务于两个目的：
① 热路径只碰线程私有内存里的窄字段；② 溢出时才付一次同步的代价（V4 的回滚分支）。
Hammer 的格子**本来就是跨线程发布位置**（线程图里的原子行，与 `main_loop_count` 同一条
发布纪律，H6），所以：

- 64 位 `Relaxed` 的加法在 x86_64/aarch64 上与 32 位同价，而它消掉了溢出分支、回滚、
  采集侧的同步路径与 32/64 两级结构；
- 丢掉 32 位并不丢语义：`calls +1`、`vectors += n`、`clocks += delta` 与 VPP 逐条相同（V4、V7）；
- 代价被明确接受：轮次读到的是"迟一拍、逐格独立"的快照（D7 第 2 条），与 VPP 注释自认的
  不精确（V17）同一性质。
- **保留**的 VPP 形状：per-thread + per-node 的位置（V3）、派发点唯一的更新调用（V7）、
  只增不减的累计量（V1）、`suspends` 只在 thread zero 的 process 路径增加（V6、V9）。

**D1.5 thread zero 的行。**thread zero 不跑数据面派发循环，但它是 process 节点的属主（H15），
所以它的行只承载 `suspends`（V6 的"PROCESS 节点只在 main thread 同步"在 Hammer 里的对应）。

### D2 `cpu_time_now`：落在 `hammer-infra`

**先查现成的（crates.io / docs.rs / 源码，2026-09-17；需求只是"读一次硬件计数器返回 u64"，
不校准、不换算、不要 `Instant`）：**

| 候选 | 事实 | 取舍 |
| --- | --- | --- |
| std `core::arch::x86_64::_rdtsc` / `_rdtscp` | 稳定 intrinsic（本机 `rustc 1.98.1` 编译验证；`doc.rust-lang.org/core/arch/x86_64` 索引里有） | **采用**：x86_64 不自己写 asm |
| std `core::arch::aarch64` | 稳定模块只有 `__isb`/`__dmb`/`__dsb`/`__nop`/`__wfe`/`__yield`/`__rndr` 等，**没有** `cntvct_el0`/`cntfrq_el0`（`doc.rust-lang.org/core/arch/aarch64` 索引） | aarch64 只能 `core::arch::asm!`，与 `prefetch.rs` 的既有写法同类（`hammer-infra/src/prefetch.rs:17-27`） |
| `quanta` 0.12.6 | README 平台矩阵写明 TSC 只支持 `x86`/`x86_64`，ARM 平台走 stdlib fallback（**不是** `cntvct_el0`）；依赖 `raw-cpuid`/`portable-atomic`/`libc`；API 是校准后的 `Clock`/`Instant`（`Clock::raw()` 只是副产品），自带 upkeep 线程与 `mock` feature | 不采用：语义是超集，且 ARM 上给不出硬件计数器 |
| `minstant` 0.1.7 / `fastant` 0.1.11 | 都是 `std::time::Instant` 的替代品（TSC + 校准），对外是纳秒时间点 | 不采用：不是裸 ticks |
| `tick_counter` 0.4.5 | **零依赖**；x86_64 `rdtsc`/`rdtscp`、aarch64 `mrs cntvct_el0`/`cntfrq_el0`（源码里就是要的那两条 asm），但对外是 `start()/stop()/elapsed()/frequency()`，其中 x86 的 `frequency()` 会实测 1 秒 | 不采用：为两条 asm 加依赖，还要再包一层 |
| `cputicks` 0.1.0 | 多架构（含 aarch64）tick 计数器，但只有 0.1.0 一个版本 | 不采用：同上，且单版本 |
| `armv8` 0.0.1 | 只覆盖 aarch64 寄存器访问 | 不采用：只解决一半 |

**结论（拟议）：不新增依赖。**VPP 的对应物就是两个一行函数（V28：x86_64 一条 `rdtsc`、aarch64
一条 `mrs cntvct_el0`，无 fence、不换算），Hammer 的 infra 层也已经在用 `core::arch`（`simd.rs`、
`prefetch.rs`）。如果以后要支持更多架构（`cputicks` 那种 riscv64/s390x 面），再按上表换成 crate，
届时改动只在这一个函数里。

```rust
// crates/hammer-infra/src/time.rs（新模块；lib.rs 加 pub mod time;）

/// 读一次 CPU 时间戳计数器：VPP `clib_cpu_time_now`（V28）。
///
/// 返回**裸计数器**（x86_64 `rdtsc`、aarch64 `cntvct_el0`），不是纳秒、不保证跨核可比、
/// 不带 fence 或串行化——VPP 的实现同样只有一条 asm。它只用于差值与统计；要变成时间
/// 需要 VPP 那套 `clib_time_init` 校准出的 `seconds_per_clock`（V28），而 Hammer 今天
/// 没有该消费者（V25、V29：目录里连时钟频率条目都没有）。
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn cpu_time_now() -> u64 {
    // SAFETY: `_rdtsc` 只读计数器，无副作用；不承诺与其它指令的先后序（VPP 同）。
    unsafe { core::arch::x86_64::_rdtsc() }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn cpu_time_now() -> u64 {
    let counter: u64;
    // SAFETY: `cntvct_el0` 是 EL0 可读的计数器（V28），无内存访问、不改栈与标志位。
    unsafe {
        core::arch::asm!(
            "mrs {}, cntvct_el0",
            out(reg) counter,
            options(nomem, nostack, preserves_flags)
        );
    }
    counter
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
compile_error!("cpu_time_now has no counter read for this target");
```

- **归属**：`hammer-infra` 是 Hammer 的 `vppinfra` 对应层，`clib_cpu_time_now` 就在
  `vppinfra/time.h`（V28）；`hammer-runtime → hammer-infra` 的依赖方向允许（H17）。
- **依赖**：新增任何候选 crate 都要走依赖审批；本设计的取舍是不加（理由见上表）。上表也是
  将来要换实现时的对照面——换 crate 只改 `time.rs` 一个函数，不动 `clocks` 列、不动 D1/D3–D5。
- **不做的事**：不做 TSC 校准（`clocks_per_second`/`seconds_per_clock`，V28）、不做
  `clib_time_now` 风格的秒累加、不做频率漂移校验；这些只在有人真的需要"ticks → 时间"时再加
  （V25 的 `show node time` 是 VPP 唯一的消费者）。今天 `clocks` 列就是 ticks。
- **调用点只有 D1.2 的两处**（循环刷新与派发结束）：一次派发两次读，和 VPP 相同（V7、V8）。

### D3 声明与登记：五条条目、四条采集者登记

```rust
// crates/hammer-runtime/src/node_stats.rs

/// `/sys/node/*` 的五条条目（V10–V13、V16）。
///
/// 这条声明**不进 registration image**：条目是否存在由 `per_node_counters` 决定（V16、V24），
/// 所以它由本模块的登记项在开关打开时 install（ADR-0022 D1.2 的"owner 驱动的参数化声明"
/// 同一形状：不进 image 的声明只生成 `install`，由 owner 自己决定何时装）。
/// 形状（行/列）也不在这里建立：行数 = 有节点图的线程数、列数 = 节点数，都是图事实，
/// 而图在 stats 登记之后才物化（H12）；VPP 同样在采集点才 `vlib_stats_validate`（V13）。
#[derive(Stats)]
pub(crate) struct NodeStats {
    #[stats(path = "/sys/node/names")]
    names: NameVector,
    #[stats(path = "/sys/node/clocks")]
    clocks: SimpleCounter,
    #[stats(path = "/sys/node/vectors")]
    vectors: SimpleCounter,
    #[stats(path = "/sys/node/calls")]
    calls: SimpleCounter,
    #[stats(path = "/sys/node/suspends")]
    suspends: SimpleCounter,
}

/// VPP `node_counters[]` 的一项（V10、V11）：目录名的最后一段、那一列计数器、条目索引。
/// 四个计数器共用一个采集者类型，`counter` 是实例自己的字段（用户要求）。
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeCounter { Clocks, Vectors, Calls, Suspends }

impl NodeCounter {
    pub(crate) const ALL: [Self; 4] = [Self::Clocks, Self::Vectors, Self::Calls, Self::Suspends];

    /// VPP `node_counters[].name`：`clocks|vectors|calls|suspends`（V11）。
    pub(crate) fn name(self) -> &'static str;

    /// VPP `node_counters[].entry_index`（V11）。
    fn entry_index(self, node_stats: &NodeStats) -> DirectoryIndex;
}

/// VPP `collector.c:161-176` + `vlib_stats_register_collector_fn`（V16、V23 的登记部分）：
/// 开关打开时建五条条目，并为每个计数器登记一个采集者。
#[hammer_component_macros::stats_collect_registration]
fn register_node_stats(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    if !stats_main.segment.node_counters_enabled() {
        return Ok(());     // V24：关闭时目录里没有 /sys/node/*，轮次也没有 node 家族工作
    }
    NodeStats::install(stats_main)?;
    let node_stats = NodeStats::global();
    for counter in NodeCounter::ALL {
        stats_main.register_collector(NodeCounterCollector {
            entry_index: counter.entry_index(node_stats),
            counter,
        });
    }
    Ok(())
}
```

- **登记时点**：与 ADR-0021 的既有登记项同一条通道（`run_stats_registrations()`，H12、H16），
  发生在 `StatsMain` 发布之前——`register_collector(&mut self, …)` 的前提（ADR-0021 D9.1）。
- **必须的宏 surface（需批准，A2/A3）**：① `#[derive(Stats)]` 支持 `NameVector` 字段
  （install 建空名字向量，长度由 `set_name` 增长，V20/H10）；② 计数向量的 `columns` 可省略，
  省略时 install 只建条目、不在 install 期 `validate`（形状由 D4 的图事实建立）。
  这两点是"声明只表达注册语义、形状属于运行期事实"的直接结果，宏里不出现 `/sys/node`、
  节点名或任何列序（H9 的边界不变）。
- **与既有登记同一个形态**：`NodeStats` 声明（注册语义）+ 本模块登记项（更新语义）。
  family 逻辑全在 `hammer-runtime` 的 node 域里，`hammer-stats` 机制不知道 node 概念（§5）。

### D4 图事实的发布：形状、名字与别名（一步，不每轮 diff）

```rust
// crates/hammer-runtime/src/node_stats.rs

/// 把图事实（节点数、节点名）装进 `/sys/node/*`，并建立 `/nodes/<name>/<counter>` 别名。
///
/// VPP 在 collector process 启动时建条目（V16），并在采集中**第一次名字 diff** 时
/// `validate` 形状、写名字向量、建别名（V13）。Hammer 的节点槽与名字在 worker 启动前冻结
/// （H3 的同槽同名断言），所以这些只发布一次：在图已物化、`start_processes` 之前的
/// main-loop-enter（H12）。发布点因此只有一个，且**不取 stats 之外的任何锁状态**。
#[hammer_component_macros::main_loop_enter_function]
fn install_node_stats(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let stats_main = StatsMain::global()?;
    if !stats_main.segment.node_counters_enabled() {
        return Ok(());                       // 条目不存在（D3），没有形状要发布
    }
    let node_stats = NodeStats::global();
    // 槽号是 u32（`push_node_slot` 里 `NodeId::new(u32::try_from(len).expect(…))`，H4），
    // 所以这里的转换不可能失败：失败就是不变量被破坏。
    let columns = u32::try_from(main.nodes().node_count())
        .expect("node slots are u32-indexed");
    if columns == 0 {
        return Ok(());                       // 0 节点只出现在测试图里：条目保留、形状为空
    }
    // 行 = thread zero + Data Workers（D0；非图 runtime 线程没有节点图）
    let rows = ThreadMain::global().worker_count() + 1;   // u32，不可能溢出
    for counter in NodeCounter::ALL {
        // V13 的 vlib_stats_validate（Hammer 只在发布点做一次）
        stats_main.segment.validate(counter.entry_index(node_stats), rows - 1, columns - 1)?;
    }
    for slot in 0..columns {
        let Some(name) = main.nodes().node_name(NodeId::new(slot))? else {
            continue;   // 未命名的槽没有名字与别名（VPP 的 n->name 由注册保证非空）
        };
        // V13 的 vlib_stats_set_string_vector(&node_names, n->index, "%v", n->name)
        stats_main.segment.set_name(node_stats.names.index, slot, name)?;
        for counter in NodeCounter::ALL {
            // V13 的 vlib_stats_add_symlink(entry, n->index, "/nodes/%U/%s", …)
            stats_main.segment.add_symlink(
                counter.entry_index(node_stats),
                slot,
                &format!("/nodes/{name}/{}", counter.name()),
            )?;
        }
    }
    Ok(())
}
```

- **每条理由**：形状来自 `validate`（V18/V13）、名字来自 `set_name`（V20/V13）、别名来自
  `add_symlink`（V19/V13）——三条都是既有机制原语，node 域只提供"哪个槽、哪个名字"。
- **为什么不是每轮 diff**：VPP 需要 diff 是因为它的节点向量按类型分表、下标可以换位（V13 的注释）；
  Hammer 的槽→名字是冻结事实，且 `inherit_worker_state` 已经断言它（H3）。因此
  **VPP 的 bitmap + 段锁重建在 Hammer 里退化为"发布一次 + 一条前提条件"**（前提条件见 §9 未决项）。
- **结构变更的锁**：`validate`/`set_name`/`add_symlink` 走 ADR-0021 D10 的段锁 guard
  （只会扩容时真的取锁，V18）；发布点在 thread zero、在 `start_processes`（第一个轮次）之前
  （H12 的顺序：main-loop-enter 早于 `start_processes`）。它只改目录结构、不与 Data Worker
  共享任何状态，所以**不需要额外的 `WorkerBarrier` 同步**，与 `start_workers` 的先后也无关系。

### D5 采集：一个采集者类型，四条登记

```rust
/// VPP `update_node_counters` 的单元格写（V14）：把自己那个计数器投影到
/// `row = 线程`、`column = 节点槽` 的单元格。
///
/// 一个类型服务四个计数器：`counter` 就是实例自己的字段（VPP 里对应 `node_counters[]`
/// 的下标，V11）。采集不取段锁、不分配、不返回 `Result`（ADR-0021 D9.3）。
struct NodeCounterCollector {
    entry_index: DirectoryIndex,
    counter: NodeCounter,
}

impl Collector for NodeCounterCollector {
    fn entry_index(&self) -> DirectoryIndex { self.entry_index }

    fn collect(&self, entry: &DirectoryEntry) {
        let threads = ThreadMain::global();
        for thread_index in 0..=threads.worker_count() {
            let counters = threads
                .thread_node_counters(thread_index)
                .expect("node counter rows cover thread zero and every Data Worker");
            for (slot, node_counters) in counters.iter().enumerate() {
                entry.set_simple_counter_cell(
                    thread_index,
                    u32::try_from(slot).expect("node slot fits the published column width"),
                    node_counters.value(self.counter),
                );
            }
        }
    }
}
```

- **形状与登记的对应**：四条登记 → 四个条目（每个条目一个计数器、列 = 节点槽）；`rows × columns` 的宽度
  由 D4 发布，采集者只写已发布范围内的格子（越界是采集者自己的 bug，按 ADR-0021 断言语义处理）。
- **轮次成本**：每轮 `4 × (worker_count + 1) × node_count` 次单元格写（默认
  `update_interval = 10s`），与 VPP 同一数量级（V14 写的是同样多的格子，外加每轮的名字 diff）；
  不取锁、不分配、不借 worker 的 `RefCell`——只经描述符上的句柄读那一行原子（D1.3）。

### D6 `suspends`：process 节点的唯一来源（thread zero 行）

- **来源**：Hammer 的 process 节点归 thread zero 的 `NodeMain`（H15），挂起点有两处：
  `process_wait`（定时挂起，VPP `vlib_process_suspend`）与事件等待登记
  （`take_process_events` 把节点放进 `suspended_processes`）。两处各 `add_suspend()` 一次，
  对应 VPP 每次进入挂起的 `p->n_suspends += 1`（V9）。
- **行**：写 `thread index 0` 的那一格（V6：PROCESS 节点的计数只在 main thread 同步），
  写路径与派发点同一条：`main.nodes().node_counters(node).add_suspend()`——thread zero 自己的图
  （D1.1），不经过任何全局查找、不取锁。
- **process 节点的 `calls`/`vectors`/`clocks` 恒为 0**：VPP 在 `vlib_process_resume` 之后用
  `dispatch_suspended_process` 更新这三个计数（V6、V7 的 process 版本）；Hammer 的 process
  节点是 Tokio future（`process.rs` 顶部注释），没有"一次派发"这个点，本 ADR 不为此新增
  插桩面（D8）。**`/sys/node/calls` 对 process 节点读到的 0 是"Hammer 没有该计数"，不是
  "没有派发过"**——客户端契约按 D7 第 6 条写明。

### D7 与 VPP 的有意差异

| 差异 | VPP | Hammer | 理由与回归点 |
| --- | --- | --- | --- |
| 每线程计数器与同步 | 32 位 per-thread 累加 + 溢出时同步成 64 位 `stats_total`（V3–V5） | 每线程每节点四个 64 位 `Relaxed` 原子格，存在该线程的图里（`NodeMain`），无溢出路径、无两级结构（D1.1、D1.4） | 格子本来就是跨线程发布位置（H6 的既有形状）；语义（+1/+=n/+=delta）逐条不变 |
| 采集快照一致性 | 采集不 barrier sync，注释自认计数会不准（V17） | 同样不 barrier；逐格 `Relaxed`，跨格/跨线程不承诺一致快照 | 与 VPP 同一条取舍；若以后需要一致快照，只能用 ADR-0019 D4.6 的第二个选项（轮次前短 barrier）并实测暂停 |
| 名字/别名维护 | 每轮名字 diff，变化时取段锁重建（V12、V13） | 发布一次（main-loop-enter，D4）；节点身份冻结由 `inherit_worker_state` 断言（H3） | Hammer 没有"节点换名/换序"的运行期路径；代价是一条前提条件（§9 未决项 3） |
| 单元格语义 | `stats_total - stats_last_clear`（V14），`clear` 命令重置基线（V26） | 单元格 = 累计值（等于 VPP 从未 clear 的情况）；`clear` 面不存在（`Sys.last_stats_clear` 的注释即此） | 差值需要一个 owner 侧基线；Hammer 的 `clear` 面由 ADR-0019 M4 的另一半负责，本 ADR 不预置 |
| `max_clock`/`max_clock_n` | 采集并只被 `show node` 消费（V1、V25） | 不采集（D8）；若加入，位置是同一行 `NodeCounters` 里的另外两格 | 没有消费者就不发布、也不用零值占位（ADR-0021 的同一原则：不用零值冒充统计） |
| process 节点计数 | calls/vectors/clocks/suspends 都有（V6、V7、V9） | 只有 `suspends`；另外三个恒 0（D6） | Hammer 的 process 节点是 Tokio future，没有派发点；插桩面另立 |
| 采集者形状 | node 计数不是注册表的一行，而是 `do_stat_segment_updates` 里硬编码的第一步（V15） | 四条 `Collector` 登记（每计数器一条），轮次完全由登记表驱动 | 用户明确要求"注册而不是写死"（ADR-0021 D9 的同一决定）；机制的"一行 = 一个条目"因此需要四条行 |
| `clocks` 单位 | 裸 ticks，`show node time` 才用 `seconds_per_clock` 换算（V25、V28） | 裸 ticks，不换算（目录里也没有频率条目，V29） | 没有消费者就没有校准；客户端不得把 `/nodes/*/clocks` 当时间 |

### D8 明确不做

- **不做 `clear` 命令面**：不新增 `/sys/last_stats_clear` 的写入者、不做 owner 侧基线
  （`stats_last_clear`，V26）；`clear` 与它的一次性同步属于另一份设计。
- **不采集 `max_clock`/`max_clock_n`**、不做 `n_vectors_by_next_node` 风格的按 next 计数
  （VPP 有，Hammer 无消费者）、不做 `/sys/vector_rate*`（ADR-0019 M4 的另一部分）。
- **不给 process 节点插桩 calls/vectors/clocks**（D6）。
- **不在轮次里做名字 diff、不取段锁、不分配**（D5）；不在采集者里访问 worker 的图
  （`NodeRuntimeInner`/`NodeRuntime`）或借用别的 worker 的状态。
- **不引入第二套 per-thread 容器**：计数器行就在该线程的图里（`NodeMain`，D1.1），描述符上
  只有一个句柄；不新增"StatsWorkerSnapshot"/"StatsWorkerView"之类中间对象，也不把行指针缓存在别处。
- **不动 `hammer-stats` 的机制 surface**（除 ADR-0021 已批准的那几项）：不加 node 家族字段/
  方法，不加第二条登记通道，不在 `StatsSegment`/`StatsMain` 上出现 `Node*` 类型。
- **不给 `hammer-infra` 加 TSC 校准/频率校验**（D2）。
- **不复活旧 ADR-0022 的进程级 node 计数块、`publish_*` 系列步骤或 `MainLoopRate`**。

### D9 stats client 集成契约

客户端实现属于外部 `netsystem-client` 仓库；分层（`StatsClient` / `StatsProvider` / 使用方）与
`StatsReader`、`StatsProvider`、`StatsClient::report::<P>()` 三个名字、语义都与 ADR-0021 §6.4
完全相同，本仓不新增客户端类型、不改协议 crate。本节只写本家族的读取规则与形状校验，
候选签名见 6.5。

**客户端要面对的两个运行期事实：**

- **家族可能整体不存在**：`per_node_counters = false` 时目录里没有 `/sys/node/*`，也没有
  `/nodes/*`（D0、V16）。这不是畸形映射，也不是错误——report 的
  `type Report = Option<NodeStats>`，`None` 就是"这个进程没开 node 计数"；只有"条目存在但形状
  不对"才走类型化错误。
- **形状是运行期事实**：列数 = 节点槽数、行数 = 线程数（thread zero + Data Worker），两者都不在
  声明里，只能从 `names` 向量长度与计数向量的行数读出（D0、V13、V20）。

**服务端契约（客户端可以依赖的事实）：**

| 名字 | 类型 | 形状 | 语义与读取规则 |
| --- | --- | --- | --- |
| `/sys/node/names` | `NameVector`（=4） | 元素 = 节点槽 | `names[slot]` = 节点名；**它同时是列数的权威** |
| `/sys/node/{clocks,vectors,calls,suspends}` | `CounterVectorSimple`（=2） | 行 = 线程，列 = 节点槽 | 累计量（V1、V14）；四个条目必须行数相同，且每行列数 == `names.len()` |
| `/nodes/<name>/<counter>` | `Symlink`（=6） | 指向对应计数条目的第 `<slot>` 列 | 该节点跨线程的值；解引用语义（目标数据 + 列裁剪）与 ADR-0021 D7 规则 1 相同（V19、V22） |

**读取规则：**

1. **一次 report = 五个基条目各读一次**（`names` + 四个计数向量）。不逐节点读 `4N` 条
   `/nodes/*` symlink：symlink 与表是同一份数据的两个名字，逐条读既慢又会得到跨轮次混合值
   （V22 的 `copy_data` 是从表里裁列）。
2. **形状校验**：四个计数向量行数必须相等；每行的列数必须等于 `names.len()`；行数还必须与
   `/sys/num_worker_threads + 1` 一致（thread zero + Data Worker，ADR-0021 对
   `main_loop_count_per_worker` 有同类校验）。任一不满足 → 客户端仓的类型化错误（条目名、期望与
   实际行列数），不 panic、不静默裁剪、不按短的列数截断。
3. **列 → 节点名**：`NodeStats::node(name)` 在 `names` 里定位槽号，再取四个向量的该列；
   `names` 里重名（服务端 bug）必须按"没有唯一槽号"处理（`None` 或类型化错误），不得任取一列。
4. **单位**：`clocks` 是裸 CPU 计数器（ticks），不是秒也不是毫秒；目录里没有换算系数
   （V29、V25），所以客户端不得把 `clocks` 换算成时间、不得与 `Instant` 混算，只能做同机差值。
5. **不构成快照**：五个基条目不是同一瞬间的，服务端刻意不做 barrier 同步（V17、D7 第 2 条）；
   `suspends` 只有 thread zero 行可能非零（D6）；process 节点的 `calls`/`vectors`/`clocks`
   按 D6/D7 恒 0，客户端不得据此推断该节点没在运行。
6. **累计量的使用**：`clocks`/`vectors`/`calls` 单调不减，可以从两份 report 派生速率或区间量；
   provider 不保存基线、不在 report 里发布速率（服务端也没发布速率条目，V15、D8）——需要速率的
   调用方按 ADR-0021 D5 的同一条规则对两份 report 做纯函数派生。
7. `MetricValue` 不需要新变体（name vector / simple counter vector / symlink 都已存在），
   `hammer-stats-protocol` 不新增类型、`STAT_SEGMENT_VERSION` 不变。

**客户端明确不做的事：**不缓存跨 epoch 的 `DirectoryIndex`、不在 `StatsClient` 上加
`node_stats`/`node_calls` 之类的家族方法、不引入 provider 注册表或 `dyn` 查找、不重建形状、
不把 `/nodes/<name>/*` 当第二条数据源、不对 `clocks` 做时间换算、不假设开关状态（先看
`names()` 里有没有 `/sys/node/names`，没有即 `Ok(None)`）。

**互操作证据：**本仓提供只读映射 fixture（6.5）：`crates/hammer/tests/stats_segment_mapping.rs`
增加 `/sys/node/{names,clocks,vectors,calls,suspends}` 的类型/形状样例与 `/nodes/<name>/<counter>`
的 symlink 解析样例（`Symlink` 条目数据 = 目标目录下标 + 列号，与 `add_symlink` 的
`(target, column)` 一致）；客户端仓对同一 fixture 跑自己的 provider，两份 report 必须相同。

## 5. 分层隔离契约

| 层 | 可以调用/持有 | 不可以调用/持有 | 验证边界 |
| --- | --- | --- | --- |
| hammer-infra | 既有内存/分配原语之外，新增 `time::cpu_time_now`（裸计数器读） | stats 类型/路径、runtime 概念、`NodeCounters`、任何"ticks → 秒"换算 | 连续两次读的差值与 TSC 语义一致（同线程单调、可作为差值）；不分配、不取锁 |
| hammer-core | `NodeId::slot`/`NodeId::new`、图/帧/缓冲原语（既有） | stats 类型、`NodeCounters`、目录名 | 本文不新增 `hammer-core` 类型（H4） |
| hammer-stats（机制） | 既有目录原语（`validate`/`set_name`/`add_symlink`/条目级写）、采集者表与轮次 | `/sys/node` 名字、`NodeCounter`、`NodeCounters`、`ThreadMain`、任何节点/线程概念 | 机制 crate 在没有 node 声明/采集者时独立编译；`StatsSegment`/`StatsMain` 公共签名不出现 `Node*`；`collect()` 的实现里不出现 node 家族 |
| hammer-runtime（node 家族 owner，`node_stats.rs`） | `NodeStats` 声明、`NodeCounter`、`NodeCounters` 的读写、`NodeCounterCollector`、两条登记、`ThreadMain` 的只读访问、图事实（`node_count`/`node_name`） | 往机制加 node 字段/方法、第二条登记通道、在采集里取段锁、在采集里访问图内部（`NodeRuntimeInner`/`NodeRuntime`）、替 worker 拥有计数 | 五条条目与声明一一对应；四条登记与四个计数器一一对应；轮次只写自己条目的格子；发布点只出现一次 `validate`/`set_name`/`add_symlink` |
| `NodeMain`（图侧，计数器行的家） | `node_counters: Arc<[NodeCounters]>`（槽 = 节点）、`install_node_counters`、`node_counters(node)` | 解释计数器的业务含义、跨线程读、轮次语义、替别的线程写格子 | 行随图在发射前建好、长度 = 冻结的节点容量；worker 只写自己图里的行；refork 不换行（H3） |
| `WorkerThread`/`ThreadMain`（线程域） | 该线程计数器行的**句柄**（`install_node_counters`/`node_counters`、`thread_node_counters`） | 计数器存储（存储在图里）、解释计数器含义、写别的线程的格子 | 句柄在发射前安装、容量与 `with_node_capacity` 同一份；没有图的 runtime 线程返回 `None`；轮次不借 worker 的 `RefCell`、不取锁 |
| `statseg-collector-process`（既有 node） | 每轮一次 `StatsMain::collect()`、`update_interval`、sleep（H8、H12） | node 家族步骤、节点名/槽号/计数器、第二条 process node | node 的代码里不出现 node stats 概念；轮次的全部 node 工作来自登记表 |
| 外部 stats client | `StatsClient` 的只读映射与 `names()`/`read()`；`NodeStatsProvider`（`PREFIX = "/sys/node"`）把五个基条目投影成 `Option<NodeStats>`，`NodeStats::node(name)` 在客户端本地按 `names` 取列 | 家族方法进 `StatsClient`、把 `/nodes/*` 当第二条数据源、把 `/nodes/*/clocks` 当时间、假设开关打开时条目一定存在、把值当同一瞬间快照、provider 持有基线 | 只读映射、名字/类型/别名解码、开关两种形态都能读（关闭 → `Ok(None)`）、行列数与 `num_worker_threads` 不符时给类型化错误 |

边界检查以编译、真实 lifecycle 与可观察行为为主；`rg` 只用于迁移删除清单，不能替代行为证明。
本设计不新增跨层 wrapper：`NodeCounters` 是线程域拥有的四个格子的结构（不是"视图"或缓存指针），
`NodeCounter` 是"哪个计数器"的领域选择（V11 的下标），`NodeCounterCollector` 是采集者本身。

## 6. 新增/修改 API 审批清单

### 6.1 hammer-infra（A1）

| 编号 | 项 | 动作 | 说明 |
| --- | --- | --- | --- |
| A1 | `pub mod time` + `pub fn cpu_time_now() -> u64` | 新增（**需批准**） | D2 的唯一实现；x86_64 走 std 稳定 intrinsic `core::arch::x86_64::_rdtsc`、aarch64 走 `core::arch::asm!`（std 无 intrinsic）、未支持架构 `compile_error!`，不使用第三方 crate（对照表见 D2）。理由：VPP 对应物在 `vppinfra/time.h`（V28），Hammer 的 infra 层今天没有任何时间戳原语（H17），而 runtime 不应该自带 asm |

### 6.2 hammer-component-macros（A2、A3）

| 编号 | 项 | 动作 | 说明 |
| --- | --- | --- | --- |
| A2 | `#[derive(Stats)]` 支持 `NameVector` 字段：install 建空名字向量（长度 0），元素由 `set_name` 增长 | 新增（**需批准**） | VPP 的 `vlib_stats_add_string_vector` 也是先建空向量、由 `set_string_vector` 增长（V20）；`/sys/node/names` 的元素数就是节点数（图事实） |
| A3 | 计数向量声明的 `columns` 可省略：省略时 install 只建条目，不 `validate` | 新增（**需批准**） | 形状是图事实，登记时图还没物化（H12）；VPP 的形状也在采集点建立（V13、V18）。ADR-0021 的 `columns` 语义（install 期 `validate`）对 `/mem`、`/sys` 保持不变 |

### 6.3 hammer-runtime：计数器行与更新点（A4–A10）

| 编号 | 文件 | 类型 / 方法 / 字段 | 动作 |
| --- | --- | --- | --- |
| A4 | `crates/hammer-runtime/src/node_stats.rs` | `NodeCounters`（`calls`/`vectors`/`clocks`/`suspends` 四个 `AtomicU64`）+ `row(node_capacity: usize) -> Arc<[NodeCounters]>` + `update_dispatch(&self, vectors: u64, clocks: u64)` + `add_suspend(&self)` + `value(&self, NodeCounter) -> u64` | 新增（**需批准**） |
| A5 | `crates/hammer-runtime/src/worker_thread.rs` | `WorkerThread.node_counters: OnceLock<Arc<[NodeCounters]>>`（**只是句柄，存储在图里**）、`install_node_counters(&self, row: Arc<[NodeCounters]>)`、`node_counters(&self) -> Option<&[NodeCounters]>` | 新增（**需批准**） |
| A6 | `crates/hammer-runtime/src/thread_main.rs` | `thread_node_counters(&self, thread_index: u32) -> Option<&[NodeCounters]>` | 新增（**需批准**） |
| A7 | `crates/hammer-runtime/src/start_workers.rs:20-24` | 在 `with_node_capacity` 的同一处用同一份 `main.nodes().node_count()`：建 thread zero 的行与每个 worker 的行，装进各自的图（A15）与 worker 描述符（A5） | 修改 |
| A8 | `crates/hammer-runtime/src/data_plane/main.rs:40-60` | `pub(crate) last_time_stamp: u64` 字段 + 两个构造点初始化（`new_main`/`new_worker`） | 修改 |
| A9 | `crates/hammer-runtime/src/main_loop.rs:122-130` 一带、`crates/hammer-runtime/src/node.rs:2190-2205` | 主循环派发前刷新 `last_time_stamp`；`dispatch_node` 收尾读一次 `cpu_time_now` 并写三个计数 | 修改 |
| A10 | `crates/hammer-runtime/src/process.rs:97-127,173-190` | `process_wait` 与 `take_process_events` 的挂起点各调一次 `add_suspend()`（thread zero 行，写 thread zero 图里的那格） | 修改 |
| A15 | `crates/hammer-runtime/src/node.rs` | `NodeMain.node_counters: Arc<[NodeCounters]>`、`install_node_counters(&mut self, row)`、`node_counters(&self, node: NodeId) -> &NodeCounters`（`Clone`/`From<NodeRuntimeInner>` 只给空行，真行由 A7 装） | 新增（**需批准**） |

### 6.4 hammer-runtime：node 家族模块与登记（A11、A12）

| 编号 | 文件 | 项 | 动作 |
| --- | --- | --- | --- |
| A11 | `crates/hammer-runtime/src/node_stats.rs`（新模块） | `NodeStats` 声明、`NodeCounter` 枚举、`NodeCounterCollector`、`register_node_stats`（`#[stats_collect_registration]`）、`install_node_stats`（`#[main_loop_enter_function]`） | 新增（**需批准**） |
| A12 | `crates/hammer-runtime/src/lib.rs:14-41` | image 增加 `node_stats::__STATS_COLLECT_REGISTRATION_REGISTER_NODE_STATS` 与 `node_stats::__INIT_FN_INSTALL_NODE_STATS`；`mod node_stats;` | 修改 |

### 6.5 client 与 fixture（A13、A14，跨仓）

分层与读取规则见 D9；本仓不新增客户端类型，也不新增协议类型。以下是跨仓契约的候选签名：
由 `netsystem-client` 仓批准与实现。`StatsReader`、`StatsProvider`、`StatsClient::report::<P>()`
与 ADR-0021 §6.4 完全同名同义，本家族只加一个 provider、一个 report 与一个列选择枚举。

```rust
// hammer-stats-client（外部仓的 Rust binding）
// StatsReader / StatsProvider / StatsClient::report::<P>() 来自 ADR-0021 6.4，本家族不重复定义。

pub struct NodeStatsProvider;

/// 四个计数器中的一个（目录里的名字，D0、D5）。
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NodeCounter {
    Clocks,
    Vectors,
    Calls,
    Suspends,
}

/// 一个节点在四个计数器上的每线程值（长度 = 线程数，行序 = 线程）。
pub struct NodeUsage {
    pub name: String,
    pub clocks: Vec<u64>,
    pub vectors: Vec<u64>,
    pub calls: Vec<u64>,
    pub suspends: Vec<u64>,
}

/// `/sys/node/*` 一次读到的整张表：行 = 线程，列 = 节点槽。
pub struct NodeStats {
    pub names: Vec<String>,
    pub clocks: Vec<Vec<u64>>,
    pub vectors: Vec<Vec<u64>>,
    pub calls: Vec<Vec<u64>>,
    pub suspends: Vec<Vec<u64>>,
    /// 本 report 的产生时刻（客户端本地单调时钟）；累计列由调用方按需差分。
    pub sampled_at: Instant,
}

impl NodeStats {
    /// 行数 = 线程数（thread zero + Data Worker）。
    pub fn thread_count(&self) -> usize;
    /// 按节点名取一列；名字不在 `names` 里或重名时返回 `None`（D9 规则 3）。
    pub fn node(&self, name: &str) -> Option<NodeUsage>;
    pub fn counter(&self, counter: NodeCounter) -> &[Vec<u64>];
}

impl StatsProvider for NodeStatsProvider {
    /// 开关关闭时整个家族不存在（D0）；`None` 是普通结果，形状错误才是 `Err`。
    type Report = Option<NodeStats>;
    const PREFIX: &'static str = "/sys/node";
    fn report<R: StatsReader>(reader: &R) -> Result<Option<NodeStats>, Error>;
}
```

**客户端仓的验收边界：**

- `StatsClient` 不加任何家族方法；`/nodes/<name>/<counter>` 不是第二条数据源：typed report 只由
  `/sys/node/*` 五个基条目投影，`node(name)` 是本地取列，不再读段。
- provider 不推断图事实：不判断节点是 function 还是 process、不假设 thread zero 之外
  `suspends` 有值、不假设 process 节点的 `calls`/`vectors`/`clocks` 非零（D6、D7）。
- 形状不符（四个向量行数不一致、列数 ≠ `names.len()`、行数 ≠
  `/sys/num_worker_threads + 1`、类型不是预期的 `NameVector`/`CounterVectorSimple`）→
  客户端仓的类型化错误（条目名 + 期望/实际行列数），不 panic、不裁剪。
- `names()` 里没有 `/sys/node/names`（开关关闭）→ `Ok(None)`；provider 不假设其它 `/sys/*`
  条目（`num_worker_threads` 除外）存在，也不因此报错。
- 新增家族只加 provider 与 report，不改 `StatsClient`、不改协议 crate。

| 编号 | 位置 | 动作 | 说明 |
| --- | --- | --- | --- |
| A13 | `netsystem-client`（外部仓） | 新增 `/sys/node/*` 与 `/nodes/<name>/*` provider | 只读；`StatsClient`/协议/`MetricValue` 不变（ADR-0021 D7 的同一契约）；provider 依赖 `StatsReader` 能力 trait，用泛型/静态分发 |
| A14 | `crates/hammer/tests/stats_segment_mapping.rs` | 修改（新增样例） | 本仓只提供固定样例：`/sys/node/{names,clocks,vectors,calls,suspends}` 的类型与形状、`/nodes/<name>/<counter>` 是 `Symlink` 且指向第 `<slot>` 列。fixture 今天只解 `TYPE_SCALAR`/`TYPE_SIMPLE`/`TYPE_SYMLINK`/`TYPE_GAUGE`，需要新增 `TYPE_NAME_VECTOR = 4` 的字符串向量解码分支（元素 = 指向 C 字符串的指针，形状与 `Names::names` 的读取一致）；跨仓契约在客户端仓自己验证 |

### 6.6 全部动作总表

| 文件 | 类型 / 方法 / 字段 | 动作 |
| --- | --- | --- |
| `crates/hammer-infra/src/time.rs`、`lib.rs` | `cpu_time_now`、`pub mod time`（A1） | 新增 |
| `crates/hammer-component-macros/src/lib.rs` | `NameVector` 字段声明、`columns` 可省略（A2、A3） | 新增 |
| `crates/hammer-runtime/src/node_stats.rs` | `NodeCounters`、`NodeCounter`、`NodeStats`、`NodeCounterCollector`、两条登记（A4、A11） | 新增 |
| `crates/hammer-runtime/src/node.rs` | `NodeMain.node_counters` 行与两个访问器（A15） | 新增 |
| `crates/hammer-runtime/src/worker_thread.rs` | 计数器行**句柄**字段与两个访问器（A5） | 修改 |
| `crates/hammer-runtime/src/thread_main.rs` | 计数器行只读借出（A6） | 修改 |
| `crates/hammer-runtime/src/start_workers.rs` | 建行与安装点（A7） | 修改 |
| `crates/hammer-runtime/src/data_plane/main.rs` | `last_time_stamp` 字段与构造初始化（A8） | 修改 |
| `crates/hammer-runtime/src/main_loop.rs`、`node.rs` | 主循环刷新与派发点写三个计数（A9） | 修改 |
| `crates/hammer-runtime/src/process.rs` | 两处挂起计数（A10） | 修改 |
| `crates/hammer-runtime/src/lib.rs` | image 两条、`mod node_stats;`（A12） | 修改 |
| `crates/hammer/tests/stats_segment_mapping.rs` | `/sys/node/*` 与 `/nodes/*` 样例（A14） | 修改 |
| `netsystem-client`（外部仓） | `NodeStatsProvider`、`NodeStats`、`NodeUsage`、`NodeCounter`（A13，候选签名见 6.5） | 新增 |

## 7. 迁移与执行顺序

| 阶段 | 范围 | 完成证据 |
| --- | --- | --- |
| M0 hammer-infra | A1 | 连续两次 `cpu_time_now()` 的差值单调、可作为 `clocks` 的差；不分配、不取锁、`cfg` 未支持架构时编译失败 |
| M1 计数器行 | A4–A7、A15 | 两个 worker + thread zero 各自的图里有一行按冻结容量建好的计数器；行长度 = `with_node_capacity` 的 `node_count`；发射前装好、之后不再增长；worker 只写自己图里的行；描述符上只有句柄、没有存储 |
| M2 更新点 | A8、A9 | 固定次数派发后 `calls` = 派发次数、`vectors` = 包数之和；同一派发点前后手算 `cpu_time_now` 差值等于写进去的 `clocks`；两个 worker 的行互不影响 |
| M3 声明/登记/发布/采集 | A2、A3、A10、A11、A12 | 开关打开：五条 `/sys/node/*` + `4 × 节点数` 条 `/nodes/*`（类型与列号正确）、一轮后值与每线程计数器一致、thread 0 行只有 `suspends`；开关关闭：目录里完全没有这些条目；`statseg-collector-process` 的代码不变 |
| M4（跨仓） | A13、A14 | 客户端对 fixture 与真实映射得到相同 `NodeStats`（名字、行列、四个计数器、`node(name)` 投影）；开关关闭时两处都得到 `Ok(None)`；形状不符给类型化错误；`StatsClient` 公共面不出现家族方法 |

**前置顺序（硬约束）：**M3 依赖 ADR-0021 的机制（`Collector` trait、`register_collector`、
无锁轮次、条目级写、D10 段锁）与它的 M1/M2；A2/A3 依赖 `#[derive(Stats)]` 的
`path` 表达式能力（ADR-0021 A12）。ADR-0021 未落地前，本文只作为设计存在；
实施顺序：ADR-0021 → M0 → M1 → M2 → M3 → M4。

## 8. 验证矩阵

| 范围 | 触发 | 期望 |
| --- | --- | --- |
| 契约（M3） | 开关打开，2 worker、N 节点启动 | `/sys/node/names` 是 `NameVector` 且按槽号给出名字；四个计数向量是 `CounterVectorSimple`、行 = 3、列 = N；`4N` 条 `/nodes/<name>/<counter>` 是 `Symlink` 且解析到对应条目的第 `<slot>` 列 |
| 开关（M3） | `per_node_counters = false` 启动 | 目录里没有 `/sys/node/*` 与 `/nodes/*`；轮次不做 node 工作；每线程计数器行仍建好、派发仍累加（与 VPP 无条件更新每线程计数一致，V4、V16） |
| 计数正确（M2） | 在一个 worker 上派发固定帧 | `calls` = 派发次数、`vectors` = 帧包数之和、`clocks` = 同点手算的 ticks 差；`suspends` 不受影响 |
| 并发（M2） | worker 持续派发，主线程同时跑一轮 | 无数据竞争（`Relaxed` 原子）、值单调不减、采集不借 worker 的图或 `RefCell` |
| suspends（M3） | 一个 process 节点挂起两次（定时 + 事件） | thread 0 行的 `suspends` = 2，其它行 = 0；该 process 节点的 `calls`/`vectors`/`clocks` = 0（D6、D7） |
| 发布时刻（M3） | 启动序列 | 形状/名字/别名在第一个轮次之前发布；发布点之后不再出现 `validate`/`set_name`/`add_symlink`（代码层面只有一处） |
| 结构变更的线程纪律（M3） | 发布点运行在哪条线程 | 发布在 thread zero（main-loop-enter），轮次在 thread zero；worker 从不碰目录结构 |
| 隔离（M1–M3） | `cargo check -p hammer-stats`；检查公共签名 | 机制 crate 在没有 node 声明/采集者时独立编译；`StatsSegment`/`StatsMain` 不出现 `Node*`；`collect()` 与 `statseg-collector-process` 里不出现节点名/槽号/家族调用 |
| 热路径（M2） | release/LTO 下派发基线与汇编 | 三次 `Relaxed` RMW 可解释、不造成可测回归；若可测，降级方案需单独批准，不默认加开关 |
| 客户端读取（跨进程，M4） | 独立进程只读映射：`names()`、`/sys/node/*`、`/nodes/*` | 名字/类型/别名/列号正确；`names` 长度 = 列数、行数 = `num_worker_threads + 1`；开关关闭时不假设条目存在；不把 `clocks` 当时间 |
| 客户端 provider（M4） | fixture 与真实映射各跑一遍 `NodeStatsProvider`；再跑一遍开关关闭的映射 | 两份 `NodeStats` 相同；关闭时两处都是 `Ok(None)`；`node(name)` 与 `/nodes/<name>/<counter>` 读到同一列；列数/行数不符时给类型化错误 |
| 布局 | 编译期 size/align 断言与真实映射 fixture | `STAT_SEGMENT_VERSION` 不变；`WorkerThread` 的既有 cacheline offset 断言仍成立（新增字段只放句柄）；计数器行按节点容量一次分配、长度与冻结点一致 |

测试只在最终 pre-commit gate 执行，遵循仓库测试时机规则；不做源文本断言。
本轮没有执行任何测试、fixture 或性能测量。

## 9. 审查结果与未决项

**已核实：**VPP 的 node stats 是"每线程计数（`vlib_node_runtime_t`，V3）→ 64 位节点总量
（`vlib_node_stats_t`，V1、V5）→ 采集点投影成目录单元格（V14）"三级，目录契约是五个固定条目
加每节点四条别名（V10–V13、V16、V19），`suspends` 只属于 process 节点（V6、V9），
`clocks` 是裸 ticks（V25、V28），采集刻意不做 barrier 同步（V17）。Hammer 今天没有任何 node
计数、名字向量或别名（H8、H18），派发点只有一处（H2），节点身份在 worker 启动前冻结（H3、H5），
机制原语齐备（H10）。本设计因此把"每线程计数器"落在 `WorkerThread`（与 VPP 的每线程位置同构、
与 `main_loop_count` 的既有形状同形，H6），把"形状/名字/别名"压成一次发布（H3 的冻结事实），
把"投影"写成四条采集者登记（注册而非写死），把 `cpu_time_now` 放进 infra（V28、H17）。

**未决项（实施前需要确认或另行批准）：**

1. **A2/A3 两项宏 surface**：`NameVector` 字段声明与"`columns` 可省略"。备选方案是"声明带
   `columns = <表达式>`、由 install 期 `validate`"——在 H12 的时序下不成立（登记时图还没物化），
   所以要么批准 A2/A3，要么把声明也推迟到 main-loop-enter（那会要求
   `register_collector` 在 `StatsMain` 发布之后仍可调用，与 ADR-0021 D9.1 冲突，需要先改机制决定）。
2. **计数器行的家与句柄**：本文选"行在该线程的图里（`NodeMain`，VPP `vlib_node_runtime_t` 的位置），
   线程描述符上只放指向它的句柄（VPP `vlib_worker_thread_t.vlib_main`）"。备选一：存储直接放描述符
   （前一版的 `OnceLock<Box<[NodeCounters]>>`）——离 VPP 更远，且把"节点自己的计数"写成"线程的数组"；
   备选二：所有线程的图共享一个扁平 `Arc<[NodeCounters]>`（`thread_index × capacity + slot`）——语义相同、
   少一层"每线程一行"的所有权，但仍然要有东西把这个句柄交给轮次（描述符或新全局），并没有更简单。
   三者取一需要在 A5/A6/A15 的审批里确认。
3. **前提条件（必须写进代码注释与验收）**：如果以后出现"运行期增删 Data Worker"
   （`register_thread`/`num_workers_change_functions` 面，H7）或"运行期重编号节点"
   （`rebuild_graph*` 今天只有测试调用者，H3），形状/名字/别名必须重新发布——VPP 每轮 diff 正是
   为此（V12、V13）；那时需要一次 `WorkerBarrier` 停住 worker 再改目录（ADR-0019 §9 第 10 条）。
4. **一致快照**：本文与 VPP 一样不做 barrier（V17），因此 `/sys/node/calls` 与 `/sys/node/vectors`
   不保证同一瞬间。D9 规则 5 已把这一条写成客户端契约（provider 不得声称拿到同一瞬间的值）；
   如果客户端将来要求一致快照，只能走 ADR-0019 D4.6 的第二个选项（轮次前短 `WorkerBarrier`）
   并给出实测暂停，不能靠"看起来对齐"通过。
5. **process 节点的 `calls`/`vectors`/`clocks`**：本文按 D6/D7 恒 0。如果需要，插桩点是 thread zero
   的 process 恢复点（VPP `dispatch_suspended_process`，V6、V7），属于另一份设计的范围。
6. **`clocks` 的换算**：需要一次 VPP `clib_time_init` 风格的校准与一个可读的换算系数条目
   （V28、V29 今天都不存在）。在它落地之前，`/nodes/*/clocks` 只承诺"同机、单调、可微分"。
7. **`clear` 面**：per-node 基线（VPP `stats_last_clear`，V26）与 `/sys/last_stats_clear` 的写入者
   不在本文范围；`Sys.last_stats_clear` 的注释已经记录了"owner clear baseline 还不存在"。
8. **客户端 provider 的落地节奏**：本 ADR 已固定名称与语义（`NodeStatsProvider`、`NodeStats`/
   `NodeUsage`/`NodeCounter`、D9 的读取规则与 6.5 的候选签名），实现与发布顺序需要与
   `netsystem-client` 仓协调。客户端仓可以按自己的风格改名，但"底层不认识家族、provider 只经
   `StatsReader`、编译期单态化、关闭开关 → `Ok(None)`"四条边界不随名字变化；D9 规则 4 的
   "`clocks` 不做时间换算"在换算系数条目落地（未决项 6）之前不得放宽。

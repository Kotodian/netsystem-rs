# ADR-0022: Buffer Pool Stats 的注册与采集（`/buffer-pools`）

Status: proposed — 设计草案，尚未批准实现（2026-09-17：取代被删除的旧 ADR-0022；本文只覆盖
buffer stats——用 `#[derive(Stats)]` 注册、由 collector 采集；不含 node runtime stats）
Date: 2026-09-17

> **前置依赖：本文按 ADR-0021 第四版的机制写。**采集者表是进程私有的
> `Vec<Box<dyn Collector>>`（`Collector { entry_index(&self); collect(&self, &DirectoryEntry) }`）、
> 登记是 `StatsMain::register_collector(&mut self, impl Collector + 'static)`、轮次
> `StatsMain::collect(&self)` 不取锁（`statseg-collector-process` 只调用它）、值写走条目级
> `DirectoryEntry::set_scalar`、声明走 `#[derive(Stats)]` 的 `path = <表达式>`、owner 侧登记走
> `#[stats_collect_registration]`。这些在 ADR-0021 里仍是 proposed（尚未实现）；
> **ADR-0021 的 M1/M2 未落地前不得实施本文的 M1–M3**。本文不重复论证 ADR-0021 已给出的
> 机制决定，只在需要**新增** surface 的地方标注"需批准"。
>
> **旧 ADR-0022 已删除。**旧文把 buffer pool 与 node runtime 两个家族写在一起，并用
> 自由函数 `register_buffer_pool_*`/`publish_*` 命令式创建条目、把"更新"塞进声明宏属性、
> 自造进程级 node 计数块；这些都不在本文复活。本文只写 buffer pool 家族。

## 1. 范围与结论

**用户要求：**阅读 vendored VPP 与现有 stats 代码，设计 buffer stats：条目用
`#[derive(Stats)]` 注册、值由 collector 采集；先出设计，不写代码，不执行测试，不创建
issue，不启动 daemon。

**已核实的项目事实：**

- **VPP 的 buffer pool stats 就是三个 gauge**：`/buffer-pools/<pool>/{cached,used,available}`
  （`buffer.c:944,948,953`），池名 `default-numa-<n>`（`buffer.c:785`），每个池**三条登记**
  （建 gauge + `vlib_stats_register_collector_fn`），`private_data` = 池下标
  （`buffer.c:937-956`，V7）。三个采集函数各自写一条 gauge（`buffer.c:838-872`，V5）。
- **VPP 的三个采样事实就是 Hammer 池已有的三个事实**：`n_buffers` ↔ `BufferPool.buffer_count`、
  `n_avail` ↔ `BufferPool.free` 的长度、`Σ n_cached` ↔ 各 worker 缓存长度之和（V2、V3、H1）。
  Hammer 的池**没有名字字段**（VPP `bp->name`，V8），也**没有任何 stats 面**（H14）。
- **VPP 的 `n_cached` 是池拥有的 cache-line 槽里的长度**（`vlib_buffer_pool_thread_t.n_cached`，
  `buffer.h:437-444`，V1），由拥有者线程在分配/归还快路径上直接写（`buffer_funcs.h:583-614,739-757`，
  V10、V11），采集方在池锁下求和（`buffer_get_cached`，`buffer.c:811-825`，V4）。Hammer 今天的
  对应物是 `BufferPool.workers[thread].len`：它**是池拥有的槽**（与 VPP 同构），但在 Rust 里是
  `RefCell<BufferThreadCache>`，只有绑定该槽的 worker 能借（H1、H3）——轮次连 `borrow()`
  都会 panic，所以长度必须成为池上、与 `RefCell` 并列、可共享读的那个计数（D3）。
- **Hammer 的池集合在启动期一次冻结**（`BufferMain::init`：`BUFFER_MAIN.set` 只成功一次，H4），
  空池直接以 `DataPlaneError::BufferPoolsUnavailable` 拒绝启动（`main.rs:94,210`），池只增不减；
  worker 数在 `BufferMain::init` 时决定 cache 槽数（`worker_count + 1`，`main.rs:137,229`）。
- **登记期 `BufferMain::global()` 已经发布**：`DataPlaneMain::new_main(&threads)`（建 BufferMain）
  在 `main_loop::run` 之前（`crates/hammer/src/main.rs:168-175`），而 stats 登记发生在
  `main_loop::run` → `run_init_functions` → `init_stats_main` → `run_stats_registrations()`
  （H12）。所以 owner 的登记项可以直接遍历池集合，与 VPP 在 `vlib_buffer_main_init` 里的
  `vec_foreach (bp, bm->buffer_pools)`（V7）同一个时机。
- **依赖方向允许池的采样类型落在 `hammer-core`**：`hammer-stats` 依赖 `hammer-core`
  （`Cargo.toml`），反向不行（H7）。所以"一次池采样"的拥有者是 `hammer-core`（池的 owner），
  `/buffer-pools` 的目录契约与采集者是 `hammer-stats` + `hammer-runtime`。
- **今天没有任何 `/buffer-pools` 条目、声明、采集者或 provider**（`rg '/buffer-pools' crates/`
  无命中，H14）；现有 stats 段只有 `/sys` 三个固定槽、五个 `/mem` 堆条目（`main heap`、`stat segment`
  与 api segment 的三个 region 堆）与两个 per-worker `/sys` 向量（H10）。

**结论（拟议，均需批准）：**

- **D0 家族契约：**`/buffer-pools/<pool-name>/{cached,used,available}` 三个 `Gauge`，
  池名 `default-numa-<numa-node>`，一个池三条登记——名字、类型（`DirectoryType::Gauge`，
  VPP `STAT_DIR_TYPE_GAUGE`）、取值公式与 VPP 逐条对应（V5、V7、V15）。不发布 `total`
  条目（VPP 目录里也没有，V16）。
- **D1 声明：**池的三个 gauge 由 owner 模块里**一条 `#[derive(Stats)]` 声明**
  （`BufferPoolGauges`，三个 `Gauge` 字段）描述；因为池数是运行期事实，这条声明是**参数化声明**
  ——`path` 里用 `{pool_name}` 占位符，宏生成 `install(&StatsSegment, pool_name: &str)`，
  owner 的登记项**每池 install 一次**（对应 VPP 的登记循环）。这是本文唯一的新宏 surface
  （A6，需批准；替代方案见 D1.3）。
- **D2 采样：**`hammer-core` 提供一次只读采样 `BufferPoolUsage { buffer_count, available, cached }`
  与三个窄访问器（`pool_count`/`pool_name`/`pool_usage`）；不在 `hammer-core` 里出现任何
  stats 类型、路径或列序（H7）。
- **D3 缓存长度的发布：**长度搬到池上（`BufferPool.cached_counts: Box<[AtomicUsize]>`，一个
  runtime thread 一个槽，就是 VPP `bp->threads[ti].n_cached` 的位置），`BufferThreadCache`
  不再有 `len` 字段；拥有者线程在既有四个写点用 `Relaxed` 写自己的槽、轮次用 `Relaxed` 求和
  （VPP 的 `n_cached` 是普通 `u32`，靠池锁求和；Hammer 的槽本身可共享读，理由见 D3）。
  长度只存一份，没有影子字段。
- **D4 采集：**一个采集者类型 `BufferPoolGaugeCollector`（字段 = VPP 的 `entry_index` +
  `private_data` + 该 gauge 的选择器），每池三个实例、三条登记；轮次里只做"取该 gauge 条目 →
  写一个值"（V5、V14）。
- **D5 更新映射：**`hammer-stats::buffer_pools`（家族模块）持有三个事实的名字与取值映射，
  `update_pool_gauge(&DirectoryEntry, BufferPoolGauge, BufferPoolUsage)` 是 VPP 三个
  `buffer_gauges_collect_*_fn` 里"写 `d->entry->value`"那一句的唯一落点；机制
  （`StatsSegment`/`StatsMain`）与 node 都不认识 `/buffer-pools`。
- **D6 失败与顺序：**登记失败即启动 `Err`（无轮次、无 worker、没有半家族），不写逐池回滚；
  登记项列在 `Sys` 之后、`run_stats_registrations()` 之内（H11、H12）。
- **D7 client：**客户端分层与 ADR-0021 D7 完全相同：`StatsClient` 只做连接/只读映射/`names()`/
  `read()`，`/buffer-pools` 的语义属于家族 provider `BufferPoolStatsProvider`
  （`const PREFIX = "/buffer-pools"`、`type Report = BufferPoolStats`），两者按 ADR-0021 §6.4 的
  `StatsReader`/`StatsProvider`/`report::<P>()` 契约组合；取值是**缓冲区个数**（不是字节），
  协议与 `MetricValue` 不新增类型。读取规则见 D7，候选签名见 6.5；实现属于外部
  `netsystem-client` 仓（跨仓契约）。
- **D8 明确不做：**`total` 条目、速率条目、`/buffer-pools/<pool>` 的向量/别名变体、node
  runtime stats、per-thread detail 条目、池的运行时增删、把池状态复制进 stats 段。

## 2. VPP 源码证据

路径相对仓库根，行号对应 vendored VPP。

| 编号 | 源码 | 核实的行为 |
| --- | --- | --- |
| V1 | `third_party/vpp/src/vlib/buffer.h:437-444` | 每线程缓存槽的形状：`VLIB_BUFFER_POOL_PER_THREAD_CACHE_SZ 512`，`vlib_buffer_pool_thread_t { CLIB_CACHE_LINE_ALIGN_MARK (cacheline0); u32 cached_buffers[512]; u32 n_cached; }`。`n_cached` 是**池拥有**的 cache-line 槽里的长度，不是 worker 私有栈。 |
| V2 | `third_party/vpp/src/vlib/buffer.h:446-468` | `vlib_buffer_pool_t` 的采样来源：`n_buffers`（`:457`）、`n_avail`（`:458`）、`buffers`（`:459`）、`name`（`:460`）、`lock`（`:461`）、`threads`（`:464`）；`VLIB_BUFFER_MAX_NUMA_NODES 32`（`:470`）。 |
| V3 | `third_party/vpp/src/vlib/buffer.c:461-469,602` | 建池时 `bp->buffers[bp->n_avail++] = bi`（`:467`），建完 `bp->n_buffers = bp->n_avail;`（`:602`）。`n_avail` 就是 free 链长度，`n_buffers` 此后不变。 |
| V4 | `third_party/vpp/src/vlib/buffer.c:811-825` | `buffer_get_cached()`：`clib_spinlock_lock (&bp->lock)` → `vec_foreach (bpt, bp->threads) cached += bpt->n_cached;` → unlock。求和取池锁，因为 `n_cached` 是普通 `u32`。 |
| V5 | `third_party/vpp/src/vlib/buffer.c:838-872` | 三个采集函数：`buffer_gauges_collect_used_fn` → `d->entry->value = bp->n_buffers - bp->n_avail - buffer_get_cached (bp)`（`:846`）；`..._available_fn` → `d->entry->value = bp->n_avail`（`:858`）；`..._cached_fn` → `d->entry->value = buffer_get_cached (bp)`（`:870`）。三者都先用 `buffer_get_by_index (vm->buffer_main, d->private_data)` 解析池，取不到就 `return`（`:840-845` 等）。 |
| V6 | `third_party/vpp/src/vlib/buffer.c:827-836` | `buffer_get_by_index (bm, index)`：`bm->buffer_pools` 为空或长度不足时返回 0——池只由 `private_data` 下标解析，没有名字查找。 |
| V7 | `third_party/vpp/src/vlib/buffer.c:937-956` | 登记循环的全部形状：`vec_foreach (bp, bm->buffer_pools)`（`:937`）；`vlib_stats_collector_reg_t reg = { .private_data = bp - bm->buffer_pools };`（`:939`）；`if (bp->n_buffers == 0) continue;`（`:940-941`）；三条 `reg.entry_index = vlib_stats_add_gauge ("/buffer-pools/%v/{cached,used,available}", bp->name)` + `reg.collect_fn = ...` + `vlib_stats_register_collector_fn (&reg)`（`:943-955`）。 |
| V8 | `third_party/vpp/src/vlib/buffer.c:783-786,526-530` | 池名由建立者的格式串给出：`vlib_buffer_pool_create (vm, data_size, physmem_map_index, "default-numa-%d", numa_node)`（`:785`）；`bp->name = va_format (0, fmt, &va);`（`:529`）。 |
| V9 | `third_party/vpp/src/vlib/buffer.c:607-638` | `format_vlib_buffer_pool`：CLI 表的 `Total/Avail/Cached/Used` 就是 `n_buffers`、`n_avail`、`Σ n_cached`、`n_buffers - n_avail - Σ n_cached`（`:620-630`），detail 行再按线程打印 `n_cached`（`:633-635`）——同一组三个事实的另一种输出，不是第二份存储。 |
| V10 | `third_party/vpp/src/vlib/buffer_funcs.h:583-614` | 分配快路径直接写 `bpt->n_cached`：`len = bpt->n_cached;`（`:583`）、`bpt->n_cached -= n_buffers;`（`:590`）、`bpt->n_cached = 0;`（`:606`）、`bpt->n_cached = len;`（`:614`），**不取 `bp->lock`**；只有整批从池 free 链取（`vlib_buffer_pool_get`）时才取池锁。 |
| V11 | `third_party/vpp/src/vlib/buffer_funcs.h:739-757` | 归还快路径同样直接写 `bpt->n_cached`（`:747-753,757`）。 |
| V12 | `third_party/vpp/src/vlib/buffer.c:532,693` | `vec_validate_aligned (bp->threads, vlib_get_n_threads () - 1, CLIB_CACHE_LINE_BYTES)`——`threads` 槽按线程数建立并可在变化时扩容。 |
| V13 | `third_party/vpp/src/vlib/stats/stats.h:31-57` | 采集者两张表与回调签名：`vlib_stats_collector_data_t { entry_index; vector_index; private_data; entry }`、`vlib_stats_collector_reg_t { collect_fn; entry_index; vector_index; private_data }`、`vlib_stats_collector_t { fn; entry_index; vector_index; private_data }`。回调只有一个参数，位置事实都在这一个值里。 |
| V14 | `third_party/vpp/src/vlib/stats/collector.c:131-151` | `do_stat_segment_updates`：`pool_foreach (c, sm->collectors)` 不取锁，为每条登记构造 `data = { .entry_index, .vector_index, .private_data, .entry = sm->directory_vector + c->entry_index }` 并调用 `c->fn (&data)`；循环之后 `sm->directory_vector[STAT_COUNTER_HEARTBEAT].value++`。 |
| V15 | `third_party/vpp/src/vlib/stats/stats.c:252-266`；`third_party/vpp/src/vlib/stats/shared.h:10-20` | `vlib_stats_add_gauge` = `vlib_stats_new_entry_internal (STAT_DIR_TYPE_GAUGE, name)`；`vlib_stats_set_gauge` 就是 `sm->directory_vector[index].value = value;`。gauge 是独立目录类型（`shared.h:10-20` 的 `stat_directory_type_t` 里 `STAT_DIR_TYPE_GAUGE` 是独立项，与 `STAT_DIR_TYPE_SCALAR_INDEX` 不同）。 |
| V16 | `third_party/vpp/src/vlib/buffer.c:937-956` 对照 `:607-638` | 目录里只有三个 gauge：`rg 'buffer-pools'` 在 `third_party/vpp/src` 只命中这三条 `vlib_stats_add_gauge`（其余是 prometheus/dump_metrics 消费方）。`Total` 只存在于 CLI 表里，由三个值相加得到，**不是目录条目**。 |

## 3. Hammer 现状与差异

| 编号 | 源码 | 核实的行为 |
| --- | --- | --- |
| H1 | `crates/hammer-core/src/buffer/main.rs:21-25,33-45,48-56` | 池的形状：`BufferMain { buffer_mem_start, buffer_mem_size, pools: Vec<BufferPool>, default_pool_by_numa }`；`BufferPool { mapping_index, index, data_size, allocation_size, first_buffer, buffer_count, free: Spinlock<Vec<u32>>, workers: Box<[RefCell<BufferThreadCache>]>, template }`；`BufferThreadCache { pool_index, thread_index, indices: [u32; BUFFER_THREAD_CACHE_HIGH_WATER], len: usize }`，`#[repr(align(64))]`。三个事实都在：`buffer_count`（VPP `n_buffers`）、`free` 的长度（VPP `n_avail`）、`Σ cache.len`（VPP `Σ n_cached`）；**没有名字字段**，也没有任何采样操作。 |
| H2 | `crates/hammer-core/src/buffer/mod.rs:22,28,32` | `BUFFER_CACHE_LINE_SIZE = 64`；`BUFFER_THREAD_CACHE_BATCH = 32`（从 free 链/回 free 链的批量）；`BUFFER_THREAD_CACHE_HIGH_WATER = 512`（缓存上限，与 VPP `VLIB_BUFFER_POOL_PER_THREAD_CACHE_SZ 512` 同数，V1）。 |
| H3 | `crates/hammer-core/src/buffer/pool.rs:1-19,59-77` | `borrow_worker_caches (&self, thread_index) -> Box<[RefMut<BufferThreadCache>]>` 的 SAFETY 要求调用者是 `thread_index` 那个唯一执行线程；`validate_caches` 要求 cache 集合与池集合一一对应。**轮次（thread zero）不能借别的 worker 的槽**：跨线程读 `RefCell` 是数据竞争。 |
| H4 | `crates/hammer-core/src/buffer/main.rs:88-104,137-139,178-240,242-254` | 池集合在 `init` 里一次建立：`numa_nodes` 为空 / `buffers_per_numa == 0` / `data_size == 0` → `DataPlaneError::BufferPoolsUnavailable`（`:94`）；每个映射建一个池，`indices` 为空同样 `BufferPoolsUnavailable`（`:210`），因此**不会有 `buffer_count == 0` 的池**；`buffer_count = indices.len()`（`:216`）；cache 槽 = `worker_count + 1`（`:137,:229`）；`BUFFER_MAIN.set` 断言只成功一次（`:242`），`global()` 返回已发布实例（`:249`）。池只增不减、名字集合进程内冻结。 |
| H5 | `crates/hammer-core/src/buffer/pool.rs:262-287,301-360` | 四个写点：分配时 cache 空则从 `free` 拉一个 batch（`:272-279`）、每取一个 `cache.len -= 1`（`:283`）；归还时先入 cache，`cache.len == HIGH_WATER` 则刷一个 batch 回 `free`（`:342-347`）、再 `cache.len += 1`（`:351`）。`free.lock()` 是这四个点唯一的锁，临界区是 `Vec` 的截断/追加。 |
| H6 | `crates/hammer-core/src/buffer/pool.rs:79-86` | `cached_free_buffers (caches, numa_node)` 是今天唯一"读缓存长度"的入口，且必须已经持有该 worker 的 cache 集合借用。 |
| H7 | `crates/hammer-stats/Cargo.toml`；`crates/hammer-core/Cargo.toml` | `hammer-stats → {hammer-core, hammer-infra}`；`hammer-core → hammer-infra`。池的采样类型与访问器可以放 `hammer-core` 并被 `hammer-stats` 的家族模块接受；`hammer-core` 不能依赖 `hammer-stats`（池里不能出现 stats 类型/路径）。 |
| H8 | `crates/hammer-stats/src/lib.rs:31-56,61,111-123,125-145` | 现状机制：`CollectorRegistration { collect: fn(DirectoryIndex, u32), entry_index, vector_index }`、表 `SpinLock<Vec<Collector>>`、`collect()` 每条采集函数自己 `segment.lock()` 后再 `advance_heartbeat`。**ADR-0021 D9/D10 已决定重建**（`Collector` trait 行、`register_collector(&mut self, impl Collector)`、表无锁、轮次不取段锁、条目借用交给采集者、条目级写）；本文按重建后的形状写，不按现状写。 |
| H9 | `crates/hammer-stats/src/segment.rs:196-230,428-450,569-580,934`；`crates/hammer-stats/src/protocol.rs:217` | 目录能力已具备：`add_gauge`/`set_gauge`（写标量槽）、`add_symlink`、`validate`、`entry(index) -> &DirectoryEntry`；`DirectoryType::Gauge = 9` 与 VPP `STAT_DIR_TYPE_GAUGE` 对应（V15）。家族不需要新目录原语。 |
| H10 | `crates/hammer-runtime/src/config/stats.rs:87-104,151-183,186-260` | owner 侧既有形状：`Sys` 用 `#[derive(Stats)]` 声明（三个 `bootstrap` 固定槽 + 两个 per-worker 向量），`init_stats_main` = 建段 → `Sys::bootstrap` → 绑定 listener → `run_stats_registrations()`；两个堆各一条 `#[stats_registration]`（ADR-0021 将换成声明 + `register_collector`）。 |
| H11 | `crates/hammer-runtime/src/init.rs:209-227`；`crates/hammer-runtime/src/lib.rs:31-41` | `run_stats_registrations()` 按 registration image 的 `stats_registrations` 列表顺序对每项调用 `(registration.register)(stats_main)`；列表顺序 = 执行顺序。内置 image 当前是 `Sys` → worker 数 → per-worker 向量 → 两个堆。 |
| H12 | `crates/hammer/src/main.rs:162-175`；`crates/hammer-runtime/src/main_loop.rs:44-46`；`crates/hammer-runtime/src/init.rs:205-207` | 顺序事实：`DataPlaneMain::new_main(&threads)`（内部建 `BufferMain`，`data_plane/buffer_pool.rs:21-45`）在 `main_loop::run` **之前**；`main_loop::run` 里先 `run_init_functions`（含 `init_stats_main` → `run_stats_registrations()`）。⇒ **登记期 `BufferMain::global()` 已发布、池集合与 cache 槽数已冻结**，与 VPP 在 `vlib_buffer_main_init` 里登记同一个时机（V7）。 |
| H13 | `crates/hammer-runtime/src/data_plane/buffer_pool.rs:22-45` | 池集合的来源：从 worker 的 NUMA 节点收集 `numa_nodes`、排序去重（无节点时退化为 `[0]`），再调 `BufferMain::init(...)`。`BufferMain::init` 自身不去重（H4）：重复节点会让两个池同名，见 D6。 |
| H14 | `rg '/buffer-pools' crates/ docs/adr/0021-…` 无命中（除本文） | 今天没有任何 buffer pool stats 条目、声明、采集者、别名或客户端 provider；本文是这一家族的第一份设计。 |

## 4. 拟议决定

### D0 家族契约与目录形状

| 条目 | 类型 | owner（业务） | 更新者与节奏 |
| --- | --- | --- | --- |
| `/buffer-pools/<pool-name>/cached` | `Gauge` | buffer pool 的 owner：`hammer-runtime`（池由 `DataPlaneMain::new_main` 建立，H12） | 每池一条 `BufferPoolGaugeCollector` 登记项；值 = `Σ` 池的每线程 `cached_counts` 槽（V5 的 `_cached_fn`，D3） |
| `/buffer-pools/<pool-name>/used` | `Gauge` | 同上 | 同上；值 = `buffer_count - available - cached`（V5 的 `_used_fn`） |
| `/buffer-pools/<pool-name>/available` | `Gauge` | 同上 | 同上；值 = `free` 链长度（V5 的 `_available_fn`） |

- **池名 = `default-numa-<numa-node>`**，与 VPP 的调用点逐字一致（V8），由池在建立时确定并随池保存
  （D2 的 `BufferPool.name`）；名字在一台机器上唯一（一个 NUMA 节点一个池，H13）。
- **类型是 `DirectoryType::Gauge`**（H9 的 `add_gauge`）：与 VPP `STAT_DIR_TYPE_GAUGE` 相同（V15），
  值是 `u64` 个缓冲区**个数**（不是字节；与 `/mem/<heap>` 的字节列不可混用）。
- **不发布 `total` 条目**：VPP 目录里只有这三个 gauge，`Total` 只出现在 CLI 表的计算里（V16、V9）。
  客户端需要 total 时由三个值相加，或由后续命令提供；本文不加第四个 gauge，也不加别名。
- **不做 `/buffer-pools/<pool>` 向量 + 别名变体**：mem 家族用"一条 7 列向量 + 3 个别名"是因为
  VPP 的 mem provider 自己就是那个形状；buffer pool 在 VPP 里是三个独立 gauge，照搬即可。
- **没有速率条目**：VPP 的 buffer pool stats 只有三个瞬时 gauge（V5），没有 rate，也没有
  per-thread detail 条目（`format_vlib_buffer_pool` 的 detail 行只是 CLI 输出，V9）。

### D1 声明：`#[derive(Stats)]` 声明每池三个 gauge（参数化声明）

**D1.1 形状。**池的三个 gauge 在 owner 模块里用**一条 `#[derive(Stats)]` 声明**描述：条目名模板、
行宽（gauge 无行/列）、字段与 gauge 的对应。VPP 在登记循环里用格式串
`"/buffer-pools/%v/cached"` + `bp->name` 生成名字（V7、V8），Hammer 的对应物是
**`path` 里的 `{pool_name}` 占位符**：`path` 是模板，占位符就是 install 的参数，宏为每个不同的
占位符生成一个 `&str` 形参。

```rust
// crates/hammer-runtime/src/data_plane/buffer_stats.rs（owner 侧声明）

/// 每池三个 gauge，名字与 VPP 的登记循环逐字对应（`buffer.c:944,948,953`）。
///
/// 这是一条**参数化声明**：`{pool_name}` 是 install 参数，由池自己的名字填
/// （VPP `bp->name`，`buffer.c:529`）。宏不认识 `/buffer-pools`，只把 owner 给的
/// 模板与字段绑成条目（同名模板字段由同一次 install 建立）。
#[derive(Stats)]
pub(crate) struct BufferPoolGauges {
    #[stats(path = "/buffer-pools/{pool_name}/cached")]
    cached: Gauge,
    #[stats(path = "/buffer-pools/{pool_name}/used")]
    used: Gauge,
    #[stats(path = "/buffer-pools/{pool_name}/available")]
    available: Gauge,
}
```

宏生成 `BufferPoolGauges::install(segment: &StatsSegment, pool_name: &str) -> StatsResult<Self>`：
一次 install 建立三个 gauge 并返回它们（每个 `Gauge` 持有自己的 `DirectoryIndex`）。
**参数化声明不生成 registration image 项**——它的实例化由 owner 的采集登记项驱动
（VPP 的登记循环也在同一个函数里做"建条目 + 登记采集者"两件事，V7），这一点与
ADR-0021 的固定声明（`Sys`、`MainHeapUsage`：声明项自己进 image）不同，理由写在 D1.2。

**D1.2 每池 install 由 owner 的登记项驱动（VPP 的 `vec_foreach`）。**

```rust
// 同一个 owner 模块：登记项 = VPP `buffer.c:937-956` 的循环体
#[stats_collect_registration]
fn register_buffer_pools(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    let buffer_main = BufferMain::global();
    // VPP: vec_foreach (bp, bm->buffer_pools)（buffer.c:937）
    for pool_index in 0..buffer_main.pool_count() {
        let pool_index = pool_index as u8;
        // VPP: vlib_stats_add_gauge ("/buffer-pools/%v/…", bp->name)（buffer.c:944,948,953）
        let gauges = BufferPoolGauges::install(&stats_main.segment, buffer_main.pool_name(pool_index))?;
        // VPP: reg.private_data = bp - bm->buffer_pools; register_collector_fn（buffer.c:939,946,950,955）
        for (gauge, entry_index) in [
            (BufferPoolGauge::Cached, gauges.cached.index),
            (BufferPoolGauge::Used, gauges.used.index),
            (BufferPoolGauge::Available, gauges.available.index),
        ] {
            stats_main.register_collector(BufferPoolGaugeCollector {
                entry_index,
                pool_index,
                gauge,
            });
        }
    }
    Ok(())
}
```

**D1.3 为什么需要"参数化声明"而不是下面三条备选**（都需要在审批时确认；A6）：

| 备选 | 为什么不用 |
| --- | --- |
| 每个池一条静态声明（像 `/mem` 的五个堆） | 池数是运行期事实（NUMA 节点集合，H13），编译期不知道有几条；照 `MAX_BUFFER_POOLS = 255` 展开是不可接受的。 |
| owner 的登记项直接 `segment.add_gauge(&format!("…"))`（VPP 的写法，也是旧 ADR-0022 的 `register_buffer_pool_*` 自由函数） | 用户已明确：**注册语义由 `#[derive(Stats)]` 承载**，条目名/形状不在业务代码里命令式创建（ADR-0021 D9.5 把 mem 的同名自由函数删掉就是这个决定）。轨迹式的 `add_gauge` 调用也让"这个家族有哪些条目"无法从一个声明读出来。 |
| 一条条目 `/buffer-pools`、行 = 池、列 = 3 | 丢掉 VPP 的每池独立名字与 gauge 类型（V7、V15），客户端要解析行号，且与 `/mem/<heap>` 的每堆独立条目不对称。 |
| 新增一条"家族声明"专用宏（如 `#[derive(PoolStats)]`） | 多一个只服务一个家族的宏；`{name}` 占位符是 `path` 表达式能力（ADR-0021 已要求 `path` 可求值）的自然延伸，宏仍然只搬模板与字段，不认识任何家族语义。 |

参数化声明**不新增**"更新语义进宏"的口子：宏只生成 `install`（建条目），不生成采集者、不生成
`collect_fn`、不生成 `private_data`（那三样属于采集者类型与登记项，D4）。

### D2 采样面：`hammer-core` 的三个只读访问器与一次采样

池的 owner 是 `hammer-core`（H1、H7）；`/buffer-pools` 的条目与采集者是 `hammer-runtime`
（D0）。因此 `hammer-core` 只补"把池的三个事实读出来"的窄面，不含任何 stats 概念。

```rust
// crates/hammer-core/src/buffer/main.rs

/// 一次池采样：三个事实与 VPP 的三个 gauge 一一对应（`buffer.c:838-872`）。
///
/// 拥有数值，不借用池、不持有锁；`cached` 是各 worker 缓存长度之和（VPP `Σ n_cached`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolUsage {
    pub buffer_count: u64, // VPP `n_buffers`（建池后不变，`buffer.c:602`）
    pub available: u64,    // VPP `n_avail`（池 free 链长度）
    pub cached: u64,       // VPP `Σ bpt->n_cached`
}

impl BufferPoolUsage {
    /// VPP `buffer_gauges_collect_used_fn` 的公式（`buffer.c:846`）。
    ///
    /// 三个事实不是同一瞬间的原子快照（VPP 同样是三次独立读，V5）：瞬时不一致时取 0，
    /// 不使用 `u32` 回绕（VPP 的 C 写法会回绕成一个巨大的值）。
    pub fn used(&self) -> u64 {
        self.buffer_count
            .saturating_sub(self.available)
            .saturating_sub(self.cached)
    }
}

impl BufferMain {
    /// 已建立的池数；池集合在 `init` 后不再变化（H4）。
    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }

    /// 池名（VPP `bp->name`，`buffer.c:529`）；`pool_index` 必须来自 `pool_count()`。
    pub fn pool_name(&self, pool_index: u8) -> &str {
        &self.pools[usize::from(pool_index)].name
    }

    /// 采样一个池；`pool_index` 同上。
    ///
    /// 只读：`available` 在池的 free 链锁内取长度（VPP 读 `n_avail` 不取锁，V4 里那把锁
    /// 保护的是 `n_cached` 的求和；Hammer 的 free 链是 `Spinlock<Vec<u32>>`，读长度必须持锁），
    /// `cached` 用 Relaxed 求和（D3）。没有分配、没有 I/O。
    pub fn pool_usage(&self, pool_index: u8) -> BufferPoolUsage {
        let pool = &self.pools[usize::from(pool_index)];
        // `available` = VPP `n_avail`：free 链长度，就是 `_available_fn` 写的那个值（V5）。
        let available =
            u64::try_from(pool.free.lock().len()).expect("a pool's free chain fits u64");
        // `cached` = VPP `Σ bpt->n_cached`（V4、V5）：每个 runtime thread 自己的槽一次
        // `Relaxed` 读，不借 `RefCell`、不取池锁——槽本身就是池上可共享读的原子（D3）。
        let cached = pool
            .cached_counts
            .iter()
            .map(|count| {
                u64::try_from(count.load(Ordering::Relaxed)).expect("a cached count fits u64")
            })
            .sum();
        BufferPoolUsage {
            // `buffer_count` 建池后不变（H4），一次读即可（VPP `bp->n_buffers`）。
            buffer_count: u64::try_from(pool.buffer_count).expect("a pool's buffer count fits u64"),
            available,
            cached,
        }
    }
}
```

- **`BufferPool.name: String`** 作为字段保存在池上（VPP `bp->name` 同位置，V8）：建立时用
  `format!("default-numa-{numa_node}")` 生成一次（`main.rs:178-240` 的建池循环里），此后不变，
  不进热路径。
- 访问器只给"名字 + 一次采样"，**不暴露 `BufferPool`、不返回持有锁的 guard、不提供迭代器
  或切片**：唯一的跨线程读者是轮次，它只需要这三个事实（H3 禁止它借 worker 的 `RefCell`）。
- 采样不做缓存、不做增量、不记基线：每次调用就是一次读（VPP 的采集函数也是每次现读，V5）。

### D3 每线程缓存长度的发布：池拥有的原子槽（VPP `n_cached`）

VPP 的 `n_cached` 在池拥有的 `bp->threads[]` 槽里（V1），拥有者线程直接写（V10、V11），
采集方在池锁下求和（V4）。Hammer 今天的同位置是 `BufferPool.workers[thread].len`（H1），但它是
`RefCell` 里的普通 `usize`：worker 在整次操作里持有该槽的可变借用（H3），轮次连 `borrow()`
都会 panic——**把 `len` 的类型改成 `AtomicUsize` 也不解决**：原子只让并发读合法，不会让轮次
拿到那个 `RefCell` 的借用。

**决定：长度搬到池自己的每线程槽：`BufferPool.cached_counts: Box<[AtomicUsize]>`（一个 runtime
thread 一个槽，长度 = `workers.len()`，即 `worker_count + 1`，H4），`BufferThreadCache` 的
`len` 字段删除。**

- **写者**：拥有该槽的 worker，仍只在现有四个写点写（`pool.rs:272-279,283,342-347,351`，H5）。
  它在一次操作里以局部变量持有长度，在批边界用 `Relaxed` 存进本线程的槽
  （`self.cached_counts[usize::from(cache.thread_index)].store(len, Ordering::Relaxed)`，
  `alloc_indices` 的两条返回路径、`free_buffers` 的循环末尾），语义与 VPP 把 `n_cached`
  当普通计数器改一样，只是 Rust 里并发读必须走原子。
- **读者**：轮次（thread zero）在 `pool_usage` 里按线程槽 `Relaxed` 求和——就是
  `buffer_get_cached` 的对应物（V4），但**不取池锁**、不碰 `workers` 的 `RefCell`（VPP 取锁是
  因为 `n_cached` 是普通 `u32`；Hammer 的槽本身是原子的，轮次也不需要与写者建立任何顺序——
  统计值允许迟一拍，AGENTS 的同步规则里 `Relaxed` 正是给统计的）。
- **为什么槽必须在 `RefCell` 外面**：`BufferPool.workers` 的每个槽是
  `RefCell<BufferThreadCache>`，worker 独占借它很久（H3）；轮次借不到它，因此也借不到它
  里面的任何字段，无论那个字段是不是原子。VPP 的 `vlib_buffer_pool_thread_t` 把 indices 与
  `n_cached` 装在同一个结构里，采集者直接读那个结构（V1、V4）；Hammer 的 `RefCell` 把这个
  结构一分为二，于是 `n_cached` 的那一半与 `RefCell` 并列放在池上，读者够得到的那一半
  才是可以共享读的。
- **一份事实、没有影子字段**：`BufferThreadCache` 不再保存 `len`，唯一的计数就是池上这个
  槽；`indices` 数组与它的长度不再存两份。
- **代价与验收**：每个索引的搬动多一次 Relaxed store（分配/归还路径已有 batch 摊薄，H2）；
  M3 用 release/LTO 汇编与基准确认不回归（§8、§9）。
- **不做**：不给每池加"缓存总数"聚合原子（那是第二份事实，且 VPP 没有）、不给轮次加锁、
  不给槽加 cache-line 隔离包装（VPP 用 `CLIB_CACHE_LINE_ALIGN_MARK`；这里每批只写一次，
  伪共享代价可以忽略，也不为了占位新增一个包装类型）、不改 `indices` 数组的 worker 独占语义。

### D4 采集：一个采集者类型，每池三条登记（VPP 三条 `register_collector_fn`）

```rust
// crates/hammer-runtime/src/data_plane/buffer_stats.rs

/// VPP 三个 `buffer_gauges_collect_*_fn`（`buffer.c:838-872`）在 Hammer 的对应物。
///
/// VPP 用三个函数 + `private_data`（池下标）区分；Hammer 用**一个类型**承载三者：
/// 实例字段就是 VPP 表行的 `entry_index` 与 `private_data` 两格（V13），
/// `gauge` 是三个函数之间唯一不同的那一格（写哪个事实）。
///
/// VPP: `d->entry` 由机制解析后交给回调；Hammer: `collect` 收到本条目的共享借用。
struct BufferPoolGaugeCollector {
    entry_index: DirectoryIndex, // VPP `reg.entry_index`
    pool_index: u8,              // VPP `reg.private_data = bp - bm->buffer_pools`
    gauge: BufferPoolGauge,      // VPP 三个 collect_fn 的差别
}

impl Collector for BufferPoolGaugeCollector {
    fn entry_index(&self) -> DirectoryIndex {
        self.entry_index
    }

    fn collect(&self, entry: &DirectoryEntry) {
        // VPP: bp = buffer_get_by_index (vm->buffer_main, d->private_data)
        let usage = BufferMain::global().pool_usage(self.pool_index);
        // VPP: d->entry->value = …
        hammer_stats::buffer_pools::update_pool_gauge(entry, self.gauge, usage);
    }
}
```

- 每池三个实例（cached/used/available），登记顺序 = VPP 的 `cached` → `used` → `available`
  （V7）；每条登记的 `entry_index` 是它自己那条 gauge（VPP 每条登记只写一条 gauge，V5）。
- 采集者不取段锁、不查目录、不认识别的条目：`collect` 的入参就是它自己那条 gauge（V14、
  ADR-0021 D9.3）。
- **不复制 VPP 的 `if (!bp) return;`**（V5、V6）：登记遍历的就是已发布池集合，池只增不减
  （H4），取不到池是 bug 而不是运行期状态——按仓库错误规则用断言终止（与 ADR-0021 轮次里
  `entry(...)` 的 `expect` 同一条规则）。
- 每池不额外登记"总览"条目、不做 per-thread 登记（V9 的 detail 只是 CLI 输出）。

### D5 更新映射：`hammer-stats::buffer_pools` 家族模块

VPP 的换算只出现在三个采集函数体里（V5）。Hammer 把它收在家族模块，业务侧（`hammer-runtime`）
只调它：

```rust
// crates/hammer-stats/src/buffer_pools.rs（新文件）

/// VPP 三个 `buffer_gauges_collect_*_fn` 里"写哪一条 gauge"的差别（`buffer.c:838-872`）。
pub enum BufferPoolGauge {
    Cached,
    Used,
    Available,
}

/// 把一次池采样写进该 gauge 条目；与 VPP `d->entry->value = …` 同一条写路径。
///
/// 条目级写：不取段锁、不分配、不返回 `Result`（形状/类型是声明与登记的事，
/// 不适配即本模块的 bug）。
pub fn update_pool_gauge(
    entry: &DirectoryEntry,
    gauge: BufferPoolGauge,
    usage: BufferPoolUsage,
) {
    entry.set_scalar(match gauge {
        BufferPoolGauge::Cached => usage.cached,
        BufferPoolGauge::Used => usage.used(),
        BufferPoolGauge::Available => usage.available,
    });
}
```

- 家族模块只认识"三个事实 → 一个 gauge 值"，不认识池、不持锁、不建条目、不登记采集者
  （登记是 owner 的登记项，D1.2）。
- `hammer-stats` 已依赖 `hammer-core`（H7），所以这里直接接受 `BufferPoolUsage`；`StatsSegment`
  与 `StatsMain` 上**不加任何 buffer 家族方法/字段**（机制不认识 `/buffer-pools`）。
- 三个 gauge 各由一条登记采样，所以同一轮里三次读不是同一瞬间（VPP 相同，V5）；
  客户端不得假设 `cached + available + used == buffer_count` 在跨条目读取时成立。

### D6 失败、容量与启动顺序

- **登记顺序**：`register_buffer_pools` 列在 image 的 `stats_registrations` 里、`Sys` 之后
  （H11 的列表顺序 = 执行顺序；池条目是普通 gauge，不需要固定槽）。
- **失败即启动 `Err`**：`install` 或 `register_collector` 失败沿 `run_stats_registrations()`
  冒泡 → `init_stats_main` 失败 → 启动终止。此时**没有 worker、没有 `statseg-collector-process`
  轮次**（H12），所以"已经装好的前几个池条目"不可能被任何客户端读到，也不写逐池回滚
  （旧 ADR-0022 的逐池回滚/`release_partial` 形状不再出现）。
- **重名**：`BufferMain::init` 自己不去重 `numa_nodes`（H4），重复节点会让两个池同名，
  第二条 `install` 以既有的条目重名错误失败 → 同一个启动 `Err` 路径。今天唯一的生产调用点
  已经排序去重（H13），本文不为它新增 `DataPlaneError` 变体；这一条进 §9 未决项。
- **容量**：池数 ≤ `MAX_BUFFER_POOLS`（255）由 `BufferMain::init` 把关（`BufferMain:88-94`），
  所以登记产生的条目数（3 × 池数 ≤ 765）不会失控；不做额外的容量检查。
- **名字长度**：条目名 = `/buffer-pools/` + `default-numa-<n>`，远小于目录名上限
  （VPP `VLIB_STATS_MAX_NAME_SZ 128`，Hammer 的声明宏上限 126）；不需要截断逻辑。

### D7 client 契约（外部 `netsystem-client` 仓）

客户端实现属于外部 `netsystem-client` 仓库；本节定义它必须遵守的共享内存契约与客户端内部分层，
候选签名见 6.5。本仓不新增客户端类型，也不改变协议 crate 的类型集合。

**客户端分层（与 ADR-0021 D7 同一三层，不重复论证）：**

| 层 | 类型 | 职责 | 不负责 |
| --- | --- | --- | --- |
| 底层交互（机制） | `StatsClient` | 连接（socket + SCM_RIGHTS）、只读映射、epoch 重试与畸形映射拒绝、`names()`、`read(name) -> MetricValue` | 不认识 `/buffer-pools`、池名或度量名；不开设家族方法（`StatsClient::buffer_pool_usages` 这类形状不加） |
| family 投影 | `BufferPoolStatsProvider`（零尺寸类型，`const PREFIX = "/buffer-pools"`） | 用 `names()` 枚举家族名字、按段切出池名与度量、校验类型是 `Gauge`、把三个值翻译成 typed report；每个池的三个条目各读一次 | 不持有连接/映射、不缓存 `DirectoryIndex`、不写段、不保存跨轮基线 |
| 使用方 | `client.report::<BufferPoolStatsProvider>()` 的调用者、格式化/CLI | 读一份 report 并组合输出 | 不重新实现名字/度量解析、不把计数当字节 |

provider 只依赖 `StatsReader`（`names()`/`read()`）这一个能力 trait，所以家族投影可以在内存
fixture 上单测，不需要真实 socket 或映射（ADR-0021 §6.4 的同一取舍）；`report` 是编译期单态化的
泛型入口——没有 `dyn`、没有 provider 注册表。

**服务端契约（客户端可以依赖的事实）：**

| 名字 | 类型 | 形状 | 语义与读取规则 |
| --- | --- | --- | --- |
| `/buffer-pools/<pool>/cached` | `Gauge`（=9） | scalar | 各 worker 缓存长度之和（V5、D2） |
| `/buffer-pools/<pool>/used` | `Gauge` | scalar | `buffer_count - available - cached`（V5、D2） |
| `/buffer-pools/<pool>/available` | `Gauge` | scalar | 池空闲链长度（V5） |

池名 = `default-numa-<n>`（D0、V8）；一个池三条条目；目录里**没有** `/buffer-pools/<pool>` 裸条目、
没有 symlink、没有向量、没有速率条目（D8）。

**读取规则：**

1. 枚举：`names()` 里以 `/buffer-pools/` 开头的名字，去掉前缀后按 `/` 切成两段——第一段是池名，
   第二段是度量名（`cached|used|available`）。度量名不属于这三个的条目（今天不存在）不进池集合、
   不报错、不警告：服务端将来加新度量是兼容扩展，不破坏既有 provider。池集合 = 目录里出现的池名
   并集；客户端不假设池数、不假设 NUMA 节点连续、不从服务端源码推断（H4、H13）。
2. 取值：`Gauge` 的 u64 是缓冲区**个数**，不是字节；不得与 `/mem/<heap>` 的字节列相加或比较。
3. 每个池的三个 gauge 各读一次，**不构成原子快照**（V5、D2）：三个值可能来自相邻两轮；客户端可以
   重试结构变化（epoch），但不得声称拿到"同一瞬间"的三个值。
4. 形状/类型由 provider 校验：条目类型不是 `Gauge`、某个池缺少三个度量之一 → 客户端仓的类型化
   错误（带池名与度量名），不静默补 0、不 panic、不裁剪。
5. `total` 不在目录里（V16）：需要"和"时由调用方相加，provider 不发布也不缓存派生值。
6. `MetricValue` 不需要新变体（gauge 已存在），`hammer-stats-protocol` 不新增类型、
   `STAT_SEGMENT_VERSION` 不变（与 ADR-0021 的结论相同）。

**客户端明确不做的事：**不重建池、不写段、不缓存跨 epoch 的 `DirectoryIndex`、不把家族方法加到
`StatsClient`、不引入 provider 注册表或 `dyn` 查找、不把三个 gauge 当字节或当原子快照、不在客户端
求和缓存长度（求和只在服务端采样里发生，D2、D3）。

**格式化归属：**`show buffer-pool` 一类的输出属于 Module-owned ctl（CONTEXT），本 ADR 只保证数据
来源；命令的请求/回复与格式化器由对应 owner 另行设计，本节不新增 Binary API 命令。

**互操作证据：**本仓提供只读映射 fixture（6.5）：`crates/hammer/tests/stats_segment_mapping.rs`
增加 `/buffer-pools/default-numa-0/{cached,used,available}` 的读取样例（名字、类型 `Gauge`、
取值 = 个数）；客户端仓对同一 fixture 跑自己的 provider，两份 report 必须相同。

### D8 明确不做

- 不发布 `total`、不做速率（瞬时值，VPP 也没有 rate）、不做 per-thread detail 条目（V9）。
- 不做 `/buffer-pools` 的向量 + symlink 变体（D0）。
- 不复活旧 ADR-0022 的 node runtime stats、进程级 node 计数块、`publish_*` 家族步骤、
  `release_partial`、`MainLoopRate` 之类自造类型。
- 不做池的运行时增删或热加缓存槽（池集合与 cache 槽数进程内冻结，VPP 的 `threads` 扩容
  V12 在 Hammer 没有对应需求：worker 数在启动时决定，H4）。
- 不把池状态（`n_buffers`/`n_avail` 的副本）复制进 stats 段或 runtime 的其它结构；三个 gauge
  是唯一落点。
- 不在 `StatsSegment`/`StatsMain` 上加 buffer 家族方法、字段或别名创建路径。

## 5. 分层隔离契约

| 层 | 可以调用/持有 | 不可以调用/持有 | 验证边界 |
| --- | --- | --- | --- |
| hammer-core（buffer pool 的 owner） | `BufferPool` 的三个事实、一次采样 `BufferPoolUsage`、`pool_count`/`pool_name`/`pool_usage`、池上的每线程 `cached_counts` 原子槽 | 任何 stats 类型/路径/列序/gauge 名字、`StatsSegment`、`StatsMain`、hammer-stats 依赖（H7） | `pool_usage` 数值正确（采样 = 一次读、无分配、无 stats 依赖）、`used()` 不回绕；采样与分配并发时无数据竞争（Relaxed） |
| hammer-stats（机制） | 既有目录原语（`add_gauge`/`set_gauge`/`validate`/`add_symlink`/`entry`）、采集者表与轮次、条目级写 | `/buffer-pools` 的名字、池下标、池对象、`BufferPoolUsage` 之外任何 buffer 概念；`collect()` 的实现里不出现 buffer 家族 | 机制 crate 在没有 buffer 声明/采集者的情况下独立编译；`StatsSegment`/`StatsMain` 的公共签名不出现 `BufferPool*` |
| `/buffer-pools` 家族模块（`hammer-stats::buffer_pools`） | `BufferPoolGauge`（三个事实的身份）、`BufferPoolUsage` 的换算与条目级写 | 池对象、`BufferMain`、`StatsSegment`/`StatsMain`、条目注册与采集者登记、池名与路径 | 家族模块里没有条目创建、没有池名字、没有锁；换算只有一处（V5 的三个函数体在 Hammer 的单一落点） |
| hammer-runtime（池条目的 owner） | `BufferPoolGauges` 声明（`path` 模板 + 三个 gauge 字段）、`BufferPoolGaugeCollector`（字段 = `entry_index` + 池下标 + gauge）、`register_buffer_pools` 登记项、image 里的一条列表项 | 给机制加家族字段/方法、把池状态复制进段、替池拥有计数、在采集里取段锁、在轮次里借 worker 的 `RefCell`、在别处再写一次三个 gauge、第二条登记通道 | 声明与目录一致（三条名字/类型）、每池三条登记与声明一一对应、轮次只写自己那条 gauge、登记顺序与 image 一致、池名字与 VPP 格式一致 |
| `statseg-collector-process`（node，既有） | 每轮一次 `StatsMain::collect()`、`update_interval`、sleep（ADR-0021 H12） | 池名/池下标/`BufferPoolUsage`、任何家族步骤、第二条 process node | node 的代码里不出现 buffer 概念；轮次的全部 buffer 工作来自登记表 |
| 外部 stats client | `StatsClient` 的只读映射与 `names()`/`read()`；`BufferPoolStatsProvider`（`PREFIX = "/buffer-pools"`）把三个 gauge 投影成 `BufferPoolStats` | 家族方法进 `StatsClient`、`dyn`/注册表式 provider 查找、写段、缓存跨 epoch 的 `DirectoryIndex`、把三个 gauge 当原子快照、把计数当字节、provider 持有基线或缓存求和 | 只读映射、名字/类型解码、provider 在 fixture 与真实映射上得到相同 report、缺度量时的类型化错误 |

边界检查以编译、真实 lifecycle 与可观察行为为主；`rg` 只用于迁移删除清单，不能替代行为证明。
本设计不新增跨层 wrapper：`BufferPoolUsage` 是拥有数值的值类型（换算是它的方法），
`gauge → 值`的映射在家族模块，采集者只搬一个值。

## 6. 新增/修改 API 审批清单

以下都是**拟议、未批准**。每项给出最终结果、owner/消费者，以及为什么现有 surface 不足。

### 6.1 hammer-core（A1–A4）

```rust
// crates/hammer-core/src/buffer/main.rs

/// A1：一次池采样（三个事实，见 D2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolUsage {
    pub buffer_count: u64,
    pub available: u64,
    pub cached: u64,
}

impl BufferPoolUsage {
    pub fn used(&self) -> u64;
}

pub(super) struct BufferPool {
    // A3：VPP `bp->name`（`buffer.c:529`）；建池时生成一次，此后不变。
    pub(super) name: String,
    // A4：VPP `bp->threads[ti].n_cached` 的位置（D3）。一个 runtime thread 一个槽：
    // 拥有者线程 `Relaxed` 写自己的槽，轮次 `Relaxed` 求和——两者都走 `&BufferPool`，
    // 因此都绕开了 `workers` 的 `RefCell`。
    pub(super) cached_counts: Box<[AtomicUsize]>,
    // ...既有字段不变...
}

pub struct BufferThreadCache {
    // A4：`len: usize` 删除；长度就是池上的 `cached_counts[thread_index]`（D3），
    // 一次操作内以局部变量持有、批边界存回。`indices` 数组本身不变。
    pub(super) indices: [u32; BUFFER_THREAD_CACHE_HIGH_WATER],
    // ...既有字段不变...
}

impl BufferMain {
    // A2：三个只读访问器（D2）。
    pub fn pool_count(&self) -> usize;
    pub fn pool_name(&self, pool_index: u8) -> &str;
    pub fn pool_usage(&self, pool_index: u8) -> BufferPoolUsage;
}
```

| 项 | 位置 | 动作 | 说明与迁移 |
| --- | --- | --- | --- |
| A1 `BufferPoolUsage` | `crates/hammer-core/src/buffer/main.rs` | 新增（`pub`、`Copy`） | 拥有数值、不借用池；`HeapUsage`（ADR-0021 A1，hammer-infra）的同族形状，消费者是 `hammer-stats::buffer_pools` 与 runtime 的采集者。没有它，采集者要么借 `BufferPool`（`pub(super)`，且会跨线程碰 `RefCell`，H3）要么自己拼三个字段（把业务换算搬进 runtime） |
| A2 `pool_count`/`pool_name`/`pool_usage` | 同上 | 新增 | 唯一跨线程只读面。没有它，登记项拿不到池名与三个事实（H1、H3）。刻意不返回 `&BufferPool`、不返回 guard、不返回迭代器 |
| A3 `BufferPool.name` | `main.rs:33-45` | 新增字段 | VPP 的 `bp->name` 在同一个结构里（V2、V8）；没有它，条目名只能由 runtime 自己拼 NUMA 号，池的身份就不在池自己身上 |
| A4 `BufferPool.cached_counts: Box<[AtomicUsize]>`（`BufferThreadCache.len` 删除） | `main.rs:33-45`（新字段）、`main.rs:48-56`（删字段）；写入点 `pool.rs:272-279,283,342-347,351` | 新增字段 + 删除字段 + 写点用 `Relaxed` 存回自己的槽 | 轮次无法借 worker 的 `RefCell`（H3），所以 `len` 留在 `RefCell` 里（哪怕类型是 `AtomicUsize`）轮次也够不到；没有它就没有 `cached` 这个事实。长度只存一份（D3 的"没有影子字段"） |

### 6.2 hammer-stats：`buffer_pools` 家族模块（A5）

```rust
// crates/hammer-stats/src/buffer_pools.rs（新文件）

/// A5：三个事实的身份（VPP 三个 `buffer_gauges_collect_*_fn` 的差别，`buffer.c:838-872`）。
pub enum BufferPoolGauge {
    Cached,
    Used,
    Available,
}

/// A5：把一次池采样写进该 gauge 条目（VPP `d->entry->value = …`）。
pub fn update_pool_gauge(
    entry: &DirectoryEntry,
    gauge: BufferPoolGauge,
    usage: BufferPoolUsage,
);
```

| 项 | 位置 | 动作 | 说明与迁移 |
| --- | --- | --- | --- |
| A5 `BufferPoolGauge` + `update_pool_gauge` | `crates/hammer-stats/src/buffer_pools.rs`（`lib.rs` 里 `pub mod buffer_pools;`） | 新增 | 家族换算（`used = count - available - cached`）的唯一落点，对应 VPP 三个采集函数体（V5）；`mem::update_mem_usage`（ADR-0021 §6.2）的同族形状。没有它，runtime 的采集者要自己写列映射，家族语义就散在业务里 |
| 机制 | `crates/hammer-stats/src/{lib.rs,segment.rs}` | **不修改**（除 ADR-0021 已列项） | 本家族不需要新目录原语：gauge 的建/写已存在（H9）。不新增 `add_buffer_pool*`/`set_heap_usage` 之类机制方法（用户明确反对把业务放进机制），也不在轮次里加家族步骤 |

### 6.3 hammer-runtime：owner 侧声明、采集者与登记项（A7、A8）

```rust
// crates/hammer-runtime/src/data_plane/buffer_stats.rs（新文件；`data_plane/main.rs` 里 `mod buffer_stats;`）

/// A7：参数化声明（D1.1）；`{pool_name}` 是 install 参数。
#[derive(Stats)]
pub(crate) struct BufferPoolGauges {
    #[stats(path = "/buffer-pools/{pool_name}/cached")]
    cached: Gauge,
    #[stats(path = "/buffer-pools/{pool_name}/used")]
    used: Gauge,
    #[stats(path = "/buffer-pools/{pool_name}/available")]
    available: Gauge,
}

/// A7：一个采集者类型服务所有池（VPP 的 `entry_index` + `private_data` + 选择器）。
struct BufferPoolGaugeCollector {
    entry_index: DirectoryIndex,
    pool_index: u8,
    gauge: BufferPoolGauge,
}

impl Collector for BufferPoolGaugeCollector { /* D4 */ }

/// A7：VPP `buffer.c:937-956` 的登记循环。
#[stats_collect_registration]
fn register_buffer_pools(stats_main: &mut StatsMain) -> RuntimeResult<()>;
```

| 项 | 位置 | 动作 | 说明与迁移 |
| --- | --- | --- | --- |
| A7 `BufferPoolGauges` 声明 + `BufferPoolGaugeCollector` + `register_buffer_pools` | `crates/hammer-runtime/src/data_plane/buffer_stats.rs`（新文件） | 新增 | 池条目的 owner 是建立池的 runtime（H12）；放在 `buffer_pool.rs` 旁即"业务文件里有业务的 stats"。没有它，池没有 stats 面（H14） |
| `data_plane/main.rs:36` | 模块声明 | 修改 | 新增 `mod buffer_stats;`（与既有 `mod buffer_pool;` 同处） |
| A8 image 列表 | `crates/hammer-runtime/src/lib.rs:31-41` | 修改：`stats_registrations` 增加 `data_plane::buffer_stats::__STATS_COLLECT_REGISTRATION_REGISTER_BUFFER_POOLS`（排在 `Sys` 之后、与其它登记项并列） | 登记项是 discovery 的唯一入口（ADR-0021 6.5）；顺序 = 执行顺序（H11） |
| 既有 `/sys`、`/mem` owner 代码 | `config/stats.rs` | **不修改**（除 ADR-0021 已列项） | 本家族不碰 `Sys`、不碰堆条目、不改 `statseg-collector-process`（H10） |

### 6.4 hammer-component-macros：`path` 占位符与参数化 `install`（A6，需批准）

```rust
// crates/hammer-component-macros/src/lib.rs：`#[derive(Stats)]` 新增能力（D1.3）

// 模板里的 `{name}` 占位符 = 该声明的 install 参数（每个不同的占位符一个 `&str` 形参）。
// 生成：
impl BufferPoolGauges {
    pub fn install(
        segment: &::hammer_stats::StatsSegment,
        pool_name: &str,
    ) -> ::hammer_stats::StatsResult<Self>;
}
```

| 项 | 语义 | VPP 对应 |
| --- | --- | --- |
| `path` 里的 `{name}` 占位符 | 模板在 install 时用实参填成完整目录名；不同占位符 → 不同形参；参数类型 `&str` | `vlib_stats_add_gauge ("/buffer-pools/%v/cached", bp->name)`（`buffer.c:944`，V7、V8）——VPP 同样是"格式串 + 实参" |
| 由此生成 `install(&StatsSegment, <占位符…>) -> StatsResult<Self>` | 一次 install 建立该声明里带同一组占位符的全部条目，返回持有各自 `DirectoryIndex` 的值 | VPP 一次 `vlib_buffer_pool_create` 后由调用方决定名字；Hammer 由池自己的名字填 |
| 参数化声明不进 image | 没有池名就无法 install，实例化由 owner 的采集登记项驱动（D1.2） | VPP 的"建条目 + 登记采集者"也在同一个循环里（V7） |

**为什么需要（A6 的理由）：**池数是运行期事实（H13），而 `#[derive(Stats)]` 的既有属性
（`path` 字面量 + ADR-0021 的 `columns`/`symlinks`）只描述一条静态条目；没有这条能力，
要么回到命令式 `add_gauge`（用户已否决），要么放弃 VPP 的每池独立名字（D1.3 表格）。

**不进入设计：**占位符不表示"任意表达式"（只是 install 形参名，宏不推断类型、不做类型转换，
`&str` 之外的类型不在本次范围）；不给参数化声明生成 image 项或全局实例；不加"家族宏"；
不用占位符承载任何家族语义（宏看到的是 `/buffer-pools/{pool_name}/cached` 这样一个字符串模板，
不理解它）。占位符之外的 `path`/`columns`/`symlinks` 语义与 ADR-0021 6.5 完全一致。

### 6.5 client（外部仓，A9）

分层与读取规则见 D7；本仓不新增客户端类型，也不新增协议类型。以下是跨仓契约的候选签名：
由 `netsystem-client` 仓批准与实现。`StatsReader`、`StatsProvider`、`StatsClient::report::<P>()`
三个名字与语义与 ADR-0021 §6.4 完全相同，本家族只加一个 provider 与一个 report 类型，
不改底层交互。

```rust
// hammer-stats-client（外部仓的 Rust binding）
// StatsReader / StatsProvider / StatsClient::report::<P>() 来自 ADR-0021 6.4，本家族不重复定义。

pub struct BufferPoolStatsProvider;

/// 一个池的三个已发布计数；单位是缓冲区个数，不是字节（D7 规则 2）。
pub struct BufferPoolUsage {
    pub name: String,
    pub cached: u64,
    pub used: u64,
    pub available: u64,
}

pub struct BufferPoolStats {
    /// 目录里出现的池（今天 = `BufferMain` 的池集合，H4）。
    pub pools: Vec<BufferPoolUsage>,
}

impl BufferPoolStats {
    /// 按池名（`default-numa-<n>`）查找；不解析名字里的数字。
    pub fn pool(&self, name: &str) -> Option<&BufferPoolUsage>;
}

impl StatsProvider for BufferPoolStatsProvider {
    const PREFIX: &'static str = "/buffer-pools";
    type Report = BufferPoolStats;
    fn report<R: StatsReader>(reader: &R) -> Result<BufferPoolStats, Error>;
}
```

**客户端仓的验收边界：**

- `StatsClient` 不加任何家族方法：`buffer_pool_usages`、`pool_count` 这类访问器属于 provider，
  加回底层就是把家族语义塞进机制层（ADR-0021 §6.4 的同一验收）。
- provider 只经 `StatsReader`（`names()` + `read()`）：每个池的三个 gauge 各读一次，不逐列读基
  条目、不跨条目拼"快照"、不保存上一轮基线（三个值都是瞬时 gauge，没有累计量、没有速率，D8）。
- `BufferPoolUsage` 只承载三个发布值；total 由调用方相加（D7 规则 5），provider 不提供也不缓存。
- 名字必须恰好是 `/buffer-pools/` + 池名 + 度量名两段；池名不做格式解析（不把
  `default-numa-` 之后的数字当索引，也不假设该前缀存在）；池名或度量名多一段/少一段的条目不进
  池集合。类型不是 `Gauge`、或池缺三个度量之一 → 客户端仓的类型化错误（池名 + 度量名 + 实际类型），
  不 panic、不静默补 0。
- 目录里没有池（`BufferMain::init` 拒绝空池，H4，因此只在进程未完成启动时可见）时 `pools` 为空
  向量，是成功结果不是错误。
- 新增家族（node 计数、session）只加 provider 与 report，不改 `StatsClient`、不改协议 crate。

| 项 | 位置 | 动作 | 说明 |
| --- | --- | --- | --- |
| A9 `/buffer-pools` provider | `netsystem-client`（外部仓） | 新增 | 候选类型见上（`BufferPoolStatsProvider`/`BufferPoolUsage`/`BufferPoolStats`）；只读三个 gauge；`StatsClient`/协议/`MetricValue` 不变（D7） |
| fixture | `crates/hammer/tests/stats_segment_mapping.rs` | 修改（新增样例） | 本仓只提供固定样例：名字在 `/buffer-pools/` 下、类型 `Gauge`、取值 = 个数；跨仓契约的验证点 |

### 6.6 全部动作总表

| 文件 | 类型 / 方法 / 字段 | 动作 |
| --- | --- | --- |
| `crates/hammer-core/src/buffer/main.rs` | `BufferPoolUsage` + `used()`（A1） | 新增 |
| `crates/hammer-core/src/buffer/main.rs` | `BufferPool.name: String`（A3） | 新增字段 |
| `crates/hammer-core/src/buffer/main.rs` | `BufferMain::pool_count`/`pool_name`/`pool_usage`（A2） | 新增 |
| `crates/hammer-core/src/buffer/main.rs:33-45` | `BufferPool.cached_counts: Box<[AtomicUsize]>`（A4） | 新增字段 |
| `crates/hammer-core/src/buffer/main.rs:48-56` | `BufferThreadCache.len: usize` 删除（A4） | 修改 |
| `crates/hammer-core/src/buffer/pool.rs:272-279,283,342-347,351` | 缓存长度的四个写点（`Relaxed` 存回） | 修改 |
| `crates/hammer-core/src/buffer/pool.rs:79-86` | `cached_free_buffers` 的读（`.load(Relaxed)`） | 修改 |
| `crates/hammer-stats/src/buffer_pools.rs`、`lib.rs` | `BufferPoolGauge`、`update_pool_gauge`、`pub mod buffer_pools`（A5） | 新增 |
| `crates/hammer-component-macros/src/lib.rs` | `path` 占位符 + 参数化 `install`（A6） | 新增（需批准） |
| `crates/hammer-runtime/src/data_plane/buffer_stats.rs`、`data_plane/main.rs` | `BufferPoolGauges`、`BufferPoolGaugeCollector`、`register_buffer_pools`、`mod buffer_stats;`（A7） | 新增 |
| `crates/hammer-runtime/src/lib.rs:31-41` | `stats_registrations` 增加一条登记项（A8） | 修改 |
| `crates/hammer/tests/stats_segment_mapping.rs` | `/buffer-pools/*` 读取样例（A9 的服务端侧 fixture） | 修改 |
| 外部 `netsystem-client` | `/buffer-pools` provider（A9） | 新增（跨仓） |
| `crates/hammer-stats/src/{lib.rs,segment.rs}`、`config/stats.rs`、`process.rs`、`main_loop.rs` | 机制、`Sys`/`/mem` owner、`statseg-collector-process` | **不修改** |

## 7. 迁移与执行顺序

| 阶段 | 范围 | 完成证据 |
| --- | --- | --- |
| M0 hammer-core | A1/A2/A3/A4：`BufferPoolUsage`、池名、三个访问器、每线程 `cached_counts` 槽与四个写点 | 真实池上 `pool_count`/`pool_name` 与 NUMA 节点一致；`pool_usage` 的三个值与已知分配/归还一致；采样不分配、不借别的 worker 的槽、不碰 `RefCell`；`used()` 在并发采样下不回绕 |
| M1 hammer-stats | A5：`buffer_pools` 家族模块 | 换算单测（三个事实 → 三条 gauge 的值）；`cargo check -p hammer-stats` 在没有 runtime 的情况下通过 |
| M2 hammer-component-macros | A6：`path` 占位符 + 参数化 `install` | 编译期用例：同一声明 install 两次得到不同名字的两组条目；缺占位符实参是编译错误；既有 `path` 字面量声明（`Sys`、`/mem`）行为不变 |
| M3 hammer-runtime | A7/A8：owner 声明、采集者、登记项、image 一条 | 两个池的进程里 `/buffer-pools/default-numa-{0,1}/{cached,used,available}` 存在且类型是 `Gauge`；跑一轮后值与采样一致；登记顺序与 image 一致；node 与机制代码里没有 buffer 概念 |
| M4（跨仓） | A9：`BufferPoolStatsProvider` + fixture | 客户端对 fixture 与真实映射得到相同 report（池集合、三个值、单位是个数）；缺度量/类型不符给出类型化错误；`StatsClient` 公共面不出现家族方法 |

**前置顺序（硬约束）：**M1–M3 依赖 ADR-0021 的机制（`Collector` trait、`register_collector`,
无锁轮次、条目级写、`path` 表达式）与它的 M1/M2。ADR-0021 未落地前，本文只作为设计存在；
实施时先落地 ADR-0021，再按 M0 → M3 顺序做，M4 与外部仓协调。

## 8. 验证矩阵

| 范围 | 触发 | 期望 |
| --- | --- | --- |
| 采样正确性（M0） | 在 N 个池上分配 k 个 buffer、再归还 | `cached + available + used == buffer_count`；分配后 `available` 或 `cached` 减少、归还原路返回；数值与池内真值一致 |
| 并发采样（M0） | 一个 worker 持续分配/归还，主线程反复 `pool_usage` | 无数据竞争（`Relaxed` 原子）；`used()` 不回绕、不超过 `buffer_count`；不借 worker 的 `RefCell`（代码层面无该调用） |
| 声明与目录一致（M3） | 启动 2 个 NUMA 池 | 6 条 `/buffer-pools/<pool>/{cached,used,available}`，类型全部 `DirectoryType::Gauge`，名字与 `pool_name` 一致 |
| 轮次（M3） | 跑满一个 `update_interval` | 三个值变成采样值；登记顺序 = image 顺序；轮次里只写自己那条 gauge、不取段锁 |
| 机制/业务边界（M2、M3） | 单独 `cargo check -p hammer-stats`；检查公共签名 | 机制 crate 在没有 buffer 声明/采集者时独立编译；`StatsSegment`/`StatsMain` 不出现 `BufferPool*`；`collect()` 与 node 里没有池名、池下标或家族调用 |
| 每池独立（M3） | 两个池各自分配/归还不同数量 | 两个池的值互不影响；每个池三条登记指向自己的条目 |
| 启动期失败（M3） | 注入一个 install 失败（例如重复池名） | 启动以 `Err` 结束：没有 worker、没有轮次、没有可读到的半家族；不写逐池回滚 |
| 客户端读取（跨进程，M4） | 独立进程只读映射：`names()`、读三个 gauge | 名字/类型/值正确；池名与目录一致；没有 symlink 要解析；畸形映射在解引用前拒绝 |
| 客户端 provider 形状（M4） | fixture 里把一个池的 `available` 换成 `Simple`，再删掉一个度量 | 两条都得到类型化错误（含池名与度量名），不 panic、不补 0；三个 gauge 不被当字节；`/buffer-pools/<pool>` 裸条目缺失时不臆造 |
| 客户端 provider 分层（M4） | fixture 与真实映射各跑一遍 provider | 两份 report 相同；`StatsClient` 公共面不出现家族方法；新增家族只加 provider |
| 热路径（M3） | release/LTO 下分配/归还基线与汇编 | 缓存长度的 Relaxed store 不造成可测回归；若可测，降频方案单独批准（D3） |
| 布局 | 编译期 size/align 断言与真实映射 fixture | `STAT_SEGMENT_VERSION` 仍为 2；新旧条目共用既有布局 |

测试只在最终 pre-commit gate 执行，遵循仓库测试时机规则；不做源文本断言。
本轮没有执行任何测试、fixture 或性能测量。

## 9. 审查结果与未决项

**已核实：**VPP 的 buffer pool stats 是**三个独立 gauge**，名字 `/buffer-pools/<pool>/{cached,used,available}`、
池名 `default-numa-<n>`、每池三条登记、`private_data` = 池下标，采集函数只做"解析自己那个池 →
写 `d->entry->value`"（V5–V8、V13、V14）。这三个值来自池自己的三个事实（`n_buffers`/`n_avail`/
`Σ n_cached`，V1–V3），Hammer 池里同样有这三个事实（H1），但每线程缓存长度今天只能被拥有它的
worker 借到（H3），所以本设计把它作为池上、与 `workers` 的 `RefCell` 并列的每线程原子槽（D3）
——这与 VPP 把 `n_cached` 放在池拥有的 `bp->threads[]` 槽里同构（V1），差别只在 VPP 用普通
`u32` + 池锁求和（V4），Hammer 用原子槽 + 无锁 Relaxed 求和。目录侧不需要新原语（gauge 已存在，
H9）；机制（`StatsSegment`/`StatsMain`）、
`Sys`/`/mem` owner、`statseg-collector-process` 都不改（H10、H11）；登记以 owner 模块里
**一条 `#[derive(Stats)]` 参数化声明 + 一个采集者类型 + 一条 `#[stats_collect_registration]` 登记项**
参与，池名与三个值的来源都在 `hammer-core`（H7）。唯一的新宏 surface 是 `path` 占位符与参数化
`install`（A6），它表达的正是一个运行期集合（池）按名字实例化同一条声明——VPP 用
`"%v" + bp->name` 的格式串做同一件事（V8）。

**与 VPP 的有意差异：**

| 差异 | VPP | Hammer | 理由与回归点 |
| --- | --- | --- | --- |
| 采集者数量 | 三个函数（`buffer_gauges_collect_{cached,used,available}_fn`），每池三条登记 | 一个类型 `BufferPoolGaugeCollector` + `gauge` 选择器，仍每池三条登记 | 用户明确要求"一个采集者类型，只是变量不一样"；语义（每条登记写一条 gauge）不变（V5） |
| `cached` 的求和 | 池锁下求和（`n_cached` 是普通 `u32`） | 池上每线程 `cached_counts` 原子槽 + Relaxed 求和，不取池锁、不碰 `RefCell` | Rust 不允许数据竞争；统计值允许迟一拍；轮次不因此变慢（D3） |
| `available` 的读 | `bp->n_avail` 是普通字段，不取锁 | 读 `free` 链长度必须持既有 `Spinlock<Vec<u32>>` | Hammer 的 free 链是受锁容器；临界区只是 `len()`（H5）。这是轮次里唯一的锁，且它属于池而不是 stats |
| 瞬时不一致 | `used` 用 C 的 `u32` 回绕 | `saturating_sub` 取 0 | 统计值不应回绕成天文数字（D2） |
| 空池/缺失池 | 登记跳过 `n_buffers == 0`，采集取不到池就静默返回 | 不复制这两个分支：池集合启动期冻结、`init` 拒绝空池（H4），不可达即 bug | 与 ADR-0021 轮次里 `expect("…live directory entry")` 同一条错误规则 |
| `threads` 槽 | 随线程数扩容（V12） | `BufferMain::init` 按 `worker_count + 1` 一次建立，进程内不变 | Hammer 没有运行期 worker 数变化的路径（H4） |
| 目录类型 | 三个 `STAT_DIR_TYPE_GAUGE` | 三个 `DirectoryType::Gauge`（=9） | 等价类型（V15、H9），不是差异，只为对齐出处 |

**尚需决定/验证：**

1. **A6 的最终语法**：`path` 里的 `{pool_name}` 占位符（本设计）与"显式声明 install 参数的
   属性"（如 `#[stats(instances = [pool_name: &str])]`）之间选一个。若选后者，占位符与参数
   声明必须一致，宏要报不一致的编译错误；两种写法都只允许 `&str`。
2. **每线程 `cached_counts` 槽的写入粒度**：本设计把每次长度变化的最终值 Relaxed 存回
   （与 VPP 的逐次写一致）；备选是只在 batch 边界发布（每 32 个索引一次，代价更低但 gauge
   最多滞后一个 batch）。需要 M0/M3 的基线与汇编决定。
3. **重复 NUMA 节点**：`BufferMain::init` 不去重（H4），生产调用点已去重（H13）。当前选择是
   "靠登记期的条目重名错误终止启动"，不新增 `DataPlaneError` 变体；如果要更早失败，需要在
   `BufferMain::init` 里加一条校验（单独批准）。
4. **`total` 是否要发布**：本设计不发布（V16）。如果客户端/CLI 需要"一次读到的 total"，
   应当由命令组合（三个值相加）或新增 gauge 解决，后者要单独批准。
5. **CLI/命令**：`show buffer-pool` 一类的输出属于 Module-owned ctl（CONTEXT），本文只保证
   数据来源；命令的请求/回复与格式化器由 runtime 的对应 owner 另行设计。
6. **客户端 provider 的落地节奏**：本 ADR 已固定名称与语义（`BufferPoolStatsProvider`、
   `BufferPoolStats`/`BufferPoolUsage`、D7 的读取规则与 6.5 的候选签名），实现与发布顺序需要与
   `netsystem-client` 仓协调。客户端仓可以按自己的风格改名，但"底层不认识家族、provider 只经
   `StatsReader`、编译期单态化"三条边界不随名字变化。
7. **`BufferPoolUsage` 的整数域**：本设计用 `u64`（目录列的类型）。若以后池容量/索引改用更窄
   类型，换算点只有 `pool_usage` 一处，不需要改声明或采集者。

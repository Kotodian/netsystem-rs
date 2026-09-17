# ADR-0021: Mem Stats、System Stats 的目录契约与 Stats Client 集成

Status: proposed — 设计草案，尚未批准实现（2026-09-16 第四版：采集者表行改为
`Box<dyn Collector>`，**每个采集者是一个自己实现 `Collector` 的具体类型**；声明面与
无锁写路径按 vendored VPP 重写，见下）
Date: 2026-09-16

> **本版修订（采集者表行对齐 VPP）。**第一版把采集者表写成
> `SpinLock<Vec<Collector>>`、回调签名写成 `fn(DirectoryIndex, u32)`、让每个
> owner 的采集代码自己 `stats_main.segment.lock()` 再调 `mem::update_mem_usage
> (&mut segment, …)`、`/mem` 条目与 `/sys` 新条目由自由函数命令式创建、并把
> “更新”写成声明宏的属性（`collector = <fn>`），也没有把轮次末尾的
> `heartbeat++` 写成机制自己的步骤。这九处都与 VPP 不符，本版按 vendored 源码
> 改写（细节与影响清单见 **D9**）：
>
> 1. **表无锁。**VPP 的 `sm->collectors` 是**进程私有 pool**（注释原文
>    “internal, does not point to shared memory”，`stats.h:58-62`）；注册
>    （`stats.c:589-604`）与轮次遍历（`collector.c:137-146`）都不取锁，段锁
>    `stat_segment_lockp` 只保护共享目录。Hammer 的采集者表因此不放进任何
>    `SpinLock`，也不放进被段锁保护的 `StatsSegment`（V27–V29）。
> 2. **表行就是 VPP 的 `vlib_stats_collector_t`：一个采集者类型 = 一行。**VPP 的
>    表行是 `{fn, entry_index, vector_index, private_data: u64}`，回调是
>    `void (*) (vlib_stats_collector_data_t *)`——`(fn, private_data)` 这一对就是
>    一次带实例状态的虚调用（`stats.h:31-39,51-57`）。Hammer 让**每个采集者自己
>    实现 `Collector`**：方法体是 `fn` 那一格，类型自己的字段（`heap`/`threads`/
>    `api_main`）是 `private_data` 那一格，`entry_index()` 是表行的身份，`entry`
>    仍由机制解析后交给采集者（V27、D9.2）。没有函数指针表项、没有 `u64` 下标、
>    没有 `&'static ()` 擦除、没有额外的数据参数、没有手写 vtable；这是本仓库
>    "生产代码不用 `dyn`"规则的一处明确例外（理由与备选见 D9.2）。
> 3. **采集者不碰 `StatsSegment`。**VPP 把**该条目的地址**交给回调，回调只写
>    自己那几列单元格，既不取段锁也不查全局。Hammer 照此：轮次不取锁，把该条目
>    的**共享借用**作为 `Collector::collect` 的参数交给采集者（采集者只写 payload
>    单元格，不改 header，所以不需要 `&mut`）；采集者只看见它自己那一条条目，不再
>    出现 `stats_main.segment.lock()` 与 `update_mem_usage (&mut segment, …)`
>    （V28、V30）。
> 4. **轮次末尾无条件 `heartbeat++`。**`collector.c:149-150` 是
>    `do_stat_segment_updates` 的最后一条语句，采集者表为空时同样执行；它形同上
>    一条循环里的采集者——就是"取 heartbeat 条目 → 写值 +1"，不是它自己的锁
>    步骤、不是专用函数、不是采集者的工作，只是机制在轮次里的最后一句
>    （V14、V28；D9.4）。
> 5. **注册语义落到 `#[derive(Stats)]`：条目、行宽、别名。**`/mem/<heap>`
>    （1 行 × 7 列 + `total`/`used`/`free` 三个别名）与 `/sys` 的新条目都写成
>    owner 模块里的声明，对应 VPP 注册点里的 `vlib_stats_add_counter_vector`
>    → `vlib_stats_validate` → `vlib_stats_add_symlink` 三连
>    （`provider_mem.c:44-63`）；宏为此扩展“绝对 `path`、行宽、`symlink`”
>    三个属性（新增 surface 需批准，6.5）。`mem::register_mem_heap` 删除
>    （条目不再由自由函数命令式创建），`hammer-stats::mem` 只留 VPP 的列枚举
>    与唯一的七列映射。
> 6. **注册与更新各归其位：`#[derive(Stats)]` 只有注册语义，采集者类型只有更新语义。**
>    `#[derive(Stats)]` 声明条目名、行宽与别名（VPP 注册点的
>    `add_counter_vector` → `validate` → `add_symlink` 三连，
>    `provider_mem.c:52-63`）；采集者是每轮写单元格的那**一个具体类型**（VPP 表行
>    里 `fn` 与 `private_data` 那两格合成一个类型），由 owner 的
>    `#[stats_collect_registration]` 项调一次 `StatsMain::register_collector` 挂进
>    轮次，对应 VPP 同一注册点的最后一句
>    `vlib_stats_register_collector_fn (&r)`（`provider_mem.c:64-70`，V30）。上一版把
>    `collector = <fn>` 与 `private_data = <expr>` 写进 `#[derive(Stats)]` 的属性，
>    等于让“注册”这个宏去表达“更新”，本版删除这两个属性：宏不认识采集者，采集者
>    也不建条目（A12、D9.5）。
> 7. **宏名只表达它真做的事：`#[stats_registration]` 改名 `#[stats_collect_registration]`。**
>    改名后的项生成 `__STATS_COLLECT_REGISTRATION_<函数名大写>`，把 owner 的
>    **采集者对象**挂进轮次；`#[derive(Stats)]` 生成的是条目声明项
>    `__STATS_REGISTRATION_<结构体名>`。两者都列进同一个 image 列表，因此名字必须
>    一眼可分：前者是"谁每轮更新"，后者是"目录里有什么"（A12、6.5）。
> 8. **一次性写值不是采集登记：`/sys/num_worker_threads` 只有声明。**该条目已由
>    worker thread 域的 `#[derive(Stats)]` 声明创建，VPP 也是在 `vlib_thread_init`
>    里 create 完就在同一函数 set（`threads.c:223-225,404`）。因此本版不再给它
>    一条采集登记项：值由该域在自己的生命周期点（`start_workers`）走 A2 的
>    "按索引写一个值"写一次；采集者表里只有每轮真正要更新的条目
>    （5 个堆 + 2 条 per-worker vector = 7 条）（A6、6.5）。
> 9. **轮次与 setter 都不取锁：写共享目录本来就是裸写。**VPP 里只有"改目录结构"
>    才取 `vlib_stats_segment_lock`（`vlib_stats_validate` 会 expand 时、
>    `remove_entry`、node 计数器重建，`stats.c:471-486`、`collector.c:61`）；
>    取值写一律无锁：`vlib_stats_set_gauge`/`vlib_stats_set_timestamp` 就是
>    `sm->directory_vector[index].value = value;`（`stats.c:263-269`、`:283-289`），
>    `do_stat_segment_updates` 里采集者的单元格写与末尾的 heartbeat 也是裸写
>    （`collector.c:131-151`）。上一版把"写一个值"设计成 `&mut DirectoryEntry`、
>    又把发布后的段包进 `SpinLock`，于是轮次被迫持锁——两处都改：写入口取
>    `&self`、经映射地址写单元格与标量；`StatsMain` 直接拥有段（不是
>    `SpinLock<StatsSegment>`）；目录结构变更取 `&self` + 段锁一把（段锁的归属、
>    保护对象与全部使用点见 D10；A2）。
>
> §4 D1/D4、§6.2、§6.3、§6.5、§6.6、§7、§8、§9 中与这九条冲突的表述已在本版
> 改写；VPP 的 `private_data` 那一格在 Hammer 由**采集者对象自己的字段**表达
> （VPP 是 `u64` 实例身份）：`Box<dyn Collector>` 的 fat pointer 就是它的
> `(fn, private_data)` 对，规则见 D9.2。
## 1. 范围与结论

**用户要求：**阅读 vendored VPP 源码和现有 stats 代码，设计 mem stats 与
system stats，并设计 stats client 对这两个家族的集成；先出设计 ADR，不写代码。
本次只新增本文，不修改任何实现，不执行测试，不创建 issue，也不把 ADR-0019 中
拟议的 API 当作本文新增 API 的批准。

**已核实的项目事实：**

- Hammer stats 段已经具备这两个家族所需的目录能力：gauge/scalar、simple counter
  vector、symlink、`validate` 扩行列、`set_gauge`/`set_timestamp`；目录里目前
  只有三个固定 `/sys` scalar（heartbeat、last_stats_clear、boottime）。这些能力
  按"目录机制"使用：VPP 的 mem provider 也只调用通用操作，不扩展 stats 段的
  类型（V3、V20、V21）。
- **轮次驱动已经是既有 process node（H12）：**`statseg-collector-process`
  （`#[process_node]`，静态项登记在 runtime 注册镜像的 `process_nodes` 里）由
  thread-zero `DataPlaneMain` 拥有，是**薄驱动**：设置一次 boottime，然后每轮
  "调用一次 `StatsMain::collect()` → 取 `update_interval` → sleep"。本设计保持
  这个形状，只把 `StatsMain::collect()` 补成 VPP `do_stat_segment_updates` 的
  对应物：**遍历 owner 登记的采集者，然后推进 heartbeat**（V14、V21、V26）。
  **具体家族步骤（有哪些堆、哪些 `/sys` 条目）既不进入 node，也不进入
  `StatsMain::collect()`**；`hammer-stats` 只提供通用机制（进程私有表 +
 条目的共享借用交给采集者 + 心跳）与 `/mem` 的列映射（D9）。不新增 node、
  不新增线程、不新增 Process 类型。VPP 的同名 node 也是这个形状：只调
  `do_stat_segment_updates` + `vlib_process_suspend`（V23）。
- **本次核实发现的既有缺陷（H10）：**`set_gauge`/`set_timestamp` 写入的是
  `entry_of_type` 返回的 `DirectoryEntry` **拷贝**，共享目录槽不变
  （`segment.rs:194-204,843-867`、`protocol.rs:448-451`）；现有两个写入者
  （heartbeat 与 boottime）因此都不会更新段。修复方式是把目录访问改成借用
  （读写都取 `&DirectoryEntry`，值经映射地址落盘；改目录结构的操作取 `&self` +
  段锁，见 D10）并让拷贝返回的 API 消失，
  **与 mem/system 家族在同一批改动里完成**，不设"先修写路径"的独立前置阶段
  （A2、M1）。
- **现有唯一表示"stats 堆装不下"的变体是 `StatsError::HeapExhausted`**
  （`lib.rs:99-103`，Display 在 `lib.rs:140-147`），4 个构造点都在
  `segment.rs` 的段内分配路径上（`:296,974,981,990`）；而现行段内分配是"调用点
  手工选堆"（`segment_heap.allocate`/`heap.allocate`），尽管 `activate()` 之后
  活动堆已经是段堆、普通分配本来就落在那里（H13）。既定决定是删除这个变体：
  段内分配统一为"窗口 + 普通分配"，耗尽走分配器的失败路径（D6、A9）。
- 没有任何 `/mem/*` 条目；`MemHeap` 只有 `size()` 和 crate-private 的
  `free_space()`，没有一次完整采样；`DataPlaneMain.main_loop_count` 是 worker
  私有的非原子 wrapping `u32`，其它线程不可读。
- VPP 的 mem provider **不进入 stats 数据结构，也不集中登记堆**：每个堆的 owner
  在**自己的注册点**调用一次 `vlib_stats_register_mem_heap(heap)`，provider 用
  堆名创建 `/mem/<heap>` 七列 counter vector 与 `total/used/free` 三个 symlink，
  再用 `vlib_stats_register_collector_fn` 把该堆的采集者挂进轮次（V3、V21、
  V26）。VPP 只登记 stat segment heap 与 main heap 两个堆，worker 的 thread
  heap 并不注册；注册哪些堆是 owner 的选择（V6）。Hammer 现有的五个堆
  （`main heap`、`stat segment`、api segment 的三个 region 堆，H14）按同一形状
  由各自的 owner 声明条目并登记采集者（注册与更新是两步，D9.5）；机制持有登记
  表并每轮遍历（D1、D9、A5、A8、A11）。上一版草案把"有哪些堆"写死在 runtime
  的一个结构体和 `init_stats_main` 的两行清单里，本版按 VPP 改成每个堆自己的
  声明 + 自己的采集者登记。
- VPP 的 system stats 由**业务层自己声明**：`threads.c` 自己创建
  `/sys/num_worker_threads` gauge 并调用通用 `vlib_stats_set_gauge`
  （V11、V20）；`/sys/vector_rate*` 与 `/sys/loops_per_worker` 是 stats owner
  自己的速率 collector（V12），不是通用机制的一部分。
- stats client（`StatsClient`/`MetricValue`）按 ADR-0020 第 5 节已迁出本仓，由
  外部 `netsystem-client` 仓库的 `hammer-stats-protocol` + `hammer-stats-client`
  拥有。本仓不得重新引入客户端实现，因此本文的客户端部分是**跨仓契约**，
  不是本仓的实现清单。

**结论（拟议，均需批准）：**

- **D0 家族边界：**定义 `/mem/<heap>`（内存堆采样）与 `/sys/*`（进程运行时计数器）
  两个家族。`/mem` 采用"一个 mspace 堆 = 一个条目"：现有五个堆（`main heap`、
  `stat segment`、api segment 的三个 region 堆，H14）各由自己的 owner 登记、
  自己更新，插件堆走同一条路径（A5、A11、D2）。它们复用现有 `DirectoryType`，
  共享布局不变，`STAT_SEGMENT_VERSION` 保持 2，不需要新的 `MetricValue` 变体。
- **D1 声明、机制与业务的边界（对"业务和 stats 底层耦合"的回答）：**三层各归
  其位。机制是 `hammer-stats` 的通用目录操作（`validate`、`add_symlink`、
  `set_gauge`/`set_timestamp`/`set_simple_counter`、`remove_entry`、`heap()`
  借出段自己的堆）、段内分配的活动堆窗口（窗口内普通分配，耗尽即崩溃），以及
  **采集者表与轮次**：表是进程私有的（无锁，`Vec<Box<dyn Collector>>`），轮次不取
  锁、把条目的共享借用交给采集者，最后推进 heartbeat（D9、A8）。`/mem`
  家族只留自己的列契约：VPP `stat_mem_usage_e` 的列常量与唯一的七列映射
  （`hammer-stats::mem::update_mem_usage (entry, usage)`），不是
  `StatsSegment`/`StatsMain` 的方法（A11、6.2）。业务是每个条目
  的 owner：`hammer-runtime` 声明 `main heap`、`stat segment` 与两条 per-worker
  `/sys` vector，`hammer-ipc`（镜像由 `hammer-service` 列出）声明 api segment
  的三个 region 堆，未来的插件声明自己的堆——**每个条目在 owner 模块里有两条
  项：`#[derive(Stats)]` 声明管注册（条目名、行宽、别名），该 owner 的**一个采集者
  类型 + 一条登记项**管更新（D9.5；VPP 的
  `vlib_stats_register_mem_heap` 也是同一个注册点里连续做这两件事，V30）；
  `init_stats_main` 里没有任何堆清单，由 stats init 的
  `run_stats_registrations()` 遍历所有 image 的声明执行（A5、A6、A11、D2），
  顺序 = image 列表顺序。`Sys` 的固定条目仍用同一个宏声明，`bootstrap` 只服务
  段预建的固定槽。写值操作（`set_gauge`/`set_timestamp`/`set_simple_counter`
  与条目级的 `set_simple_counter_cell`/`set_scalar`）
  **不返回 `Result`**：它们只按已发布的索引/行列写值，和 VPP 的 setter 一样没有
  错误路径（V20）；索引/类型/行列失配是自己模块的 bug，按断言处理。安装期操作
  （`add_*`/`validate`/`add_symlink`/`find`）继续返回
  `StatsResult`，因为重复名字、类型冲突与容量是真实可失败的输入。机制因此
  **不认识** `MemHeap`、runtime 状态、worker/thread index、任何一个堆名或
  `/mem` 的列序；每个 owner 只声明自己的条目与采集者。家族不是写死的
  步骤链，而是一组声明；`statseg-collector-process` node 每轮只调用
  `StatsMain::collect()`（A2、A5–A8、A11、H12）。
- **D2 mem stats：**一个堆 = 一个 1 行 × 7 列 simple counter vector +
  `total/used/free` 三个 symlink + 一个采集者；**注册语义（条目、行宽、别名）
  与更新语义（VPP 的 `fn` + `private_data` 两格）是该堆 owner 模块里的两条项，
  不是一条**：一条 `#[derive(Stats)]` 声明管注册，该堆那条 `register_collector`
  登记项管更新（对应 VPP `provider_mem.c` 注册点的两半，V3、V26、V30；
  `StatsSegment` 不增加 family 字段、不增加 family 方法，家族模块只留列常量与
  七列映射）。更新侧只有**一个采集者类型** `HeapCollector`，一个堆一个实例——
  VPP 同样是一个 `stat_provider_mem_usage_update_fn` 加每堆一次
  `vlib_stats_register_mem_heap`（V26、V30）。现在覆盖五个堆：
  `main heap`、`stat segment`、api segment 的三个 region 堆（H14）；采样操作由
  infra 提供（`MemHeap::usage() -> HeapUsage`）。加一个堆 = owner 加一条声明与
  一条采集登记项，机制与 `init_stats_main` 都不改（A5、A11、D9）。
- **D3 system stats：**保留现有三个固定 scalar；新增 `/sys/num_worker_threads`
  （gauge）、`/sys/main_loop_count_per_worker`（每个 Data Worker 一列的累计主循环
  次数）与 `/sys/loops_per_worker`（每个 Data Worker 一列的 loops/s，VPP 同名
  同义：200µs 采样窗口 + `K = exp(-1/20)` 指数阻尼，V12、V13）。窗口取 VPP
  **代码**的值（`main.c:1713` 的 `now + 2e-4`），不是 VPP 注释里写的 20ms
  （注释与代码不一致，见 V13 与 6.3 的常量说明）。`/sys/vector_rate` 与
  `/sys/vector_rate_per_worker` 需要每 worker 的 internal node calls/vectors
  计数（VPP `vlib_internal_node_vector_rate`，V12、V13），Hammer 现在没有这个
  计数（归 ADR-0019 M4 的 node 计数），本 ADR 不发布、也不用零值占位。
- **D4 更新模型：**owner 侧每线程状态（累计计数原子 + 主循环自己算出的阻尼速率
  发布值）+ owner 采集者回调按轮采样。worker 只更新自己的两个值；`/mem/*` 与
  `/sys` 列的唯一写者是登记表里的采集者（5 个堆采集者实例 + 2 条 per-worker
  vector 采集者，共 7 个采集者实例，由 3 个采集者类型服务：`HeapCollector`
  （每堆一个实例）、`MainLoopCountCollector`、`LoopsPerSecondCollector`；实例
  状态就是采集者类型自己的字段，VPP 用一个函数加 `private_data` 下标做同一件事，
  V26、V30、D9.2），它们由
  `StatsMain::collect()` 在 `statseg-collector-process` node 的每一轮按登记顺序
  调用：轮次不取锁，把**该条目的共享借用**交给采集者（VPP 的 `data.entry`，V28），
  采集者只写自己的单元格，不取锁、不查全局；轮次的最后一条语句无条件推进
  heartbeat（V14、V28）。worker 不缓存共享 stats
  单元格指针，扩容安全性与 ADR-0019 的 worker 行安装问题解耦。采集者与
  `StatsMain::collect()` 都不返回 `Result`：索引、类型与行列在安装期已经确定，
  运行期没有可恢复失败。
- **D5 速率策略：**发布 VPP 同名的 `/sys/loops_per_worker` 速率投影：值由
  worker 自己的主循环按 VPP 的窗口与阻尼常数计算（窗口 `now + 2e-4` = 200µs，
  `main.c:1713`；`K = exp(-1/20)`，`main.c:1974`，V13）；Hammer 逐字复制这一对值、
  不按 VPP 注释的 20ms 说法"修正"，owner 采集者每轮把发布值复制进列；累计计数
  `/sys/main_loop_count_per_worker` 仍然发布，是权威值。这一条修正 CONTEXT 的
  Runtime Statistic 契约（把 `_Avoid_: stored rate` 限定为"没有 owner、没有固定
  窗口、由查询临时计算的速率"），实施时同步修订该术语条目。`/sys/vector_rate*`
  依赖 node 调用/向量计数（ADR-0019 M4），不在本次范围。
- **D6 生命周期与失败行为：**堆条目、行宽与别名由每个 owner 的声明在启动期
  一次建齐；`/sys` 的 gauge 与两个 vector 的
  行宽由各自的 owner 声明（worker thread 域与主循环）完成。它们都在 main loop 与 worker 启动之前完成，任何一步
  Err 都直接返回使启动失败，此时不存在能读到目录的客户端，因此不需要回滚步骤；
  运行期也不会出现指向不存在条目的轮次步骤。api segment 的 region 堆在退出路径
  上会 unmap，所以它们的采样先检查映射存在性（D2）；`MemHeap::destroy` 与已登记
  的五个堆无关。
- **写路径修正（H10/A2）与固定槽顺序（H11）不是前置阶段：**scalar/gauge/单元格
  的写路径在同一次改动中重建（借用化 + 删除拷贝返回），目录顺序要求由 M2 的
  注册顺序满足；两者都是本次 M1/M2 交付物的一部分（第 7 节）。
- **D7 client 集成：**客户端内部分三层，与服务端 D1 同构：`StatsClient` 是
  **底层交互**（连接、只读映射、epoch 重试、`names()`/`read()` 返回原始
  `MetricValue`），不认识任何 family；family 语义由 `StatsProvider` 实现
  （`MemoryStatsProvider`、`SystemStatsProvider`）翻译成 typed report；
  格式化与命令由调用方组合。provider 只依赖 `StatsReader` 能力 trait
  （`StatsClient` 是唯一生产实现），泛型静态分发，不给 `StatsClient` 加家族
  方法、不加 `dyn`、不加注册表。不新增协议类型、不改变 epoch/in_progress
  契约。
- **D8 范围外：**`/sys/node/*`（ADR-0019 M4）、`/sys/session/*`（session 插件）、
  `/sys/vector_rate*`（随 ADR-0019 M4 的 node 计数落地）、SVM/SSVM 堆、Binary API
  memory ctl 命令、客户端仓的具体代码。

本次审查基线：

- Hammer HEAD：`3e038726bc6f778316ebf904bc20a965cf369c18`，工作区干净。
- `third_party/vpp` HEAD：`629fe2764bd997189fedd2d98cbe8dc9189c1ec3`，无修改。
- 相关契约：CONTEXT.md 的 Stats Metric、Stats Owner、Runtime Statistic、
  Statistic Clear Baseline、Show Diagnostic、Module-owned ctl；ADR-0012 的
  MemHeap；ADR-0019 的 stats segment、collector、只读客户端映射；ADR-0020
  第 5 节的 stats client 归属与协议 crate 拆分。

文中"事实"指本次源码核实；"拟议决定"都需要后续批准；候选 Rust 签名不是已实现
surface。VPP 是所有权与行为参照，C 的裸指针访问不能直接当作 Rust 安全证明。

## 2. VPP 源码证据

路径相对于仓库根，行号对应本文基线。

| 编号 | 源码 | 核实的行为 |
| --- | --- | --- |
| V1 | `third_party/vpp/src/vlib/stats/provider_mem.c:10-17` | 内存条目是七列：`STAT_MEM_TOTAL=0`、`USED`、`FREE`、`USED_MMAP`、`TOTAL_ALLOC`、`FREE_CHUNKS`、`RELEASABLE`。 |
| V2 | `provider_mem.c:26-39` | 采样函数一次调用 `clib_mem_get_heap_usage`，把七个结果写进 `counters[0][0..6]`；不分配、不取 stats 结构锁。 |
| V3 | `provider_mem.c:49-71` | 注册：`vlib_stats_add_counter_vector("/mem/%U", 堆名)`、`vlib_stats_validate(idx, 0, 6)`、三个 symlink `/mem/<name>/used`、`/total`、`/free` 分别指向列 1、0、2；collector 的 `private_data` 是该堆在 `memory_heaps_vec` 中的索引。 |
| V4 | `third_party/vpp/src/vlib/stats/init.c:59,69,81,89-90,121` | stat segment heap 名是 `"stat segment"`；共享映射建好后立刻在 stats 初始化内注册该堆。 |
| V5 | `third_party/vpp/src/vlib/threads.c:576-577`；`third_party/vpp/src/vppinfra/mem_dlmalloc.c:112-114` | main heap（名字 `"main heap"`）由 thread 初始化显式注册。 |
| V6 | `threads.c:650,795` | VPP 也为 worker 建 `thread_mheap`，但没有注册到 stats。注册是 owner 的选择，不是"所有堆自动发布"。 |
| V7 | `third_party/vpp/src/vppinfra/mem.h:246-267` | `clib_mem_usage_t` 的字段集合；`object_count` 与 `bytes_used_sbrk` 标注为不支持。 |
| V8 | `third_party/vpp/src/vppinfra/mem_dlmalloc.c:264-282` | 字段来源：`arena→bytes_total`、`uordblks→bytes_used`、`fordblks→bytes_free`、`hblkhd→bytes_used_mmap`、`usmblks→bytes_max`、`ordblks→bytes_free_reclaimed`（实际是 free chunk **个数**）、`keepcost→bytes_overhead`（实际是可 trim 的**字节**）；两个不支持字段恒 0。 |
| V9 | `mem_dlmalloc.c:555-559` | 堆名按原样输出（含空格），因此目录名形如 `/mem/main heap`。 |
| V10 | `third_party/vpp/src/vlib/stats/stats.h:16,22-31` | 段版本 2；固定 scalar 槽位 0/1/2 是 `/sys/heartbeat`、`/sys/last_stats_clear`、`/sys/boottime`。 |
| V11 | `third_party/vpp/src/vlib/threads.c:223-225,404` | `/sys/num_worker_threads` 是 gauge，值 `n_vlib_mains - 1`，即不含 main 的 worker 数。 |
| V12 | `third_party/vpp/src/vlib/stats/init.c:15-50,123-131` | registered collector：`/sys/vector_rate`（gauge，`private_data` 指向它）、`/sys/vector_rate_per_worker`、`/sys/loops_per_worker`（counter vector）；每轮 `validate(…, 0, n_threads-1)`；每线程值来自 `vlib_internal_node_vector_rate`（自上次 clear 的 vectors/calls）与 `vm->loops_per_second`，`/sys/vector_rate` 是 worker 的平均值。 |
| V13 | `third_party/vpp/src/vlib/main.c:1689-1714,1965-1975`；`src/vlib/main.h:403-419` | `loops_per_second` 由 worker 自己的主循环维护：每个采样窗口（`loop_interval_end = now + 2e-4`）算 `loops_this_reporting_interval / elapsed`，再按 `K = exp(-1/20) ≈ 0.95` 做指数阻尼，存进 `vlib_main_t`（`main.c:1965-1975` 的注释与常量写的是"每 20ms 采样一次、半衰期 1 秒"的意图，但同一文件 `:1713` 给的窗口边界是 `now + 2e-4`，即 **200µs**：**VPP 的注释与代码不一致**。Hammer 逐字复制代码里的值（200µs 窗口 + 同一个 `K = exp(-1/20)`），使 `/sys/loops_per_worker` 与 VPP 的读数可比；这是 VPP 侧事实，若将来 VPP 改正，按 6.3 的常量说明修订）；collector 每轮把这个 f64 直接写进 `counter_t`（u64）单元格，所以段里存的是**截断后的整数 loops/s**（`init.c:50-53`）。vector rate 是"每次内部 node 调用处理多少向量"（自上次 clear 的 vectors/calls），不是包速率。 |
| V14 | `third_party/vpp/src/vlib/stats/collector.c:131-151` | 一轮顺序：node counters → 注册的 collectors → `heartbeat` **加一**。heartbeat 是计数，不是时间戳。回调在轮次里按注册顺序逐个调用，每个回调拿到 `entry_index`/`vector_index`/`private_data` 的副本（`vlib_stats_collector_data_t`，`stats.h:31-39`）。 |
| V15 | `collector.c:161-176` | `/sys/node/names` 与 `/sys/node/{clocks,vectors,calls,suspends}` 只在 `node_counters_enabled` 时注册，并由同一 collector 周期更新。 |
| V16 | `third_party/vpp/src/vlib/stats/stats.c:472-525,590-604` | `validate` 负责扩行列；collector 注册进 stats main 的 pool，注册与目录操作同属控制面。 |
| V17 | `third_party/vpp/src/vpp-api/client/stat_client.h:22-46`；`stat_client.c:216-291,350-400` | 客户端把 scalar/gauge 统一按 `double` 返回；symlink 递归解析到目标，并可用 `index2` 只取一列，同时用 `via_symlink` 标记这是别名；`list` 用正则匹配名字。 |
| V18 | `third_party/vpp/src/vlib/stats/init.c:106-121` | 固定槽先落地：`foreach_stat_segment_counter_name` 把 heartbeat/last_stats_clear/boottime 的 name/type 直接写进预分配到 `STAT_COUNTERS` 的目录向量并发布 `shared_header->directory_vector`，之后才 `vlib_stats_register_mem_heap (heap)`。mem 家族因此从索引 3 起，固定槽索引不受 mem 注册影响。 |
| V19 | `third_party/dlmalloc/dlmalloc.c:4813-4819` | `mspace_mallinfo` 只做 `ok_magic` 检查后返回 `internal_mallinfo (ms)`，不取 mspace 锁；因此 `clib_mem_get_heap_usage`（V8）在 collector 中采样时与 worker 分配并发，读到的是"同一堆的一组字段"，不是加锁快照。Hammer 使用同一份 vendored dlmalloc（`crates/hammer-infra/build.rs:8-23`）。 |
| V20 | `third_party/vpp/src/vlib/stats/stats.c:263-269`；`stats.c:283-289` | `vlib_stats_set_gauge`/`vlib_stats_set_timestamp` 就是"按目录索引写一个值"的通用操作：直接写 `sm->directory_vector[index].value`，不认识调用者是谁、也不持段锁。写入者可以是 provider（`provider_mem.c` 的列）也可以是业务层（`threads.c:404` 的 gauge）。 |
| V21 | `third_party/vpp/src/vlib/stats/stats.c:590-604`；`stats.h:41-62` | collector 注册由通用函数完成：`vlib_stats_register_collector_fn` 把 `collect_fn`/`entry_index`/`vector_index`/`private_data` 追加到 `sm->collectors`。该 pool 属于进程内 `vlib_stats_segment_t`（`stats.h:58-62` 注释明确"internal, does not point to shared memory"），注册项里没有任何 family 类型或对象引用。 |
| V22 | `collector.c:61`；`stats.c:263-269,471-486` | 轮次本身不持有段锁：`update_node_counters`（一轮的第一步）需要改目录时自己取 `vlib_stats_segment_lock ()`，`vlib_stats_set_gauge` 完全不取锁，`vlib_stats_validate` 只在真的需要扩行列时才取锁（`will_expand`）。因此"未知回调不在 stats 锁内执行"是 VPP 的既有事实，不是 Hammer 的偏离。 |
| V23 | `third_party/vpp/src/vlib/stats/collector.c:173-190` | 轮次驱动是**一个 process node**：`stat_segment_collector_process`，节点名 `"statseg-collector-process"`，`VLIB_REGISTER_NODE (… .type = VLIB_NODE_TYPE_PROCESS)`。它先设 `STAT_COUNTER_BOOTTIME`，再 `while (1) { do_stat_segment_updates (vm, sm); vlib_process_suspend (vm, sm->update_interval); }`：家族采样（mem provider 的回调、node counters）与 heartbeat 都在这个节点的循环里发生。 |
| V24 | `third_party/vpp/src/vlib/stats/stats.c:63,155-180,420-455,470-525,648-651` | 段内对象从不由调用点"手工选堆"分配：`vlib_stats_set_heap()` 就是 `clib_mem_set_heap (sm->heap)`（`:63`），随后 `vec_alloc`/`vec_validate_aligned`/`clib_mem_alloc_aligned` 在**当时的活动堆**上分配，最后还原旧堆（`:155-180`、`:420-455`、`:470-525`）；ring buffer 的创建同样只有 `clib_mem_set_heap` + `clib_mem_alloc_aligned` 两步（`:648-651`）。窗口决定堆，窗口内的分配调用本身不带堆参数。 |
| V25 | `third_party/vpp/src/vppinfra/mem_dlmalloc.c:361-416,446-476`；`third_party/vpp/src/vppinfra/unix-misc.c:225-237` | 这条路径没有"分配失败返回值"可检查：窗口内用的 `clib_mem_alloc`/`clib_mem_alloc_aligned`（`:387-400`）与 `vec_validate_aligned` 扩容走的 `clib_mem_heap_realloc_aligned`（`:446-476`）都经 `clib_mem_heap_alloc_inline`（`:361-381`）并传 `os_out_of_memory_on_failure = 1`，`mspace_memalign` 返回空即 `os_out_of_memory()` → `os_panic()` → `abort()`。只有显式的 `*_or_null` 变体（`:402-416,433-447`）把 0 交给调用者，stats 段一个都没用。 |
| V26 | `third_party/vpp/src/vlib/stats/stats.h:31-47`；`provider_mem.c:10-17,26-39,49-71`；`stats/init.c:118-131`；`buffer.c:938-960`；`session.c:1995,2006-2010`；`devices.c:74,108` | 每个 provider 在自己的注册点同时完成"建条目 + 登记采集者"：mem provider 对**每个堆**调一次 `vlib_stats_register_mem_heap`（建 counter vector + `validate(0,6)` + 三个 symlink + `register_collector_fn`），并把堆在**自己** `memory_heaps_vec` 里的下标放进 `private_data`；`stats/init.c` 用一条登记同时喂 `/sys/vector_rate` gauge（索引放 `private_data`）与 `/sys/vector_rate_per_worker` vector；buffer pool 每个池三条登记（`private_data` = 池下标）。`collect_fn` 收到的是调用期构造的 `vlib_stats_collector_data_t`（`entry_index`/`vector_index`/`private_data`/已解析的 `entry` 指针），因此同一函数可以服务多个实例。`vector_index` 在整棵 VPP 树里只被存储与透传（`stats.c:600`、`collector.c:142`），没有任何采集函数读它；`private_data` 只被"有实例集合"的 provider 读（buffer pool / heap / gauge index）。 |
| V27 | `third_party/vpp/src/vlib/stats/stats.h:31-57` | 回调签名与两张表的字段：`vlib_stats_collector_data_t { entry_index; vector_index; private_data; entry }`；`typedef void (*vlib_stats_collector_fn_t) (vlib_stats_collector_data_t *)`；注册输入 `vlib_stats_collector_reg_t { collect_fn; entry_index; vector_index; private_data }`；已安装项 `vlib_stats_collector_t { fn; entry_index; vector_index; private_data }`。**回调只有一个参数**，位置事实都在这一个值里。 |
| V28 | `third_party/vpp/src/vlib/stats/collector.c:134-151` | `do_stat_segment_updates` 的确切形状：`pool_foreach (c, sm->collectors)` **不取任何锁**；每轮为每条登记构造 `data = { .entry_index, .vector_index, .private_data, .entry = sm->directory_vector + c->entry_index }`，调用 `c->fn (&data)`；循环结束后的**最后一条语句**是 `sm->directory_vector[STAT_COUNTER_HEARTBEAT].value++`——空表也执行，且它就在同一个函数里，不是另一条路径。 |
| V29 | `third_party/vpp/src/vlib/stats/stats.h:51-62`；`stats.c:586-604,263-269,283-289`；`collector.c:131-151` | `vlib_stats_segment_t.collectors` 上方的注释原文是 "internal, does not point to shared memory"；`vlib_stats_register_collector_fn` 直接 `pool_get_zero (sm->collectors, c)` 后填四个字段，**不取** `stat_segment_lockp`。段锁（`stat_segment_lockp`/`vlib_stats_segment_lock`）保护的是共享目录结构（`in_progress`/`epoch`/目录与名字表的增删改，`stats.c:471-486`），取值写一律不取：`vlib_stats_set_gauge`/`set_timestamp` 就是 `sm->directory_vector[index].value = value;`，`do_stat_segment_updates` 的单元格写与末尾 heartbeat 也是裸写。 |
| V30 | `third_party/vpp/src/vlib/stats/provider_mem.c:41-71` | mem provider 的注册体：`vec_add1 (memory_heaps_vec, heap)` → `add_counter_vector ("/mem/%U")` → `validate (idx, 0, STAT_MEM_RELEASABLE)` → 三个 `add_symlink` → `r.private_data = vec_len (memory_heaps_vec) - 1` → `r.collect_fn = stat_provider_mem_usage_update_fn` → `register_collector_fn (&r)`。**条目、别名、采集者在同一个注册点落地**，`private_data` 是 provider 自己集合里的下标；VPP 树内只登记两个堆——段堆（`stats/init.c:121`）与主堆（`vlib/threads.c:577`），api segment 的 region 堆是 Hammer 自己的扩展（H14）。 |
| V31 | `third_party/vpp/src/vlib/stats/init.c:106-121` | 三个固定 `/sys` 槽（`last_stats_clear`、`heartbeat`、`boottime`）的类型都是 `STAT_DIR_TYPE_SCALAR_INDEX`，由 `foreach_stat_segment_counter_name` 直接写进预分配目录；`/sys/heartbeat` 因此是**整数计数**，由轮次 `value++` 推进（V28），不是时间戳。 |
| V32 | `third_party/vpp/src/vlib/stats/stats.h:72`；`stats.c:11-52,141-194,239-244,343-364,481-524,624-682`；`collector.c:61-91` | 段锁的完整协议与用户：`stat_segment_lockp` 是段里的 `clib_spinlock_t *`（`stats.h:72`）；`vlib_stats_segment_lock`/`unlock`（`stats.c:11-52`）取 spinlock 后置 `shared_header->in_progress = 1`，放锁前先 `epoch++` 再以 release store 清 `in_progress`，并用 `n_locks`/`locking_thread_index` 支持同线程重入。取锁的是结构变更：新建条目并发布 `shared_header->directory_vector`（`:239-244`）、`remove_entry`（`:141-194`）、`validate` 的 `will_expand` 分支（`:481-524`，扩容分配就在锁内）、name vector 写（`:343-364`）、ring buffer 创建（`:624-682`）、process node 里重建 node 计数器（`collector.c:61-91`）。值写（`set_gauge`/`set_timestamp`，`:263-269`、`:283-289`）与轮次的单元格/heartbeat 写都不取这把锁。 |

## 3. Hammer 现状与差异

| 编号 | 当前 Hammer 证据 | 差异与后果 |
| --- | --- | --- |
| H1 | `crates/hammer-stats/src/segment.rs:184-206,349-372,486` | 已有 `add_gauge`/`set_gauge`、`add_timestamp`/`set_timestamp`、`add_simple_counter`、`add_symlink`/`rename_symlink`、`validate`（扩行列，失败可回滚）。mem/system 家族所需目录能力已具备。 |
| H2 | `crates/hammer-stats/src/protocol.rs:3,207-217` | `DirectoryType` 已含 `CounterVectorSimple`、`Symlink`、`Gauge`、`ScalarIndex`；名字上限 128 字节（含结尾 NUL，可用 127）。不需要新类型码。 |
| H3 | `crates/hammer-runtime/src/config/stats.rs:84-97`；`:99-121` | 现有唯一 `derive(Stats)` 聚合 `Sys` 只有 heartbeat/last_stats_clear/boottime 三个固定 scalar；既有 process node `statseg-collector-process` 只设置 boottime 并周期调用 `StatsMain::collect()`。 |
| H4 | `crates/hammer-stats/src/lib.rs:75-78`；`segment.rs:150-154` | `collect()` 目前只做 `heartbeat++`（注释声称等价于 `do_stat_segment_updates`，实际只是它的最后一步）；`StatsMain` 既没有采集者表，也没有 `register_collector` 入口（V21），所以任何 family 都只能硬编码进 `collect()`，或由调用者在 `collect()` 外面自己拼步骤。本设计按 VPP 补齐：新增登记表，`collect()` 成为 `do_stat_segment_updates` 的对应物（遍历登记 → heartbeat++），family 以登记参与（A8、V14、V21、V26）。 |
| H5 | `crates/hammer-infra/src/mem/mod.rs:590-600,858-866,949,955` | `MemHeap` 是固定 mspace 的 dlmalloc 堆；对外只有 `size()`/`name()`，`free_space()` 是 crate-private 且只取一个字段；`MemMain::main_heap()` 提供进程生命周期主堆；`MemMain.heaps` 已登记全部堆但没有遍历 API。 |
| H6 | `crates/hammer-stats/src/segment.rs:53-136`；`crates/hammer-runtime/src/config/stats.rs:148` | stats 段堆在 `StatsSegment::create` 中创建，名字来自 runtime 传入的 `"stat segment"`，与 VPP 同名。 |
| H7 | `crates/hammer-runtime/src/data_plane/main.rs:43`；`data_plane/worker.rs:21-23`；`main_loop.rs:137`；`thread_main.rs:191-193` | `main_loop_count` 是 worker 私有 wrapping `u32`，只在 `data_plane_main_loop` 内自增；线程间不可读。`ThreadMain` 在 configure 后提供 `worker_count()`，`thread_count = worker_count + 1`，线程 0 是 main thread，且**不**运行 `data_plane_main_loop`（`process.rs:399-431` 是 Tokio process loop）。 |
| H8 | `crates/hammer-runtime/src/config/stats.rs:145-176`；`crates/hammer-runtime/src/init.rs:209-225` | 启动顺序：`StatsMain::init` → `Sys::bootstrap` → `bind_listener` + FileMain → `run_stats_registrations`。所有目录变更都在 main thread、在 worker 启动（`start_workers` 是 main_loop_enter 函数）之前完成。 |
| H9 | `git show dbb0c862:crates/hammer-ipc/src/stats_client.rs`（删除前）；`docs/adr/0020-…md` 第 5 节 | 旧内嵌客户端只有 `connect/list/read -> MetricValue`。客户端与协议 crate 现归外部仓库；本仓不能新增客户端类型，只能定义共享内存契约与验证 fixture。 |
| H10 | `crates/hammer-stats/src/segment.rs:194-204,843-867,894`；`crates/hammer-stats/src/protocol.rs:385-390,448-451` | `set_gauge`/`set_timestamp` 在 `entry_of_type(...)` 的返回值上调用 `set_scalar_value(&mut self)`；`entry_of_type` 经 `entry()` 用 `directory().get(i).copied()` 返回 `DirectoryEntry` **拷贝**（`protocol.rs:385-390` 的 `#[derive(Clone, Copy)]`），所以写落在临时值上，共享目录槽不变。共享目录的实际写路径只有 `create_entry`/`grow_directory` 使用的 `directory_mut()`。后果：heartbeat 与 boottime 现状不会更新，D3 的 gauge 也拿不到正确语义。**根因是"读 API 返回所有权拷贝"这一形状，不是漏写一行**；修复必须删除这个形状（A2）。 |
| H11 | `crates/hammer-component-macros/src/lib.rs:283-317`；`crates/hammer-runtime/src/config/stats.rs:158` | `Sys::bootstrap` 用 `add_*` 逐项创建固定槽并断言返回的 `DirectoryIndex` 等于 `STAT_COUNTER_*`。因此任何 mem 目录项都必须排在 `Sys::bootstrap` 之后注册，否则固定槽会落到索引 3 起（VPP 用 V18 的预分配达到同一顺序）。这是 M2 的注册顺序约束，不需要新机制。 |
| H12 | `crates/hammer-runtime/src/config/stats.rs:99-121`；`crates/hammer-runtime/src/lib.rs:32`；`crates/hammer-runtime/src/process.rs:1-3,21-23` | 轮次驱动已经是**一个 process node**：`#[process_node(name = "statseg-collector-process")] fn stat_segment_collector_process`（`NodeKind::Process`，静态项 `__PROCESS_NODE_STATSEG_COLLECTOR_PROCESS` 登记在注册镜像的 `process_nodes` 里），由 thread-zero `DataPlaneMain` 拥有、由 thread-zero Tokio 调度。它是薄驱动（只设 boottime + 每轮一次 `StatsMain::collect()` + sleep），与 VPP 的 `statseg-collector-process`（V23）同名同形状；本设计**不改**这个循环体——升级发生在 `collect()` 里（遍历登记表，A8），mem/system 逻辑不进入 node，不新增第二个 process node、不新增 Process 类型。 |
| H13 | `crates/hammer-infra/src/mem/mod.rs:362-382,504-537,745-765,826-848,2007-2086` | 段堆分配的实际路由：`MemHeap::activate()`（`:745-756`）把当前线程的活动堆切到该堆，`ActiveHeap` guard 在 drop 时还原；全局分配器 `MemMain`（`#[global_allocator]`，`:2007-2086`）在拦截开启（`MemMain::init` 里置 1，`:380-382`）后把 `alloc`/`alloc_zeroed`/`dealloc`/`realloc` 路由到 `MemThreadMain::active_heap()`（未显式设置时回退 main heap，`:526-537`），内部就是 `(*heap).allocate(layout)`（`:2018-2023`）——与公开的 `MemHeap::allocate(&self, …) -> Option`（`:758`）是同一条 mspace 调用。所以 `activate()` 作用域内的普通分配（`Vec`/`HashMap`/`alloc::alloc`）就是从该堆出的。**但 `hammer-stats` 现在没用这个形状：**`add_ring_buffer`（`:294-296`）先 `activate()` 再手工 `segment_heap.allocate`（窗口因此只是把同一个堆又指定了一遍），`allocate_vector`（`:961-1000`）则在**没有窗口**的情况下用 `heap.allocate` 选堆（5 个调用点 `:234,428,533,562,809` 与 `allocate_string` 全靠它），而 `:340,529,679` 的 `activate()` 只包住释放路径。设计统一为"窗口决定堆、窗口内普通分配"（V24、V25）。 |
| H14 | `crates/hammer-infra/src/mem/mod.rs:351-353`；`crates/hammer-stats/src/segment.rs:108`；`crates/hammer-service/src/binary_api.rs:753-759`、`crates/hammer-ipc/src/binary_api/memory_shared.rs:753-766,866-867`、`crates/hammer-infra/src/svm/region.rs:760,771-776`；`crates/hammer-infra/src/svm/ssvm.rs:690` | 现有 mspace 堆至少 5 个：`main heap`（进程启动时创建）、`stat segment`（`StatsSegment::create` 用 runtime 传入的名字创建）、api segment 的 root region 私有堆与 api region 的私有/data 堆（region 由 `map_shared_region` 放进 `ApiMain.mapped_shmem_regions`，退出时 `unmap_shared_regions`），以及按 session 创建的 `ssvm heap`。它们都没有任何 stats 条目；堆名会重复（每个 region 都叫 `svm region`/`svm data`），所以 `/mem/<name>` 的名字不能直接取堆名。 |

由此得到四个可执行的设计目标：

1. mem stats 缺的是"一次完整采样（infra）+ 每个堆 owner 的登记（名字 + 自己的
   采样函数）+ 家族模块的 `/mem` 契约与机制侧的轮次遍历"，不是新目录原语、不是 stats 侧框架，
   也不需要把 `MemHeap` 搬进 `StatsSegment`；堆清单不是代码，是各 owner 的
   登记项集合（H14、A5、A11）。
2. system stats 缺的是"worker 数量与主循环计数的 owner 侧写入"，而 Hammer 的
   main thread 不跑 data-plane 主循环，不能照抄 VPP 的 `n_threads` 列语义。
3. 轮次缺的是"owner 登记入口"（H4），不是 family 步骤清单：现状里 family 要么
   硬编码进 `collect()`，要么由调用者在 `collect()` 外面拼一条链；按 VPP 补
   `register_collector` + 遍历登记表（V14、V21、V26），家族清单就不成为代码
   （A8、D1）。
4. 共享目录的写路径（H10）与固定槽顺序（H11）都不是独立前置阶段：写路径修正
   属于 M1 的机制改建（借用化 + 删除拷贝返回），顺序约束属于 M2 的注册顺序。

## 4. 拟议决定

### D0 家族边界与目录契约

| 家族 | 条目 | 类型 | owner（业务） | 更新者与节奏 |
| --- | --- | --- | --- | --- |
| mem | `/mem/<heap>` | `CounterVectorSimple`，1 行 × 7 列 | 每个堆的 owner：`hammer-runtime`（`main heap`、`stat segment`）、`hammer-ipc`（api segment 的三个 region 堆）、以后的插件 | 注册与更新是该堆 owner 模块里的两步（D9.5）：`#[derive(Stats)]` 声明条目（绝对 `path` + `columns` + 别名，对应 V30 的注册三连），该堆自己的采集者类型由 `#[stats_collect_registration]` 项经 `register_collector` 挂进轮次（对应 V30 的最后一句）；`StatsMain::collect()` 每轮调用它（node 只负责调用 `collect()`） |
| mem | `/mem/<heap>/total`、`/used`、`/free` | `Symlink` → 列 0、1、2 | 同上 | 由同一声明的 `symlinks` 建立（VPP `add_symlink`，V3），不单独更新 |
| sys | `/sys/heartbeat`、`/sys/last_stats_clear`、`/sys/boottime` | `ScalarIndex` | `hammer-stats`（heartbeat/boottime）与未来的 clear baseline owner | heartbeat 每轮 +1，位置是 `StatsMain::collect()` 的**最后一条语句**（无条件，V14、V28）；boottime 启动时一次；last_stats_clear 待 clear 功能 |
| sys | `/sys/num_worker_threads` | `Gauge` | `hammer-runtime`（worker thread 域，`thread_main.rs`/`start_workers.rs`） | 由该域的 `#[derive(Stats)]` 声明创建条目（D9.5），值由该域在自己的生命周期点 `set_gauge` 一次（`start_workers`；VPP 在 `vlib_thread_init` 里 create + set，`threads.c:221-225,404`）；**没有采集者**（A6） |
| sys | `/sys/main_loop_count_per_worker` | `CounterVectorSimple`，1 行 × `worker_count` 列 | `hammer-runtime`（stats owner，`config/stats.rs`） | worker 本地 `fetch_add`；更新者是该条目自己的 `MainLoopCountCollector`（`register_collector` 挂进轮次），`StatsMain::collect()` 每轮借出条目并复制（A6、A8、D9） |
| sys | `/sys/loops_per_worker` | `CounterVectorSimple`，1 行 × `worker_count` 列 | `hammer-runtime`（stats owner，`config/stats.rs`） | worker 主循环按 200µs 窗口算阻尼 loops/s 并发布进自己的每线程原子（VPP 的 `vlib_main_t.loops_per_second`，V13）；更新者是该条目自己的 `LoopsPerSecondCollector`（与 `main_loop_count_per_worker` 各一个采集者类型；VPP 的 `vector_rate_collector_fn` 是同一个函数服务两条条目），`StatsMain::collect()` 每轮复制（A6、A8、D5） |

三条硬约束：

- 目录只使用现有 `DirectoryType`；`STAT_SEGMENT_VERSION` 保持 2；共享结构布局、
  epoch/in_progress 协议、只读映射方式都不变。
- 家族内部的所有条目（vector 与 symlink）必须在同一次注册里出现；客户端要么
  看到完整家族，要么什么都看不到。
- 每个家族只能通过 stats 的通用机制接入：宏声明 + 目录/写操作 + owner 自己的
  每轮步骤。业务类型、堆引用、worker 索引不得出现在 `StatsSegment` 的字段或
  签名里，也不得进入 `StatsMain::collect()`（D1）。

### D1 机制、声明与业务的边界

**问题：**草案第一版把 mem family 实现成一个自造的 provider 框架：段内
`memory_heaps` 字段、`add_memory_heap` 事务、`sample_memory_heaps` 段内步骤，
后来又造出 `MEMORY_HEAPS`、`publish_memory_heap`、
`publish_process_memory_heaps`。那既把业务并进了 `hammer-stats`，也绕过了仓库
已有的声明机制：`Sys` 用 `#[derive(Stats)]` 声明条目，mem family 却手写目录
操作，术语（"publish"）也不在既有词汇表里（宏生成的是
`bootstrap`/`install`/`StatsRegistration`）。第二版改掉了这些，但把轮次写成
runtime 的 `run_stat_segment_updates = update_memory_stats →
update_main_loop_count → StatsMain::collect()`，把堆清单写成
`init_stats_main` 里一个两行的 `[(mem.stat_segment, "stat segment"),
(mem.main_heap, "main heap")]` 数组：顺序、存在性与"有哪些家族"都成了业务代码，
第三个堆（api segment）和插件堆都加不进来。本版按 VPP 修正：**每个堆由它的
owner 自己登记（名字 + 自己的采集者），登记项进入 owner 所在 registration
image 的 stats 集合，由 stats init 的遍历执行**（H14、V26、A5、A11）。

**拟议三层划分：**

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| 声明（`hammer-component-macros` 的 `#[derive(Stats)]`/`#[stats_collect_registration]` + `hammer-runtime::registration`） | **注册语义只有一处**：`#[derive(Stats)]` 为每个条目生成 `install(&mut StatsMain)`（建条目、行宽 `columns`、别名 `symlinks`）与 `__STATS_REGISTRATION_<Struct>` 静态项。**更新语义另有一处**：owner 的 `#[stats_collect_registration]` 函数 `fn(&mut StatsMain) -> RuntimeResult<()>` 在条目装好后 `register_collector`，并生成它自己的静态项。两类项都列进 owner 所在 crate/插件的 registration image（A12、D9.5）；`bootstrap` 只服务段预建的固定槽 | 不表达堆的生命周期、不保存堆引用、不拼其它 owner 的步骤、不排序（顺序 = image 列表顺序）；`#[derive(Stats)]` 不认识采集者 |
| 机制（`hammer-stats`：`StatsSegment`/`StatsMain`） | 通用目录与写操作（`validate`、`add_symlink`、`set_gauge`/`set_timestamp`/`set_simple_counter`、条目级 `set_simple_counter_cell`/`set_scalar`、`remove_entry`、`heap()` 借出段自己的堆；结构变更取 `&self` + D10 的段锁，值写取 `&self` 且不取锁，见 D10）、段内分配的活动堆窗口（窗口内普通分配，耗尽即崩溃）、**进程私有的采集者表与轮次**（`Collector` trait、`StatsMain.collectors: Vec<Box<dyn Collector>>`、`register_collector(&mut self, impl Collector + 'static)`、`collect(&self)`）、`StatsError` | 不认识 `MemHeap`、`HeapUsage`、runtime 状态、worker/thread index；不知道任何一个堆名或 `/mem` 的列序；不保存堆引用；`collect()` 的实现里不出现任何 family 名字 |
| `/mem` 家族模块（`hammer-stats::mem`，VPP `provider_mem.c` 的对应物） | 只留 `/mem` 的列契约：VPP `stat_mem_usage_e` 的列常量与唯一入口 `update_mem_usage (entry: &DirectoryEntry, usage: HeapUsage)`（七列映射，写行 0） | 不认识 `MemHeap` 与 `HeapUsage` 之外的 infra 类型、不知道有哪些堆、不保存堆引用、不建条目、不登记采集者、不写 `StatsSegment` 的内部；不是 `StatsSegment`/`StatsMain` 的方法（6.2） |
| 业务（每个堆的 owner、`/sys` 各条目的 owner） | `hammer-runtime`：worker thread 域的 `/sys/num_worker_threads`（`thread_main.rs`）、主循环的两个 per-worker vector 与它们的采集者（`config/stats.rs`）、`main heap`/`stat segment` 两条堆登记；`hammer-ipc`（镜像列在 `hammer-service`）：api segment 三条 region 堆登记；未来的插件：自己的堆登记。每条登记 = 名字 + 自己的采集者（`private_data` + `collect`） | 不往 `StatsSegment`/`StatsMain` 加 family 字段或方法，不写共享结构体内部，不替插件拥有计数/堆，不在 `init_stats_main` 里写堆清单，也不在 runtime 里拼一条"家族步骤链"当轮次 |

**回答"宏里面加什么 bootstrap"：什么都不用加，也不该加。**`bootstrap` 的语义
是"这个槽由统计段自己预建，安装时只按固定索引绑定"——现在只有
`/sys/heartbeat`、`/sys/last_stats_clear`、`/sys/boottime` 三个固定槽属于它
（`Sys` 的 `bootstrap = STAT_COUNTER_*`，H11、V18）；它仍先于所有 `install`
执行，固定槽索引不受任何堆条目影响。

**为什么条目（含 mem 堆）改由 `#[derive(Stats)]` 声明（V30）：**第一版认为 mem
条目要绑定"运行期事实"（堆名与该堆的采样函数），超出该宏的语义，于是给每个堆写
一个自由函数。这个判断不成立：五个堆的目录名都是 owner 的事实
（`/mem/main heap`、`/mem/stat segment` 是字面量，api segment 的
`/mem/<region> pvt` 与 `/mem/<region> data` 来自配置期的 region 名），行宽、
别名同样是 owner 自己的声明——正是**注册语义**能表达的东西。本版因此把注册固定
到 `#[derive(Stats)]`（新增 `path`/`columns`/`symlinks` 三个字段属性，6.5），
把**更新语义**留给该 owner 自己的一个采集者类型 + 一条 `register_collector`
登记项（对应 VPP 同一注册点里紧随其后的
`vlib_stats_register_collector_fn (&r)`，V30）；`mem::register_mem_heap` 删除，
`#[stats_collect_registration]` 保留为更新那一半（D9.5）。

**为什么声明用属性宏而不是手写 `static`：**手写 `static __STATS_REGISTRATION_… =
StatsRegistration { name: …, register: … }` 会把静态项名字、`name` 字段与函数名
各写一遍，而仓库里同类声明（`#[init_function]`、`#[main_loop_enter_function]`、
`#[derive(Stats)]`）都由 `hammer-component-macros` 生成静态项、由 image 列表
收录。`#[derive(Stats)]` 生成的 `__STATS_REGISTRATION_<Struct>` 就是这条既有
形状：宏只生成一个静态项，`RegistrationImage` 仍是唯一的登记载体，加载、顺序与
`run_stats_registrations()` 的遍历完全不变（A12、H8）。

**为什么保留 VPP 的登记集合（V14、V21、V26、V29）：**上一版把轮次写成 runtime
的一条函数链、把堆清单写进 `init_stats_main`：每加一个 family/堆都要改这两处
业务代码。VPP 的答案是数据驱动：每个 provider 在自己的注册点把 `collect_fn` 与
位置事实写进 `sm->collectors`（V26、V30），`do_stat_segment_updates` 只做
"遍历 → 调用 → heartbeat++"（V14、V28）。Hammer 采用同一形状，并把 VPP 的一张
表拆成职责明确的两层：**声明与条目**（owner 模块里的 `#[derive(Stats)]`，启动期
由 `run_stats_registrations()` 遍历执行）、**采集者表**（`StatsMain.collectors`，
进程私有、无锁，`StatsMain::collect()` 每轮遍历 → 借出条目 → 采样 → heartbeat++，
D9）。ADR-0019 §6.6 拟议的采集者登记表由此
落地，并按本 ADR 的决定修订签名、归属与表行形状（见 §9）。VPP 用
`private_data` 携带 owner 自己解释的实例状态（`memory_heaps_vec` 下标、
gauge 索引、池下标），Hammer 让**采集者类型自己的字段**携带同一格（类型由编译器
检查，D9.2），表行由 `Box<dyn Collector>` 的 fat pointer 承担 VPP 的
`(fn, private_data)` 对。VPP 的 `vector_index` 全树只被存储与透传、没有任何采集者
读它（V26 末段），Hammer 因此不设这一格。

**词汇：**本 ADR 不使用"publish"这类自造词。**注册语义**是 `#[derive(Stats)]`
的 `install`（`path`/`columns`/`symlinks`），**更新语义**是 owner 的一个采集者类型
（VPP 的 `fn` + `private_data` 两格）与它所在的
`#[stats_collect_registration]` 项（D9.5）；固定槽用
`bootstrap`，`/mem` 的列契约用 `mem::update_mem_usage`，轮次用 `collect`。

### D2 mem stats：堆集合、条目形状与每堆登记

**堆集合（"一个 mspace 堆 = 一个 `/mem/<name>` 条目"，H14）：**

| 堆（`MemHeap::name()`） | owner（声明者） | 声明点 | 条目名 | VPP 对应 |
| --- | --- | --- | --- | --- |
| `main heap` | `hammer-runtime` | `config/stats.rs` 的 `#[derive(Stats)]` 声明 | `/mem/main heap` | `threads.c:576-577` |
| `stat segment` | `hammer-runtime`（堆本身由 `hammer-stats` 拥有） | `config/stats.rs` 的 `#[derive(Stats)]` 声明 | `/mem/stat segment` | `stats/init.c:121` |
| api root region 私有堆（`svm region`） | `hammer-ipc`（镜像由 `hammer-service` 列出） | `memory_shared.rs` 的 `#[derive(Stats)]` 声明 | `/mem/global_vm pvt` | 无（VPP 不登记 region 堆，V6） |
| api region 私有堆（`svm region`） | 同上 | 同上 | `/mem/<region-name> pvt`（默认 `/mem/vpe-api pvt`） | 同上 |
| api region data 堆（`svm data`） | 同上 | 同上 | `/mem/<region-name> data`（默认 `/mem/vpe-api data`） | 同上 |
| 插件堆（SSVM 等，之后落地） | 各自插件 | 插件 image 的登记项 | 插件自选唯一名 | 无 |

注册是一条 `#[derive(Stats)]` 声明、更新是该堆自己的一个采集者类型
与它所在的 `#[stats_collect_registration]` 登记项，两步都由 owner 模块里的项生成
（D9.5，A12）。
堆名会重复（每个 region 都叫
`svm region`/`svm data`），所以条目名不直接取 `MemHeap::name()`：`/mem/<name>` 的 `<name>` 由 owner 选，VPP 同名
（`main heap`/`stat segment`）保持原样，region 堆用 `<region 名去掉前导 '/'>
<角色>`（`pvt`/`data`）。重名仍由既有 `DuplicateName` 拒绝（D6）；集合是
owner 的选择（V6），加/减一个堆只是加/减一对“声明 + 登记项”。

**条目形状与列语义（列序与 VPP 完全一致）：**

| 列 | VPP 常量 | Hammer 字段名（Rust） | 含义 |
| --- | --- | --- | --- |
| 0 | `STAT_MEM_TOTAL` | `total_bytes` | 堆当前从系统获得的字节数（dlmalloc `arena`） |
| 1 | `STAT_MEM_USED` | `used_bytes` | 已分配字节数（`uordblks`） |
| 2 | `STAT_MEM_FREE` | `free_bytes` | 堆内空闲字节数（`fordblks`） |
| 3 | `STAT_MEM_USED_MMAP` | `used_mmap_bytes` | mmap 区字节数（`hblkhd`） |
| 4 | `STAT_MEM_TOTAL_ALLOC` | `max_allocated_bytes` | 历史最大已分配字节数（`usmblks`） |
| 5 | `STAT_MEM_FREE_CHUNKS` | `free_chunk_count` | 空闲 chunk **个数**（`ordblks`） |
| 6 | `STAT_MEM_RELEASABLE` | `releasable_bytes` | 可 trim **字节数**（`keepcost`） |

VPP 的两个 C 字段名与内容不符（V8）：`bytes_free_reclaimed` 存的是个数，
`bytes_overhead` 存的是可释放字节。Hammer 的 Rust 字段必须按事实命名，不能
把这个命名错误一起复制；列序号保持相同，VPP 风格客户端仍能按列读取。
`object_count`、`bytes_used_sbrk` 在 dlmalloc mspace 下没有可靠来源，不进入
目录（不用零值冒充统计）。

**symlink：**`/mem/<name>/total|used|free` 分别指向列 0/1/2，语义与 VPP 相同
（V3）。别名由同一声明的 `symlinks = [("total", 0), ("used", 1), ("free", 2)]`
建立，别名路径 = 条目名 + 别名（VPP `add_symlink (idx, STAT_MEM_USED, "/mem/%U/
used", …)`，V3）：owner 不重复写路径字符串，别名规则在声明里只有一处。

**一个采集者类型，一个堆一个实例（A5、D4、D9.2）：**堆与堆之间不同的只有"采哪个
堆"这一个变量，所以只有 `HeapCollector` 一个类型：实例自己的字段就是那个堆
（VPP 的 `private_data`），行为是"采自己的堆 → 用轮次交给它的条目借用调家族模块里
唯一的七列映射 `mem::update_mem_usage (entry, usage)`"——VPP 正是这个形状：
`provider_mem.c` 只有**一个** `stat_provider_mem_usage_update_fn`，每个堆调一次
`vlib_stats_register_mem_heap`，堆身份放在 `private_data` 里（`memory_heaps_vec`
下标，V26、V30）。采集者不取段锁、不查 `StatsMain::global()`（D9.3）：

```rust
/// VPP `stat_provider_mem_usage_update_fn`（`provider_mem.c:33-45`）的对应物：
/// 一个类型服务所有堆，"哪个堆"是实例自己的字段。
struct HeapCollector {
    entry_index: DirectoryIndex,
    heap: &'static MemHeap,
}
```

| 登记点（owner） | 那个实例的 `heap` 字段（VPP 的 `private_data`） | 备注 |
| --- | --- | --- |
| `main heap`（hammer-runtime） | `MemMain::main_heap()` | 进程生命周期主堆（V5） |
| `stat segment`（hammer-runtime） | `stats_main.segment.heap()` | 段自己的堆（V4、V18、A3） |
| `global_vm pvt`（hammer-ipc） | `root_region().pvt_heap()` | root region 的 pvt 堆 |
| `<region> pvt`（hammer-ipc） | `primary_region().pvt_heap()` | api region 的 pvt 堆 |
| `<region> data`（hammer-ipc） | `primary_region().data_heap()` | DATA region 的 data 堆 |

采样不分配、不做名字查找、不跨 await，也不返回 `Result`（A2、D4）；调用者是
机制（`StatsMain::collect()` 遍历采集者表），node 不直接碰它们。

**api segment 的 region 堆：引用在登记期取一次，映射存在性也只判一次。**
`HeapCollector` 持有 `&'static MemHeap`（`ApiMain::current()` 是 `&'static`，
`root_region()`/`primary_region()` 返回 `&'static SvmRegion`，再经 6.3 下面那对
`SvmRegion::{pvt_heap, data_heap}` 只读入口取堆——旧的 `RegionLock::pvt_heap`
借用绑在 guard 上、活不过一轮），所以"映射还在不在"是**登记期的前置条件**，
不是每轮的判断：
`binary_api_init` 先映射 root/api region 再声明与登记堆，并且它
`runs_before = ["stats_main_init"]`（`crates/hammer-service/src/binary_api.rs:700-703`），
所以三条登记项运行时 region 一定已映射；唯一的解除映射在 `exit_binary_api`
（`hammer-service/src/binary_api.rs:548-556` 的 `unmap_shared_regions()`），而 runtime
只在 `run_main_until` 返回、`stop_processes()` 之后才跑 main-loop exit 函数
（`crates/hammer-runtime/src/main_loop.rs:58-62`）——即**最后一个轮次之后、同一线程**。
代价是一条要写进验收的前提：**若以后新增运行期 unmap 路径，它必须先停掉持有该
region 堆引用的采集者**。本 ADR 不新增 unregister API；每 session 创建/销毁的堆
（SSVM）不在集合内（D8、第 9 节）。

### D3 system stats：条目、owner 与列映射

| 条目 | 类型与形状 | 语义 | owner / 更新点 |
| --- | --- | --- | --- |
| `/sys/num_worker_threads` | `Gauge` | 配置的 Data Worker 数（不含 main thread），与 VPP `n_vlib_mains-1` 同义 | worker thread 域（`thread_main.rs`）的声明创建条目；该域在自己的生命周期点（`start_workers`）从 `ThreadMain::worker_count()` `set_gauge` 一次，不登记采集者（VPP `threads.c:221-225,404`） |
| `/sys/main_loop_count_per_worker` | `CounterVectorSimple`，1 行 × `worker_count` 列 | 每个 Data Worker 的**累计**主循环次数（权威值）；列 `i` 对应 Data Worker `i`，即 runtime thread index `i+1` | 条目 owner 是主循环（stats owner 模块 `config/stats.rs` 的声明）；更新者是该条目自己的 `MainLoopCountCollector`（`register_collector` 挂进轮次）；worker 本地 `fetch_add(1, Relaxed)`，采集者每轮把原子值写进列 |
| `/sys/loops_per_worker` | `CounterVectorSimple`，1 行 × `worker_count` 列 | 每个 Data Worker 的 loops/s（**速率投影**，VPP 同名同义：200µs 采样窗口 + `K = exp(-1/20)`，V13；窗口取 VPP 代码值而非其 20ms 注释）；列 `i` = Data Worker `i`；最新值，不是累计量、不能差分 | 值的 owner 是 worker 自己的主循环（`DataPlaneMain` 的窗口字段 + 每线程发布原子，V13）；`update_loops_per_second` 每轮把发布值复制进列（VPP `vector_rate_collector_fn` 服务两条条目，形状一致） |
| `/sys/heartbeat` | `ScalarIndex` | 轮次计数（现有语义，每轮 +1） | 不变；`StatsMain::collect()` 遍历完登记表后取这一条条目把它的值 +1——与采集者同一种“取条目 → 写值”，不是锁步骤（V14、D9.4） |
| `/sys/boottime` | `ScalarIndex` | collector 启动时的 Unix 秒（现有语义） | 不变 |
| `/sys/last_stats_clear` | `ScalarIndex` | 上次 clear 的 Unix 秒；clear baseline 落地前保持 0 | 不变（ADR-0019 D4.7） |

四个刻意的语义选择：

1. **列只覆盖 Data Worker。** VPP 的两个 per-worker 列包含 thread 0，因为 VPP 的
   main thread 也跑 vlib main loop。Hammer 的 main thread 跑 Tokio process loop
   （H7），没有 data-plane 主循环可投影；写一个恒为零的第 0 列只会制造歧义。列数
   与列映射必须由客户端从 vector 长度推导，不能写死。
2. **累计与速率都发布，权威关系明确。** `/sys/main_loop_count_per_worker` 是累计
   计数（CONTEXT 的 Runtime Statistic 语义），`/sys/loops_per_worker` 是 owner
   计算、窗口固定的速率投影（D5）。客户端把速率当"当前值"读，需要其它窗口时从
   累计量派生，不得对速率做差分。
3. **条目的 owner 与 VPP 一致，声明走同一套机制。** 主循环的两个 per-worker
   vector 由 `Sys` 声明（字段名即路径叶子：`main_loop_count_per_worker`、
   `loops_per_worker`，都不是 `bootstrap`），行宽由 `columns = <worker 数>` 在
   `install` 里 `validate(…, 0, worker_count-1)`；这两个条目的更新者是它们自己
   的 `MainLoopCountCollector`/`LoopsPerSecondCollector` 采集者登记项（VPP
   `init.c:110-131` 也是"先 validate、
  再登记采集者"）；`/sys/num_worker_threads` 由 worker thread 域自己的声明创建
  并由该域在自己的生命周期点 `set_gauge` 一次、不登记采集者（VPP 在
  `threads.c:221-225,404` 的 `vlib_thread_init` 里做同一件事）。全部发生在
  worker 启动之前。
4. **`/sys/vector_rate*` 是推迟项，不是省略。** 它们要读每 worker 的 internal
   node calls/vectors（VPP `vlib_internal_node_vector_rate`，V12、V13），Hammer
   的图运行时还没有这两个计数；它们与 `/sys/node/*` 一起在 ADR-0019 M4 落地，
   本 ADR 不写零值占位。

`/sys/node/*` 不在本家族内：它属于 ADR-0019 M4 的 node 计数与 clear baseline，
本 ADR 不重复设计，也不把 node 调用/向量计数提前塞进 system 家族；`/sys/vector_rate*`
的加入点是同一批计数（D5、第 9 节）。

### D4 更新与同步模型：owner 原子计数 + 机制轮次遍历投影

| 层 | 行为 | 同步依据 |
| --- | --- | --- |
| 每个 Data Worker | `increment_main_loop_count` 做 VPP 主循环的相邻两步（`main.c:1685` 的计数 + `:1689-1714` 的速率块）：计数 `fetch_add(1, Relaxed)`；报告间隔到期时用 `DataPlaneMain` 自己的 `loops_this_reporting_interval`/`loop_interval_start`/`loop_interval_end`/`loops_per_second`/`damping_constant` 算阻尼 loops/s，并 `store(…, Relaxed)` 进自己的 `WorkerThread.loops_per_second`（V13） | 两个值都是单元格单写者的统计量，不参与所有权或顺序发布；仓库同步规则允许 Relaxed 用于统计。窗口字段是 worker 本地 `DataPlaneMain` 的字段，不共享、不需要原子 |
| 计数存储 | `ThreadMain` 的每线程记录中一个 cache-line 隔离的原子计数，随 `WorkerThread` 记录在 worker 启动前构造 | 固定每线程集合在 worker 启动前构造，worker 只借用自己 runtime thread index 对应的条目 |
| `StatsMain::collect()`（机制，轮次） | 每轮按**登记顺序**遍历进程私有的采集者表（登记集合 = 5 个堆各一条 + 主循环两条 per-worker vector；顺序 = 各 owner 声明在 image 里的顺序）：为每条登记取出该条目的共享借用（VPP 的 `data.entry`）并调用该表行（表行在 `Collector::collect` 里写自己那几列）；最后取 heartbeat 条目把值加一（D9.4）。这就是 VPP `do_stat_segment_updates` 的"遍历 collectors → heartbeat"（V14、V28），**全程不取锁**；`collect()` 的实现里没有 family 名字 | 表无锁（进程私有、启动期构建、此后只读，V29）；写是映射内裸写（VPP 同形），唯一写者是 thread zero 的轮次；worker 只写自己的每线程原子，两个 per-worker 采集者对每个线程的每线程原子各做一次 `Relaxed` 读 |
| 采集者登记（机制的表 + owner 的登记项） | 启动期（node 与 worker 启动前）完成，且分两步：`#[derive(Stats)]` 的 `install` 建条目/行宽/别名（注册语义），owner 的 `#[stats_collect_registration]` 项随后调一次 `register_collector(该 owner 的采集者)` 把自己的采集者挂上（更新语义；与 VPP `vlib_stats_register_collector_fn` 同形，V30）；表此后只读 | 表的写者只有启动期的登记项（单线程），读者只有轮次；没有表锁，也不与段锁嵌套 |
| `statseg-collector-process` process node（thread zero） | 薄驱动：设置 boottime，然后每轮"调用一次 `StatsMain::collect()` → 取 `update_interval` → sleep"；循环体不变，家族步骤不在 node 里（H12、V23） | node 不认识 mem/system，也不持有 stats 段；它只按 interval 调度轮次 |
| 写路径（轮次与 setter 都不取锁） | `collect()` 只是"取条目 → 写值"（采集者写自己的单元格，最后一条同形语句写 heartbeat）；`set_gauge`/`set_timestamp`/`set_simple_counter` 是"按索引写一个值"（V20、V28）。写入口取 `&self`：单元格走 `AtomicU64::from_ptr`（已有做法），标量走 `addr_of_mut!((*entry).data.value)` 的映射地址写；改目录结构的操作（`add_*`/`validate`/`add_symlink`/`remove_entry`）取 `&self` + D10 那把段锁（VPP `vlib_stats_segment_lock` 的对应物） | 唯一写者是 thread zero（轮次 + owner 自己的生命周期点：boottime 在 node、worker 数在 `start_workers`、登记在启动期）；worker 只写自己的每线程原子，其它进程只读映射。因此值写与轮次不需要锁，也不需要第二套同步——这就是 VPP 的形状（`stats.c:263-269`、`collector.c:131-151`）；`Relaxed` 只用于统计值 |

**（ADR-0024 例外）**node error counter 家族按 VPP 的形状改为"记录点写本线程那一行"
（`DataPlaneMain::record_current_node_error` → `StatsSegment::increment_simple_counter`）：
行归该线程所有（行号 = 自己的 `thread_index`）、条目索引在冻结点装好、列增长只允许发生在
冻结点之前、单元格在机制层本来就是原子访问位置。`/sys/*` 家族继续走本节的"owner 原子计数 +
轮次投影"，不变。

所有采集者与 `StatsMain::collect()` 都不返回 `Result`：写值 API 与 VPP
的 `vlib_stats_set_gauge`/`vlib_stats_set_timestamp` 一样只做"按索引写一个值"
（V20），没有可恢复失败；索引/类型/行列失配意味着本模块的安装与写入不一致，
按本地断言终止（ADR-0019 的错误分类规则）。登记表本身也不返回错误：它在启动期
增长一次（Main Heap 上的普通分配，A9 的崩溃语义适用）。

**为什么不让 worker 直接写共享单元格（VPP 的直接写法）：** 那需要把 stats
segment 内的行地址安装进 worker，并在 `validate` 换行/扩容时失效；ADR-0019
第 6.5 节明确把"worker 行安装"留给 M2 并禁止先造任意 row getter。本设计把
system stats 与那个尚未解决的所有权问题解耦：worker 只碰自己的每线程状态
（累计计数原子 + 阻尼速率发布值）与自己的窗口字段，**周期性**写共享段的只有登记表
里的 owner 采集者（五个堆各一个 + 主循环两个）；一次性写（`/sys/boottime` 由
node、`/sys/num_worker_threads` 由 worker thread 域在 `start_workers`）由各自的
owner 在自己的生命周期点按同一条写路径完成。代价是累计列的数值按
`update_interval` 粒度更新（默认 10s），这与 VPP node counters 的采样粒度一致，
不是新的限制；速率列存的是 worker 最近一次窗口的值，与 VPP 的
`vm->loops_per_second` 语义相同。

**为什么不加 barrier：** 计数是统计值，不需要精确瞬时快照；每轮为读计数进
WorkerBarrier 会周期性停住所有 worker，成本远大于收益（ADR-0019 D4.6 只在
没有合法 owner snapshot 时才允许 barrier 采样，这里存在合法的 owner 快照）。

**内存序说明：** 发布的是数值而不是所有权；`Relaxed` 读写不构成数据竞争
（每个单元格与每个每线程计数都只有单写者：计数器只有本 worker `fetch_add`，速率
只有本 worker `store`，采集者是唯一读这些值的线程），也不声称跨列的原子快照。
速率是"最近一次窗口"的值而不是快照一致量，客户端契约里明确这一点（D7）。`epoch`/`in_progress` 只保护
目录结构，不保护数值，这一点在客户端契约中再次声明。

### D5 速率策略：发布 VPP 同名速率投影，累计计数仍为权威

**问题：**本 ADR 的上一版决定"不发布任何速率"，理由是 CONTEXT 的 Runtime
Statistic 规则要求累计计数、速率在格式化时派生。这个决定撤回：VPP 的 system
stats 本身就包含速率/比值条目（V12、V13），"对齐 VPP"是本次设计的前提；把速率
全部推给客户端会让 `/sys/*` 与 VPP 客户端不兼容，也让 VPP 的
`/sys/loops_per_worker` 这个名字消失。

**拟议：**发布 owner 计算、窗口明确的速率投影，累计计数继续作为权威条目：

| VPP 条目 | Hammer 条目 | 类型 | 值的来源与窗口 | owner |
| --- | --- | --- | --- | --- |
| `/sys/loops_per_worker` | 同名 | `CounterVectorSimple`，1×`worker_count` | worker 主循环的阻尼 loops/s：窗口 200µs 采样、`K = exp(-1/20) ≈ 0.95`（V13），cell 存截断后的整数 loops/s（VPP 同样把 f64 写进 `counter_t`） | runtime（worker 主循环发布自己的值，`LoopsPerSecondCollector` 每轮复制进列） |
| `/sys/main_loop_count_per_worker` | 同名（Hammer 独有） | `CounterVectorSimple`，1×`worker_count` | 累计次数，无窗口；客户端需要任意窗口速率时对它差分 | runtime（worker `fetch_add`，`MainLoopCountCollector` 每轮复制） |
| `/sys/vector_rate_per_worker` | 推迟（ADR-0019 M4） | 计划：`CounterVectorSimple`，1×`worker_count` | 自上次 clear 的 vectors/calls（`vlib_internal_node_vector_rate`，V12、V13）；需要每 worker 的 internal node calls/vectors 计数 | node 计数的 owner（ADR-0019 M4） |
| `/sys/vector_rate` | 推迟（ADR-0019 M4） | 计划：`Gauge` | per-worker vector rate 的 worker 平均（VPP 分母是 `n_threads-1`，V12） | 同上 |

**CONTEXT 契约修订（实施时必须同步改文档）：**CONTEXT 的 Runtime Statistic 条目
现在把速率一律划到"格式化时派生"（`_Avoid_: stored rate`）。本设计把两类统计分开：

1. **累计计数**（`/sys/main_loop_count_per_worker`、`/mem/*` 七列、将来的 node
   计数）：CONTEXT 现有规则不变，客户端可对它差分。
2. **速率投影**（`/sys/loops_per_worker`）：由 owner 自己计算，窗口与阻尼常量
   固定在 owner 代码里（V13），每轮覆盖写；它不是累计计数，客户端不得差分，
   也不得把它当 clear baseline。

CONTEXT 的实施修订：新增 "Runtime Rate Projection" 术语（owner 计算、窗口固定、
覆盖写的 VPP 同名速率），并把 Runtime Statistic 的 `_Avoid_: stored rate` 限定为
"没有 owner、没有固定窗口、由查询/CLI 临时计算的速率"。这一条本身属于本次批准
范围（第 9 节的文档改动）。

**为什么 `/sys/vector_rate*` 不在本次：**它们要读每 worker 的
`internal_node_calls`/`internal_node_vectors`（VPP 的
`vlib_internal_node_vector_rate` = 自上次 clear 的 vectors/calls，
`main.h:403-421`；VPP 由同一个 rate collector 读并 clear，`init.c:44-53,118-131`）。
Hammer 的图运行时现在没有这两个计数，它们与 `/sys/node/*` 一起归 ADR-0019 M4；
写零值、或用 `main_loop_count` 冒充 vector rate 都会制造假统计，D2 对
`object_count` 已经拒绝过同类做法。M4 落地时按上表补两条，owner 是 node 计数的
owner，机制与 `collect()` 不变（只加登记项）。

### D6 生命周期、失败行为、命名与容量

- **安装与失败：**每条条目都有两条项：`#[derive(Stats)]` 生成的声明项（注册：
  `add_simple_counter`/`find` → `validate(index, 0, columns - 1)` → 三个
  `add_symlink`）与该 owner 的 `#[stats_collect_registration]` 项（更新：调
  `register_collector` 挂上自己的采集者；条目已由声明装好，A5、A11、A12）。
  `/sys/num_worker_threads` 由 worker thread 域的
  声明创建、由该域自己在 `start_workers` 里设置一次（它不是采集者）；
  两个 per-worker vector 的行宽由 `Sys`
  字段的 `columns` 建立，两条采集者登记由 `register_worker_main_loop` 完成（A6、
  VPP `threads.c:221-225,404` 与 `stats/init.c:110-131`）。它们都在 listener
  交付之后、main loop 与 worker 启动之前由 `run_stats_registrations(&mut
  stats_main)` 遍历所有 image 执行：任何一步 Err 都直接返回、启动失败，此时没有
  任何客户端能读到目录（owner 也还没发布），因此没有需要回滚的可见状态。机制侧
  不需要 `add_memory_heap` 这种 family 方法：注册是一条声明，更新是一条登记项。
- **名字：**按现有 `NameBytes` 规则（`protocol.rs:128-176`）校验：无 NUL、总长
  ≤ 127 字节。堆名可以是 `main heap` 这样的含空格字符串（与 VPP 相同，V9）。
  名字里允许 `/`，因为 stats 名字是不透明字符串；因此客户端的 `/mem/` 前缀
  枚举必须用"去掉前缀后的剩余部分"作为堆名，不得再按 `/` 切分（D7）。
- **重复名字：**返回既有 `StatsError::DuplicateName`；不做自动改名，也不静默
  复用已有条目。
- **容量与耗尽：**五个堆共 20 个目录项（5 个 vector + 15 个 symlink）加上向量
  载荷，远小于默认 32 MiB。**固定容量堆分配失败不是可恢复错误**：它表示进程
  已无法继续容纳自己声明的 stats 目录，而调用点也不再需要（也不应该）为它做
  判断——段内分配在 `activate()` 窗口里走普通分配器（H13、V24），耗尽就落在
  分配器的失败路径上（Rust `handle_alloc_error` → abort，对应 VPP 的
  `os_out_of_memory()` → `os_panic()`，V25）。因此**删除**
  `StatsError::HeapExhausted`（A9），也不保留"调用点手工选堆 + 检查返回值"的
  形状：不新增诊断函数，不新增错误变体。
  仍返回错误的只有"配置尺寸装不下 header 页 + 初始目录"的
  `CapacityTooSmall`：那是启动期输入校验，调用者能用一条配置错误诊断退出；
  它与"运行期把固定堆挤爆"是两类事实，不能共用一个错误。
- **错误类型不扩张：**本设计不新增 `StatsError`/`ProtocolError` 变体；名字过长
  走 `Protocol`，重复走 `DuplicateName`，类型/形状错误走
  `MetricTypeMismatch`/`InvalidShape`，容量错误走既有变体。
- **堆生命周期：**mem family 不在机制里保存堆引用（机制的表只存该登记的单态化
  入口），采集由 owner 在自己的函数里读自己当前的堆；五个堆的引用都在登记期取一次
  （`HeapCollector` 自己的字段，D2），所以"映射还在不在"只在登记期判一次：api
  segment 的三个 region 堆在 `exit_binary_api` 的 `unmap_shared_regions()`
  之后失效，而那是 main-loop exit 函数，跑在最后一个轮次之后、同一线程。每
  session 创建/销毁的堆（SSVM）与其它有运行期
  销毁路径的堆不纳入 `/mem/*`：本 ADR 不提供 unregister，`MemHeap::destroy`
  继续只服务未发布回滚（ADR-0019 D0.3）。
- **安装时机与顺序（H11）：**`StatsMain::create`（未发布）→ 固定槽
  （`Sys::bootstrap`）→ listener bind / FileMain 安装 →
  `run_stats_registrations(&mut stats_main)` 遍历每个 image 的
  `stats_registrations`（列表顺序：`Sys` 声明 → runtime 的两条堆声明与各自的
  采集者登记项 → runtime 的主循环采集者项 → `thread_main` 的
  `/sys/num_worker_threads` 声明（值由该域在 `start_workers` 里写，不占登记项）→
  `hammer-service` 镜像里的 api
  segment 三条声明与一条采集者登记项）→ `StatsMain::set` 发布。
  每条堆项只创建普通目录项，因此不可能早于 `Sys::bootstrap`，固定槽索引不受影响
  （H11、V18）；主循环项排在 `Sys::install` 之后（镜像列表顺序），所以它能
  读 `Sys::global()`。fd 交付回调只在 main loop 开始后运行，worker 也只在
  `start_workers`（main_loop_enter 函数）之后启动，因此客户端不可能在安装完成
  前读到半个家族。`worker_count` 取自 `ThreadMain::worker_count()`，此时 daemon
  已完成 `ThreadMain::configure()`，且 `WorkerCountZero`
  （`config/worker.rs:115-117`、`thread_main.rs:257-259`）保证它非零。
- **名字只声明一次：**`/sys` 名字由 `Sys` 字段声明；`/mem/<name>` 的名字由每个
  堆的 owner 在自己的 `#[derive(Stats)]` 声明里给出一次，条目名与三个别名都从这
  一个表达式派生（声明宏里的 `path` + `symlinks`），机制不保存任何 family 名字，
  也没有第二处拼写。

### D7 stats client 集成契约

客户端实现属于外部 `netsystem-client` 仓库；本节定义它必须遵守的共享内存契约与
客户端内部分层。本仓不新增客户端类型，也不改变协议 crate 的类型集合。

**客户端分层（客户端侧的 D1）：**

| 层 | 类型 | 职责 | 不负责 |
| --- | --- | --- | --- |
| 底层交互（机制） | `StatsClient` | 连接（socket + SCM_RIGHTS）、只读映射、epoch 重试与畸形映射拒绝、`names()`、`read(name) -> MetricValue` | 不认识 `/mem`、`/sys`、列序或 symlink 语义；不开设家族方法（`StatsClient::memory_heap_usages` 这类形状不加） |
| family 投影 | `StatsProvider` 实现：`MemoryStatsProvider`、`SystemStatsProvider`（零尺寸类型） | 用 `const PREFIX` 枚举家族名字、校验类型与列数、解析 symlink、把原始值翻译成 typed report；每个基条目只读一次 | 不持有连接/映射、不缓存 `DirectoryIndex`、不写段、不保存跨轮基线 |
| 使用方 | `client.report::<P>()` 的调用者、格式化/CLI | 读取两份 report、组合命令输出；需要其它窗口的速率时从累计列派生 | 不重新实现列序与别名解析、不对服务端速率列做差分 |

provider 只依赖 `StatsReader`（`names()`/`read()`）这一个能力 trait，所以家族
投影可以在内存 fixture 上单测，不需要真实 socket 或映射；`StatsClient` 是它唯一
的生产实现。`report` 是泛型入口，编译期单态化：没有 `dyn`、没有 provider
注册表、没有运行时查找——与服务端"家族只以登记参与"是同一取舍。新增 family
（M4 的 node 计数等）只加一个 provider，不改 `StatsClient`。

**服务端契约（客户端可以依赖的事实）：**

| 名字 | 类型 | 形状 | 语义与读取规则 |
| --- | --- | --- | --- |
| `/sys/num_worker_threads` | gauge | scalar | Data Worker 数（不含 main thread）；单个 u64 |
| `/sys/main_loop_count_per_worker` | simple counter vector | 1 行 × N 列 | 列 `i` = Data Worker `i`；N 从 vector 长度读取；**累计量**，可差分 |
| `/sys/loops_per_worker` | simple counter vector | 1 行 × N 列 | 列 `i` = Data Worker `i` 的 loops/s（服务端阻尼后的最新值，V13）；**速率投影**，只读当前值，不得差分，也不得当作累计量 |
| `/sys/heartbeat`、`/sys/boottime`、`/sys/last_stats_clear` | scalar | scalar | 现有语义（计数 / Unix 秒 / Unix 秒） |
| `/mem/<heap>` | simple counter vector | 1 行 × 7 列 | 列序见 D2；一次读取得到同一轮采样 |
| `/mem/<heap>/total`、`/used`、`/free` | symlink | 指向目标条目的第 0/1/2 列 | 读取返回"目标数据 + 选定列"，并保留别名名字（V17） |

**读取规则：**

1. symlink 读取遵循 VPP `copy_data` 语义：解析到目标条目，返回按该 symlink
   列裁剪后的同一类型数据，并把请求的名字（别名）作为结果身份。客户端不得把
   symlink 当成独立的 scalar。
2. `/mem/` 前缀枚举：列出名字后去掉 `/mem/` 前缀，剩余部分就是堆名；symlink
   名（`/mem/<heap>/used`）不计入堆集合。堆集合 = 各 owner 登记项的并集（当前
   五个：`main heap`、`stat segment`、api segment 的 root/pvt/data），客户端不
   假设数量与名字，也不从服务端源码推断；条目在进程生命周期内保持存在，堆不可
   用时它没有采集者、值保持 0（api segment 未映射，D2、D6）。
3. 列数由 vector 长度决定；客户端不得写死 worker 数或 7 这个数字以外的结构假设。
   对 `/mem/<heap>` 期望 7 列，长度不符时报类型化错误并附带实际长度。
4. 多列读取不构成原子快照：`epoch`/`in_progress` 只判定结构版本，不判定数值
   一致性。客户端可以重试结构变化，但不得声称拿到"同一瞬间"的所有列。
5. 速率分两类（D5）：`/sys/loops_per_worker` 是服务端 owner 计算、窗口固定的速率
   投影，客户端直接读当前值、不重算、不差分；`/sys/main_loop_count_per_worker`
   等累计列由客户端按需对两次采样差分来派生其它窗口的速率。服务端不保存累计列的
   基线（clear baseline 除外，见 ADR-0019 D4.7）。

规则 1–3 由 provider 实现（家族枚举、列裁剪、形状校验），规则 4–5 是 provider
与调用方共同遵守的读取语义；具体类型与方法签名见 6.4。

`MetricValue` 不需要新变体：mem 走 simple counter vector + symlink，system 走
scalar/gauge/simple counter vector。`hammer-stats-protocol` 不需要新类型、
不需要版本变化。

**客户端明确不做的事：**不重建服务端 heap、不写段、不缓存跨 epoch 的目录索引、
不把 `DirectoryIndex` 当长期身份、不在客户端实现服务端基线扣减、不对
`/sys/loops_per_worker` 再算一次速率、不在 `StatsClient` 上加家族方法、不引入
provider 注册表或 `dyn` 查找。

**格式化归属：**`show memory` / `show runtime` 这类命令属于 Module-owned ctl
（CONTEXT），本 ADR 只保证数据来源；命令的请求/回复与格式化器由对应 owner
（runtime 的 memory/runtime ctl）另行设计，客户端仓负责组合与传输。本 ADR 不新增
任何 Binary API 命令。

### D8 依赖、复用与范围外

**复用（不是前置阶段）：**

- ADR-0019 M0 的 `MemMain` VM + 映射内 `MemHeap` 是存储前提；mem stats 不得自建
  第二份映射或第二个 allocator。
- **ADR-0019 §6.6 的采集者登记表在本设计里落地**
  （D1、A8、6.2）：这是 VPP `vlib_stats_register_collector_fn` 的对应物，机制只
  存"采集者 + 条目索引"，不认识 family；落地时按本 ADR 的决定修订签名与归属
  （无 `Result`、`DirectoryIndex`、实例状态就是采集者类型自己的字段，登记从
  `StatsSegment` 移到 `StatsMain`），差异在 §9 逐条列出，实施时同步改写
  ADR-0019 §6.6，不留两种说法。
- `Sys` 聚合、`run_stats_registrations`、`statseg-collector-process` node 的位置
  沿用现状，不新增 Main、不新增 process node、不新增生命周期 hook；node 只保留
  "设置 boottime → 每轮调用一次 `StatsMain::collect()` → 取 interval 后 sleep"
  的薄驱动职责，家族以登记参与，遍历与 heartbeat 都在机制里。
- 写路径（H10/A2）与固定槽顺序（H11）不再单列前置依赖：写路径属于 M1 的机制
  改造，顺序由 `Sys::bootstrap` 先于 `run_stats_registrations` 的既有安排满足。

**范围外（明确不做）：**

- `/sys/node/names` 与 `/sys/node/{clocks,vectors,calls,suspends}`：ADR-0019 M4。
- `/sys/session/*`（VPP 中由 session 插件拥有，如 `sessions_per_worker`）：
  由 Hammer 的 session owner 在其插件内声明。
- VPP 同名的 `/sys/vector_rate` 与 `/sys/vector_rate_per_worker`（`/sys/loops_per_worker`
  属 D5 的本次交付，这两个依赖 ADR-0019 M4 的 node 调用/向量计数）。
- SVM/SSVM/classify 堆的统计条目（D2、D6；扩展条件见第 9 节）。
- Binary API memory/runtime ctl 命令与具体格式化文本（D7）。
- 客户端仓的具体实现与 CLI。

### D9 采集者模型：进程私有表、VPP 回调、注册与更新分离（本版修订）

**问题：**上一版把采集者表放进 `SpinLock<Vec<Collector>>`（VPP 里没有这把锁，
V29），把回调写成 `fn(DirectoryIndex, u32)`（VPP 是一个 `vlib_stats_collector_
data_t *`，V27），让每个 owner 的采集代码自己取段锁再调 `mem::update_mem_usage
(&mut segment, …)`（VPP 的采集函数只拿到"该条目"并写自己的单元格，V28、V30），
并且用自由函数命令式创建 `/mem` 与 `/sys` 条目（owner 侧本该只有声明，V30），
还把"更新"写成声明宏的属性 `collector = <fn>`（注册语义与更新语义混在一起）。
本节给出改后的形状；§4 D1/D4、§6.2、§6.3、§6.5、§6.6、§7、§8、§9 已同步。

**D9.1 表：进程私有、无锁（V29）。**`sm->collectors` 在 VPP 里是
`vlib_stats_segment_t` 的字段，注释写明 "internal, does not point to shared
memory"；注册（`pool_get_zero`）与轮次遍历（`pool_foreach`）都不取
`stat_segment_lockp`。Hammer 的对应物是 `StatsMain` 上一个**进程私有的追加表**
（普通集合即可：VPP 用 pool 只是 C 侧需要地址稳定，Hammer 的轮次按下标顺序借
用切片）；表与段锁无关——段锁只保护目录结构变更与 `in_progress`/`epoch` 发布，
值写与轮次都不取它（D10）。表因此没有锁，也没有"表锁"：

| 事实 | VPP | Hammer |
| --- | --- | --- |
| 表的所有者 | `vlib_stats_segment_t.collectors`（进程内，不进共享内存） | `StatsMain.collectors`（进程内，不与段同锁） |
| 登记时机 | 各 provider 在启动期注册（`vlib_stats_init`、插件 init） | 启动期、owner 未发布时；顺序 = registration image 列表顺序 |
| 登记的写者 | 注册点（单线程启动期） | 只有 `&mut StatsMain`：`register_collector (&mut self, impl Collector + 'static)` |
| 轮次的读者 | `do_stat_segment_updates`（thread-zero process node） | `StatsMain::collect (&self)`，直接借用切片，不取锁 |
| 发布次序 | segment 建立后才注册；node 在之后启动 | `init_stats_main`：建 owner（未发布）→ `Sys::bootstrap` 固定槽 → 遍历 image（条目声明 + 采集者登记）→ 发布 owner（`OnceLock::set`）→ listener/File |

发布之后的登记是 bug（启动期单线程是这条不变量的依据），因此 `register_
collector` 不接受 `&self`；ADR-0019 §6.2 的 `collectors: Pool<Collector>` 按本节
改写为进程私有的普通集合 `Vec<Box<dyn Collector>>`（D9.2；ADR-0019 的同步修订
见 §9）。

**D9.2 表行与回调：一个采集者类型 = VPP 的一行（V27、V28）。**
VPP 的表行是四个格子 `vlib_stats_collector_t { fn, entry_index, vector_index,
private_data: u64 }`（`stats.h:51-57`），回调收到一份
`vlib_stats_collector_data_t { entry_index, vector_index, private_data, entry }`
（`:31-39`）。Hammer 把"更新逻辑 + 实例状态"直接写成一个**具体类型的 `Collector`
实现**：`fn` 那一格是该类型的方法，`private_data` 那一格是该类型自己的字段，行与行
之间由 `Box<dyn Collector>` 的 fat pointer（数据指针 + vtable 指针）区分——正是 VPP
那两格的 Rust 形式。没有函数指针表项、没有 `u64` 下标、没有 `&'static ()` 擦除、没有
手写 vtable：

```rust
// crates/hammer-stats/src/lib.rs

/// 一个采集者；VPP `vlib_stats_collector_t` 表行的一行（`stats.h:51-57`）。
///
/// VPP 的行把"更新逻辑"（`fn`）与"实例状态"（`private_data`）分成两格；实现这个
/// trait 的具体类型把两者放在一起：方法体是 `fn`，类型自己的字段就是 `private_data`
/// 那一格（mem provider 在那里放堆指针/下标、buffer pool 放池下标、`init.c` 放 gauge
/// 索引——`provider_mem.c:69`、V26、V30）。字段名按领域取名（`heap`/`threads`/
/// `api_main`），不叫 `private_data`：那只是"每个 provider 自己解释的一格"的格子名。
///
/// 这是本仓库"生产代码不用 trait 对象"规则的一处**明确例外**（用户批准）：动态分发
/// 每个 stats 周期只发生几次、不在包路径上，换来的是表行与 VPP 一一对应——表是同质
/// `Vec<Box<dyn Collector>>`，机制不认识任何 family 类型。
pub trait Collector {
    /// 本行负责的目录条目（VPP 表行的 `entry_index`）；机制用它解析条目。
    fn entry_index(&self) -> DirectoryIndex;

    /// 轮次里的那一次调用（VPP `c->fn (&data)`，`collector.c:137-146`）。
    ///
    /// 参数是该条目的共享借用，对应 VPP 的 `data.entry = sm->directory_vector +
    /// c->entry_index`（`collector.c:139`）：采集者只写自己那几列单元格，不改
    /// header、不取段锁、不查全局（V28、V30）。
    fn collect(&self, entry: &DirectoryEntry);
}

impl StatsMain {
    /// 登记一个采集者；对应 VPP `vlib_stats_register_collector_fn`（`stats.c:589-604`）。
    ///
    /// 只在启动期调用（owner 未发布、node 与 worker 未启动）：表在这里增长一次，
    /// 此后只读，登记与轮次因此不需要同步（V29：VPP 的注册同样不取锁）。装箱是
    /// Main Heap 上的普通分配，失败走 A9 的 abort 路径。
    ///
    /// `+ 'static` 说的是**登记活到进程退出**（表是进程级的），不是"状态要擦成一个
    /// 指针"：采集者自己的字段就是 VPP 的 `private_data`，类型由编译器检查。字段可以
    /// 存 `&'static MemHeap`——VPP 的 `memory_heaps_vec` 存的也正是堆指针；api region
    /// 的堆引用在登记期取一次，它的映射只可能在最后一个轮次之后被拆（§6.3、D2）。
    pub fn register_collector(&mut self, collector: impl Collector + 'static);
}
```

- **VPP 的四个格子去哪了：**`fn` + `private_data` → 一个具体类型（`impl Collector`）；
  表行 `entry_index` → `Collector::entry_index()`；`data.entry` → `collect` 的参数
  `&DirectoryEntry`；`vector_index` → **不设**（VPP 整棵树只存储与透传它，没有任何
  采集函数读它，V26 末段；Hammer 的 mem/sys 条目都是单行，行号是采集者自己的事实）。
- **实例状态是采集者自己的字段，不是擦除过的指针。**堆采集者字段是
  `heap: &'static MemHeap`（V5 的 `MemMain::main_heap()`、段自己的堆）、主循环采集者
  是 `threads: &'static ThreadMain`、api segment 采集者是 `api_main: &'static ApiMain`；
  要按轮现取的状态（api region 的映射）不放字段，在 `collect` 里现借（§6.3）。
- **共享借用，不是 `&mut`。**VPP 的 `entry` 是可写裸指针；Hammer 用 `&DirectoryEntry`
  表达同一语义（"该条目"）。采集者只写 payload 单元格、从不改 header（VPP 的采集者
  也不改），所以共享借用足够：轮次不需要 `&mut StatsSegment`、也不需要锁。
- **与 VPP 的差异只有两处：**① 表行的 `fn` 那一格由 vtable 承担（VPP 是裸函数指针），
  表因此是同质 `Vec`；② 表行身份由类型表达而不是 `u64` 下标。其余三格、调用点与顺序
  都与 VPP 同形（V27、V28）。

**D9.3 轮次：无锁表、一条"取条目 → 调用"的循环（V28）。**
表中每一行都是一个自己实现了 `Collector` 的具体类型，它自己的字段就是 VPP 的
`private_data` 那一格（D9.2），所以 `collect()` 只需要"取条目 → 调用"：

```rust
impl StatsMain {
    pub fn collect(&self) {
        // 不取锁：与 VPP 的 `do_stat_segment_updates` 一样，写的是共享目录里的值。
        for collector in &self.collectors {
            // VPP: `data.entry = sm->directory_vector + c->entry_index`
            // （collector.c:139）：机制解析条目，交给采集者。
            let entry = self
                .segment
                .entry(collector.entry_index())
                .expect("a registered collector names a live directory entry");
            // VPP: `c->fn (&data)`（collector.c:137-146）：每一行在自己的
            // `Collector` 实现里写自己那几列（D9.2）。
            collector.collect(entry);
        }
        // 最后一条语句：取 heartbeat 条目，写它的值 + 1（VPP collector.c:149-150）。
        // 与上面每个采集者完全同形——"取条目 → 写值"，不是第二步、不是专用路径，
        // 也不取锁（现有的 `advance_heartbeat` 就是这个 `&self` 写，见 D9.4）。
        self.segment.advance_heartbeat();
    }
}
```

- 采集者在轮次里**只**看见它自己那一条条目的**共享借用**：没有
  `stats_main.segment.lock()`、没有把段交给家族映射的 `mem::update_mem_usage
  (&mut segment, …)`。上一版那两处形状全部删除（本条是用户指出的"直接更新
  segment"）：采集者拿到的是**它自己那一条条目**的共享借用（VPP 的 `d->entry`）
  加它自己类型里的实例状态，写的是它自己的单元格。
- 条目级写落在共享借用上，写的是已映射的地址（VPP 的 `cb[column] = value`，V2）：
  `entry.set_simple_counter_cell (row, column, value)` /
  `entry.set_scalar (value)` —— 两个方法都取 `&self`（与 `set_simple_counter` 同
  一条写模型，V29），所以 `collect` 的条目参数是共享借用就够。段级 setter
  （`set_gauge`/`set_timestamp`/`set_simple_counter`）保留给"owner 按索引写自己
  声明的槽"（固定槽、`/sys/num_worker_threads`），它们就是"取条目 → 条目级写"的
  薄封装——同一条写落点，不新增第二条语义不同的写路径（A2 的原始要求）。
- 条目查找失败是"声明与写入不一致"的 bug → 断言终止（A2、6.2 的规则）；
  轮次没有 `Result`，也没有锁。
- 与 VPP 没有差异：VPP 的轮次不取段锁（采集者裸写单元格、末尾 heartbeat 裸加一，
  `collector.c:131-151`、V22），Hammer 的轮次同样不取锁——`collect` 的条目参数
  是共享借用，单元格与标量写都经映射地址落到共享目录（A2 的"按索引写一个值"）。
  轮次里每一件事都是"取条目 → 写值"（采集者写自己的单元格，heartbeat 写固定
  槽），表本身无锁（D9.1）；没有 I/O、await、分配或未知回调。

**D9.4 心跳：轮次最后一条"取条目 → 写值"，和任何采集者同一形状（V28、V31）。**
VPP 的 `do_stat_segment_updates` 最后一条语句是
`sm->directory_vector[STAT_COUNTER_HEARTBEAT].value++`（`collector.c:149-150`）：
不走采集者表、不经过任何专用函数、没有任何额外步骤，就是**取 heartbeat 那一条
条目、把它的值加一**——与同一个函数上面那圈采集者做的完全一样（都是"拿到条目 →
写值"）。Hammer 同形：轮次最后取 `/sys/heartbeat` 条目、写它的值 +1，走的是与
采集者相同的那条条目级写路径（`segment.rs:150-154` 现有的 `advance_heartbeat`
就是这两行，取 `&self`；保留它或直接内联都等价，不新增 API、不新增锁、不新增步骤），空表时
同样执行。`/sys/heartbeat` 的类型是 VPP 的
`STAT_DIR_TYPE_SCALAR_INDEX`（Hammer 的 `Timestamp` metric 就映射到
`DirectoryType::ScalarIndex`，V31），值每轮 +1——它是整数计数，不是时间戳，也不由
任何采集者写入。

**D9.5 注册与更新各归其位：`#[derive(Stats)]` 达到的是注册语义，采集者达到的是
更新语义（V30）。**
VPP 的 `vlib_stats_register_mem_heap` 在同一个注册点做两件事：注册条目
（`add_counter_vector` → `validate` → 三个 `add_symlink`）与登记采集者
（`r.collect_fn = …; vlib_stats_register_collector_fn (&r)`）。Hammer 照同一
顺序分两步表达，两步都在**该堆自己的 owner 模块**里，也都由同一个
`RegistrationImage` 的列表顺序承载：

1. **注册语义 = `#[derive(Stats)]` 声明**：条目名、行宽、别名。
2. **更新语义 = 一个采集者类型 + 一条 `StatsMain::register_collector` 登记项**：
   每轮怎么读、写哪几列（VPP 的 `fn` 与 `private_data` 两格，D9.2）。

| 语义 | 由谁表达 | 内容 | 时机 |
| --- | --- | --- | --- |
| 注册 | `#[derive(Stats)]`（字段属性 `path`/`columns`/`symlinks`） | 条目名、行宽、别名——目录里"有什么" | 启动期 `install` 一次，此后不再变 |
| 更新 | owner 的采集者登记（由 `#[stats_collect_registration]` 项里的 `register_collector` 挂进轮次） | 每轮读什么、写哪几列单元格——值"怎么变" | 每轮 `StatsMain::collect()` |

两个"注册"不是同一件事：`#[derive(Stats)]` 注册的是**条目**（名字与形状），
`#[stats_collect_registration]` 注册的是**采集者**（把这一行挂进轮次），采集者
自己的字段与方法体才是更新语义的内容。声明宏不认识采集者，采集者也不建
条目；把实例状态与更新逻辑写成声明宏的字段属性就是让"注册"去表达"更新"，
本 ADR 明确不做（6.5、6.6）。

```rust
// crates/hammer-runtime/src/config/stats.rs：main heap 的 owner 侧两步

/// 1. 注册语义：`/mem/main heap` = 1 行 × 7 列 + 三个别名
///    （VPP `provider_mem.c:44-63`）。声明的静态项由 image 列出（与 `Sys` 同）。
#[derive(Stats)]
pub(crate) struct MainHeapUsage {
    #[stats(
        path = "/mem/main heap",
        columns = STAT_MEM_RELEASABLE + 1,
        symlinks = [("total", STAT_MEM_TOTAL), ("used", STAT_MEM_USED), ("free", STAT_MEM_FREE)],
    )]
    usage: SimpleCounter,
}

/// 2. 更新语义：VPP `stat_provider_mem_usage_update_fn`（`provider_mem.c:33-45`）
///    在 Hammer 的对应物——**一个采集者类型服务所有堆**：实例的两个字段就是 VPP
///    `vlib_stats_collector_reg_t` 的 `entry_index` 与 `private_data` 两格
///    （V27、V30：`provider_mem.c:69` 的 `r.private_data` 是 provider 自己集合里
///    的堆下标）；七列映射只有一份（`mem::update_mem_usage`，A11），所以每堆一个
///    实例，行为完全相同。
struct HeapCollector {
    entry_index: DirectoryIndex,
    heap: &'static MemHeap,
}

impl Collector for HeapCollector {
    fn entry_index(&self) -> DirectoryIndex {
        self.entry_index  // 该实例那一条声明已安装（启动期顺序保证）
    }

    fn collect(&self, entry: &DirectoryEntry) {
        hammer_stats::mem::update_mem_usage(entry, self.heap.usage());
    }
}

/// 2'. 该堆的采集者登记项；就是 VPP 注册点的最后几句
///     `r.entry_index = …; r.private_data = …; r.collect_fn = …;
///     vlib_stats_register_collector_fn (&r)`（`provider_mem.c:50-71`，V30）。
#[stats_collect_registration]
fn register_main_heap(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    stats_main.register_collector(HeapCollector {
        entry_index: MainHeapUsage::global().usage.index,
        heap: MemMain::main_heap(),   // VPP: r.private_data（:69）
    });
    Ok(())
}
```

`#[derive(Stats)]` 为此扩展三个字段属性（新增 surface 需批准）：

| 新增/修改的字段属性 | 语义 | VPP 对应 |
| --- | --- | --- |
| `path = <表达式>`（求值为 `&str`/`String` 的绝对路径，允许前导 `/`；字面量最常见） | 条目名就是 owner 给出的完整目录名，不再拼"结构体名小写/字段名"。表达式在 install 时求值：api region 的条目名来自配置期的 region 名 | `add_counter_vector ("/mem/%U", format_clib_mem_heap_name, heap)` 同样是注册期格式化出来的完整路径（`provider_mem.c:52`） |
| `columns = <表达式>` | 条目行宽；`install` 里 `validate (index, 0, columns - 1)`，表达式在 install 时求值（worker 数这类启动期事实） | `validate (idx, 0, STAT_MEM_RELEASABLE)`、`validate (reg.entry_index, 0, vlib_get_n_threads ())`（`init.c:130-132`） |
| `symlinks = [(name, column), …]` | 为指定列建别名 `<path>/<name>` | `add_symlink (idx, STAT_MEM_USED, "/mem/%U/used", …)`（`provider_mem.c:58-63`） |

**哪些角色消失或收缩：**

| 上一版的形状 | 本版 |
| --- | --- |
| `mem::register_mem_heap（stats_main, name, collect）`（A11） | **删除**：条目/行宽/别名改为 `#[derive(Stats)]` 声明，采集者登记改为 `register_collector`；两条都回到各堆 owner 的模块 |
| `mem::update_mem_usage (&mut StatsSegment, index, row, usage)` | 收缩为 `update_mem_usage (entry: &DirectoryEntry, usage: HeapUsage)`：VPP `stat_mem_usage_e` 的列常量（V1）与唯一的七列映射（V2），不建条目、不登记采集者，也不认识段（VPP 的 `cb = counters[0]` 写的正是 `d->entry`，V2） |
| `#[stats_collect_registration]` 属性宏（A12） | **保留**：只承载"更新"这一半（采集者登记），签名改为 `fn(&mut StatsMain) -> RuntimeResult<()>`（登记发生在 owner 发布之前，D9.1） |
| `register_num_worker_threads` / `register_worker_main_loop` 等自由函数登记项 | 换成 owner 模块里的声明；`/sys/num_worker_threads` 只有声明，值由 worker thread 域在 `start_workers` 写一次（不是采集者，没有采集登记项）；两个 per-worker vector 在 `Sys` 声明里，采集者由 `register_worker_main_loop` 登记 |

声明宏与 `#[stats_collect_registration]` 都只做自己那一半：声明宏不认采集者、不存堆
引用、不表达堆生命周期、不排序（顺序 = image 列表顺序）、不生成第二张全局表；
登记项不建条目（条目来自声明）、不解释列序（列序属于家族模块）。插件堆走同
一条路（自己的声明 + 自己的采集者登记），机制与 `init_stats_main` 不改。

**D9.6 需要批准的新增 surface（其余都是删除或收缩）：**`Collector` trait（两个
方法：`entry_index`、`collect`）、`StatsMain::register_collector
(&mut self, impl Collector + 'static)`、`collectors: Vec<Box<dyn Collector>>` 字段、
`DirectoryEntry::set_simple_counter_cell`/`set_scalar`、`StatsRegistration`/
`install` 改为 `fn(&mut StatsMain)`，D9.5 表里的三个字段属性，以及**一处 `dyn`
例外**（D9.2：本仓库默认禁止生产代码使用 trait 对象，本设计明确使用并给出理由）。

**D9.7 明确不进入设计：**第二个锁（表锁/独立的采集者互斥）、采集者持有或借用
`StatsSegment`（包括在轮次里再取段锁）、把实例状态或更新逻辑写成
`#[derive(Stats)]` 的字段属性（它们属于采集者类型，不属于条目声明）、
函数指针登记项与擦除的实例状态（`CollectorRegistration<P>` 的
`collect: fn(&mut CollectorData<'_, P>)` + `private_data: P` 那一版形状）、
`u64` 下标或 `&'static ()` 这类擦除指针、第二张全局表/注册表、
`mem::register_mem_heap`、`init_stats_main` 里的堆清单、采集者认识 `/mem` 列序之外
的 family 细节。

**D10 段锁：VPP `stat_segment_lockp` 的对应物，只保护目录结构变更与 `in_progress`/`epoch` 发布；值写与轮次不取它（V22、V32）。**
上一版把整个段放进 `SpinLock<StatsSegment>`，于是轮次被迫持锁；反过来把锁删掉也不是
设计——VPP 有一把真实的段锁（`stat_segment_lockp`，`stats.h:72`）。重新设计的是它的
**归属、保护对象与全部使用点**，本节把这三件事写到可直接实现的粒度。

**① 锁在哪、护什么。**VPP 的段锁是**进程私有**的 `clib_spinlock_t`
（`sm->stat_segment_lockp`，`stats.h:72`）；共享头里的 `in_progress`/`epoch` 只是给
客户端看的发布协议（seqlock 的一半）。Hammer 同形：锁是 `StatsSegment` 的字段、段由
进程级 `StatsMain` 拥有、**不进映射**。被护状态是从今天 `StatsSegment` 的两个裸字段
（`segment.rs:38-39`）搬进锁里的**名字表与空闲链头**——这才是 VPP 段锁真正保护的东西
（`sm->directory_vector_by_name`、`sm->dir_vector_first_free_elt`，
`stats.c:127,145,240`）。VPP 把这两格直接放在 `vlib_stats_segment_t` 上
（`stats.h:58-88`）；Rust 的 `SpinLock<T>` 需要一个被护类型，所以它们在这里构成
`Directory`——VPP 自己的词（`directory_vector`/`directory_vector_by_name`），字段名
逐字照搬；锁本身仍是段上的一格，名字也照搬（VPP `stat_segment_lockp` → 字段
`stat_segment_lock`）：

```rust
// crates/hammer-stats/src/segment.rs
// 依赖方向决定锁的原语只能来自 hammer-infra（hammer-runtime → hammer-stats），
// 现状如此：use hammer_infra::sync::SpinLock;
use hammer_infra::sync::{SpinLock, SpinLockGuard};

/// 段锁保护的目录状态，字段名照搬 VPP：`sm->directory_vector_by_name`
/// （名字→索引，`stats.c:145,240` 的 `hash_get_mem`/`hash_set_mem`）与
/// `sm->dir_vector_first_free_elt`（空闲索引链头，`stats.c:127`）。
///
/// 只有结构变更读写它；值写、轮次与任何采集者都不碰（V22、V32）。
struct Directory {
    directory_vector_by_name: HashMap<NameBytes, DirectoryIndex>,
    dir_vector_first_free_elt: Option<DirectoryIndex>,
}

pub struct StatsSegment {
    /// VPP `stat_segment_lockp`（`stats.h:72`）：一把只覆盖结构变更的段锁，共享头的
    /// `in_progress`/`epoch` 发布在同一个临界区里（`stats.c:11-52`）。
    /// 值写、轮次、任何采集者都永不进入它。
    stat_segment_lock: SpinLock<Directory>,
    update_interval: Duration,
    memory_size: usize,
    node_counters_enabled: bool,
    heap: NonNull<MemHeap>,
    mapping: NonNull<u8>,
    memfd: OwnedFd,
}
```

锁在段里、段在进程里，`StatsSegment` 因此要实现 `Sync`：映射内的值写是裸写
（唯一写者是 thread zero 的轮次与各 worker 的每线程原子，V29），结构变更由
`stat_segment_lock` 串行化；`Send` 保持不变（段只在创建期移动）。这与 ADR-0019 §6.1 对
`SpinLock<T>` 的用法一致：被保护的是**真实状态**（名字表/空闲链），不是"整段"。

**② 方法：VPP lock/unlock 对的 RAII 形式。**`vlib_stats_segment_lock`
（`stats.c:11-30`）的顺序是"先取 spinlock → 置 `shared_header->in_progress = 1`"；
`vlib_stats_segment_unlock`（`:32-52`）是"`epoch++` → release store 清
`in_progress` → 放 spinlock"。Hammer 用一个 guard 表达这一对，入口取 `&self`
（发布之后仍可做结构变更，不需要 `&mut StatsSegment`）：

```rust
impl StatsSegment {
    /// 取段锁：VPP `vlib_stats_segment_lock`（`stats.c:11-30`）。
    /// 方法名与字段名同形是照搬 VPP 的结果：字段是 `stat_segment_lockp`，方法就是
    /// `vlib_stats_segment_lock`；Rust 用调用语法区分字段与调用，两者都在这里。
    /// 这是**唯一**取锁的地方：其余结构操作全部走返回的 guard（见 ③）。
    fn stat_segment_lock(&self) -> StatSegmentLock<'_> {
        let directory = self.stat_segment_lock.lock(); // clib_spinlock_lock (sm->stat_segment_lockp)
        StatSegmentLock::acquire(self, directory)      // shared_header->in_progress = 1
    }

    /// 名字 → 索引：只读结构，查完立即放锁（VPP `stats.c:145` 的 `hash_get_mem`）。
    pub fn find(&self, name: &str, expected: DirectoryType) -> StatsResult<DirectoryIndex> {
        let name_bytes = NameBytes::try_from(name)?;
        let index = {
            let mut lock = self.stat_segment_lock();
            *lock
                .directory()
                .directory_vector_by_name
                .get(&name_bytes)
                .ok_or_else(|| StatsError::MetricNotFound { name: name.to_owned() })?
        }; // ← 临界区到此结束
        self.entry_of_type(index, expected)?; // 类型检查与值借出都在锁外
        Ok(index)
    }
}

/// 一次持有中的段锁：VPP `vlib_stats_segment_lock` 到 `vlib_stats_segment_unlock`
/// 之间的临界区（`stats.c:11-52`）。它同时持有段锁与目录状态的独占借用，
/// "忘记放锁""放两次"都不可表达。
struct StatSegmentLock<'a> {
    /// 字段顺序是发布顺序的一部分：`Drop::drop`（epoch++、清 `in_progress`）先跑，
    /// 之后这个 guard 才释放 spinlock（Rust 在 `Drop::drop` 返回后按声明顺序 drop 字段）。
    directory: SpinLockGuard<'a, Directory>,
    segment: &'a StatsSegment,
}

impl<'a> StatSegmentLock<'a> {
    /// 起锁：对应 `vlib_stats_segment_lock` 的两步，spinlock 已在调用方取到，
    /// 这里置 `shared_header->in_progress = 1`。
    fn acquire(segment: &'a StatsSegment, directory: SpinLockGuard<'a, Directory>) -> Self;

    /// 目录状态的独占借用：私有辅助函数都接收它（`*_locked`），不再自己取锁。
    fn directory(&mut self) -> &mut Directory;

    /// 这次临界区属于哪个段：`validate` 扩容要在锁内用段堆分配并发布新目录指针。
    fn segment(&self) -> &'a StatsSegment;
}

impl Drop for StatSegmentLock<'_> {
    /// VPP `vlib_stats_segment_unlock`（`stats.c:32-52`）：`epoch++`，再 release store
    /// 清 `in_progress`。两步都还在 spinlock 里，客户端因此看不到"半个 epoch"。
    fn drop(&mut self);
}
```

私有辅助函数保持 Hammer 自己的操作名并接收 guard 借用，例如
`create_entry_locked(&mut StatSegmentLock<'_>, …)`、`grow_directory_locked(…)`、
`remove_entry_locked(…)`：它们只在 guard 生命周期内被调用，因此**不复制 VPP 的
`n_locks` 重入计数与 `locking_thread_index`**（VPP 需要它们，是因为 C 里
lock/unlock 分开调、多个 helper 各自成对，`stats.c:11-52` 的
`if (sm->shared_header->in_progress && vm->thread_index == sm->locking_thread_index)
goto done;` 就是为这种情况；Rust 的 guard 让同一线程的重入不需要计数）。

**③ 使用点（全部取锁与不取锁的地方）。**

| Hammer 入口（都取 `&self`） | 取段锁 | 临界区里做什么 | VPP 出处 |
| --- | --- | --- | --- |
| `add_gauge`/`add_timestamp`/`add_simple_counter`/`add_combined_counter`/`add_histogram`/`add_name_vector`/`add_ring`/`add_symlink` | 取 | `create_entry_locked`：查重名 → 复用空闲索引或 `grow_directory_locked` 一次 → 写 `directory_vector_by_name` → 发布 `shared_header.directory_vector` | `stats.c:239-244`（新建并发布）、`:624-682`（ring 布局）、`:343-364`（name vector） |
| `remove_entry` | 取 | 摘 `directory_vector_by_name`、清条目 payload、索引挂回 `dir_vector_first_free_elt` | `stats.c:141-194` |
| `rename_symlink`/`set_name` | 取 | 名字表替换 + 条目名字体重写 | `stats.c:343-364` |
| `validate`（**仅** `will_expand` 分支） | 取 | 新尺寸锁外算好，锁内一次扩容 + 发布新目录指针 | `stats.c:481-524` |
| `find`（名字→索引） | 取 | 只读 `directory_vector_by_name`，查完立即放锁 | `stats.c:145` |
| 启动期登记遍历（`#[derive(Stats)]` 的 install、`Sys::bootstrap`、`run_stats_registrations`） | 取 | 与运行期同一条路径，**没有"启动期免锁"分支** | `init.c:118-131`（`vlib_stats_init` 里同样是 `add_counter_vector` + `validate` + `add_symlink`） |
| `entry`/`entry_of_type`/`heap()`/`segment_fd()`/`update_interval()` | 不取 | 按已发布索引取映射地址；读进程私有标量 | — |
| `set_gauge`/`set_timestamp`/`set_simple_counter`/条目级 `set_scalar`/`set_simple_counter_cell`/`advance_heartbeat` | 不取 | 映射内裸写（VPP `sm->directory_vector[index].value = value`） | `stats.c:263-269`、`:283-289` |
| `StatsMain::collect()` 与全部采集者 | 不取 | 取条目共享借用 → 写自己的单元格；末尾 heartbeat 同形 | `collector.c:131-151` |
| worker 的每线程计数累加 | 不取 | 写自己的 `AtomicU64`（Relaxed） | `collector.c:61-91` 只在**重建** node 计数器时取锁 |

**今天要删掉的取锁点**：值写与轮次路径上现有的 `stats_main.segment.lock()`——
`crates/hammer-stats/src/lib.rs:138`（`collect()` 的 heartbeat）、
`crates/hammer-runtime/src/config/stats.rs:191,198,213,251`（四个采集者）、
`:118,124,165,384`（启动期拿间隔/fd/固定槽的部分保留、改为 `&StatsSegment` 借用）。
`StatsMain.segment` 从 `SpinLock<StatsSegment>` 改回普通字段（§6.2 的 API 行）。

**④ 线程纪律。**结构变更只在 thread zero（`statseg-collector-process` 的属主）或
`WorkerBarrier` 停住 Data Worker 之后发生（§9 第 10 条）；worker 从不取段锁，只写
自己的每线程原子（VPP 的 per-thread node 计数同形）。guard 借 `&StatsSegment`，生命
周期被段绑定，不可能逃出段；"同一时刻只有一个结构写者"由"thread zero / barrier 之后
+ guard 独占"两条不变量保证——Hammer 不做 VPP 那种"多线程抢同一把段锁"的自旋设计。

**⑤ 临界区纪律。**只做结构变更本身：不 await、不 I/O、不调用任何采集者、不进
`WorkerBarrier`、不做可预测之外的分配；唯一的锁内分配是 `validate` 扩容，与 VPP 相同
（`stats.c:481-524`：`clib_mem_set_heap (sm->heap)` + 扩容向量；Hammer 是 A9 的
`activate()` 窗口内普通分配，耗尽即 abort）。VPP 的 `ASSERT (sm->locking_thread_index
== ~0)` 在这里由"guard 独占 + 唯一写者"两条不变量取代。

**⑥ 验收。**`stat_segment_lock.lock()` 只出现在 `StatsSegment::stat_segment_lock`
一处，其余结构操作全部经 guard（review 时按这一点检查代码，不做文本断言测试）；轮次与全部值写路径上不出现
guard；`in_progress` 只在 guard 生命周期内为 1、`epoch` 单调递增，且两者都在 spinlock
内发布；持锁期间没有 await/I-O/回调/Barrier；`validate` 扩容是唯一的锁内分配；持锁
时长在 M2 用 release 基准确认（结构变更不应出现在任何热路径上）。

## 5. 分层隔离契约

| 层 | 可以调用/持有 | 不可以调用/持有 | 验证边界 |
| --- | --- | --- | --- |
| hammer-infra | `MemHeap` mspace 采样、`MemMain` heap inventory、既有分配接口 | stats 类型/路径、`StatsSegment`、runtime 状态 | `usage()` 数值正确、不分配、不暴露 allocator 内部结构 |
| hammer-stats（机制） | 目录事务、`validate`、symlink、按索引写单元格、以自己拥有的段堆为活动堆的分配窗口（`activate()` + 普通分配，耗尽即 abort）、借出自己拥有的段堆、采集者登记表与轮次遍历（`register_collector`/`collect`） | `MemHeap`/`HeapUsage`、运行时堆实例、worker/thread index、runtime 反向依赖、socket/listener、family 专用方法（`add_memory_heap`/`add_own_memory_heap`/`set_heap_usage` 之类）；不知道任何堆名或 `/mem` 列序（名字与列序的 owner 是家族模块与各堆 owner）；`collect()` 的实现里不出现 family 名字 | 目录事务原子性、写后读回可见（H10）、`set_simple_counter` 边界、段内分配落在段堆而不是 main heap、耗尽走分配器失败路径、`collect()` 只依赖登记表 |
| `/mem` 家族模块（`hammer-stats::mem`） | 家族自己的列序：`STAT_MEM_*` 常量与 `update_mem_usage`（七列映射，写 `DirectoryEntry` 的行 0）；只调条目级写（`DirectoryEntry::set_simple_counter_cell`） | `MemHeap` 对象、堆集合、堆名、runtime/session 状态、`StatsSegment`/`StatsMain`；条目注册（那是 `#[derive(Stats)]`）、采集者登记（那是 owner 的注册项）；持有堆引用 | `/mem/<name>` 的 7 列与列序 = VPP `stat_mem_usage_e`（V1、V2）；家族模块里没有条目创建、没有堆名 |
| hammer-runtime（`main heap`/`stat segment`/主循环条目的 owner） | `Sys` 的 `#[derive(Stats)]` 声明与安装、两个堆条目声明（`path`/`columns`/`symlinks`）与两条 `HeapCollector` 实例的登记（`register_collector`）、worker thread 域的 `/sys/num_worker_threads` 声明与它在 `start_workers` 里的一次性写、`Sys` 的两个 per-worker vector 声明与它们的两条采集者登记（`MainLoopCountCollector`/`LoopsPerSecondCollector`）、`WorkerThread` 的两个每线程原子与 `ThreadMain` 解析、`DataPlaneMain` 的 VPP 同名窗口字段、node 的薄驱动 | 给机制加 family 字段/方法、在 stats 段里存业务副本、替插件拥有计数/堆、读写其它 worker 的私有状态、在 `init_stats_main` 里写堆清单、把家族步骤放进 node、新增第二个 process node | 声明与目录一致、堆条目/行宽/别名与列序、采集者登记与声明一一对应、worker 自增/窗口与投影、登记集合与登记顺序、安装顺序、worker 数量一致性、速率窗口语义与 VPP 一致（V13）、node 与 `collect()` 里没有堆名或家族调用 |
| API segment owner（`hammer-ipc`，镜像由 `hammer-service` 列出） | region 映射后的三条堆声明与三条 `HeapCollector` 实例的登记（root pvt / api pvt / api data，实例字段是 `&'static MemHeap`）、登记期一次的 `ApiMain::is_mapped()` 检查 | 给机制加 family 字段/方法、把 region 引用交给机制保存、在采集期间解引用已 unmap 的 region、在轮次里取任何锁（轮次不取段锁、不取 region 互斥量，值写也无锁，V29） | 三个条目与 region 堆一一对应、堆引用只在采集期间有效（unmap 在最后一个轮次之后）、堆名唯一 |
| 外部 stats client | `StatsClient` 的 stats 段只读 map/unmap 与 `names()`/`read()`；provider 经 `StatsReader` 读原始 `MetricValue`（含 `/sys/loops_per_worker` 速率列）；名字与列契约 | 家族方法进 `StatsClient`（`/mem`、`/sys` 语义进底层交互）、`dyn`/注册表式 provider 查找、服务端 heap 重建、写段、缓存跨 epoch 索引、服务端基线 | 只读映射、symlink/列解码、epoch 重试、provider 在 fixture 与真实映射上得到相同 report、错误分类 |

边界检查以编译、真实 lifecycle 和可观察行为为主；`rg` 只用于迁移删除清单，
不能替代行为证明。本 ADR 不新增跨层 wrapper：mem 采样结果是一个拥有数值的值
类型（`HeapUsage`），system 计数与速率是 runtime 自己的每线程状态与本地窗口。
两个 family 都用同一对通道表达：注册用 `#[derive(Stats)]` 声明（条目名、行宽、
别名），更新用该 owner 的一个采集者类型（VPP 的 `fn` +
`private_data` 两格）+ `#[stats_collect_registration]` 登记项（A12、
`register_collector`）；声明项与登记项都列进同一个 `RegistrationImage` 的
`stats_registrations`，列表顺序就是启动期执行顺序。

## 6. 新增/修改 API 审批清单

以下候选 surface 均为**拟议、未批准**。每项给出最终结果、owner/消费者，以及
为什么现有 surface 不足；没有这些条目，实施不应开始。随后的 6.1–6.6 小节给出
对应的 Rust 类型、字段与方法签名（路径相对仓库根，行号对本文基线）；这些定义
是待实现的设计，不表示已经编译通过，也不表示本仓当前存在这些类型。

| 项 | 最终结果、owner 与消费者 | 现有 surface 为什么不足 | 拟议变更与审批边界 |
| --- | --- | --- | --- |
| A1 | infra 提供"一次完整堆采样"这个 domain 操作；owner `hammer-infra`，消费者是 runtime 的 mem 采样步骤与 SSVM 现有调用点 | `MemHeap` 只有 `size()` 与 crate-private 的 `free_space()`；后者只取一个字段，且不能给出与 `total`/`used` 同源的采样 | 新增 `HeapUsage`（拥有数值，7 个 `u64` 字段，按 D2 命名）与 `MemHeap::usage(&self) -> HeapUsage`；删除 `free_space`。不返回借用、不暴露 `DlMallInfo`/mspace、不新增 heap getter |
| A2 | "按目录索引写一个值"的落盘路径，含 H10 根因删除；owner `hammer-stats`，消费者是 heartbeat/boottime 固定槽、`/sys/num_worker_threads`、mem 七列与之后所有 scalar/gauge/counter 写入者 | H10：`entry`/`entry_of_type` 返回 `DirectoryEntry` 拷贝（`segment.rs:843-867` + `protocol.rs:385-390` 的 `Copy`），所以 `set_gauge`/`set_timestamp` 的 `set_scalar_value(&mut self)` 落在临时值上；counter vector 单元格也没有任何控制面写入口。只加一个平行的可变借用入口会留下两条语义不同的写路径 | 目录访问改为借用：`entry`/`entry_of_type` 返回 `&DirectoryEntry`（只读），**删除拷贝返回的形状**；写值 API 取 `&self`、按已映射地址落盘（标量与单元格都是共享目录里的裸写，VPP `sm->directory_vector[index].value = value` 正是这个形状，V29），不返回 `Result`：`set_gauge(&self, index: DirectoryIndex, value: u64)`、`set_timestamp(&self, index: DirectoryIndex, value: u64)`、新增 `set_simple_counter(&self, index: DirectoryIndex, row: u32, column: u32, value: u64)` 都是 `()`，内部断言索引存在、类型匹配、行列在已发布形状内（消息带 index/type/row/column），失败即 panic（VPP 的 setter 同样没有错误路径，V20）；删除 `DirectoryEntry` 的 `Copy` derive（调用者改为借用，或在改结构前提取 `directory_type()`/`name_bytes()`/`data_pointer()` 这些 `Copy` 事实）。改目录结构的操作（`add_*`、`validate`、`add_symlink`、`remove_entry`、`rename_symlink`、`set_name`、`grow_directory`）取 `&self` + D10 的段锁（`StatsSegment.stat_segment_lock`），发布后仍可调用；值写与轮次都不取这把锁。不改布局、不加公共 getter、不新增 `*_row_mut`、不把写入口暴露给 worker 热路径 |
| A3 | stats 段把"自己拥有的堆"作为只读事实借出；owner `hammer-stats`，消费者是 runtime 的 `/mem/stat segment` 采样 | `heap()` 目前是私有方法（`segment.rs:868-872`），runtime 拿不到段堆的采样入口；复制一份堆指针到别处会引入第二个 owner。更新函数只拿到条目借用、既不取锁也不查全局，所以段堆引用由这条登记在自己登记时取一次（`stats_main.segment.heap()`）、放进 `private_data`（VPP 在 `vlib_stats_init` 里直接拿 `sm->heap`，`init.c:121`） | `pub fn heap(&self) -> &'static MemHeap`：可见性从私有改为公开，并把段堆的进程生命周期写进签名——控制块在段映射内、段由进程级 `StatsMain` 拥有且没有运行期 unmap/destroy 路径（与 `MemMain::main_heap()` 同一生命周期模型）。不新增 `own_heap`/`stats_heap` 别名、不改成拥有 `MemHeap` 的值、不新增第二个 heap getter；`HeapUsage` 仍是拥有数值的结果 |
| A4 | 共享协议不扩张；声明宏只在注册语义这一侧扩张 | — | 明确决定：协议不新增 `DirectoryType`、不改 `SharedHeader`/`DirectoryEntry` 布局、不提升 `STAT_SEGMENT_VERSION`（保持 2）、不新增客户端 `MetricValue` 变体。`#[derive(Stats)]` 只加三个**注册语义**字段属性（`path`/`columns`/`symlinks`，A12、6.5），`bootstrap` 语义不变且只服务固定槽；宏不承载更新语义（不加 `collector`/`private_data`） |
| A5 | `/mem/<heap>` 的注册与更新由每个堆的 owner 提供；owner 视堆而定（`hammer-runtime`、`hammer-ipc`/`hammer-service`、之后是插件），消费者是 `/mem/*` 与客户端 | Hammer 没有任何 `/mem/*` 目录项；而"一个中心结构体声明两个堆 + 两条固定采样函数 + `init_stats_main` 里两行清单"把"有哪些堆"写死在业务代码里，第三个堆（api segment）与插件堆都加不进来；旧的 `register_mem_heap` 还把条目创建与采集者登记合成一个机制函数，使"注册"与"更新"两种语义只有一个入口 | 每个堆在自己的 owner 模块里有两条项：一条 `#[derive(Stats)]` 声明（注册：`path`/`columns`/`symlinks`）与一条 `#[stats_collect_registration]` 项（更新：`register_collector (该 owner 的采集者)`）；两者都写进 owner 所在 crate/插件的 registration image，由 `run_stats_registrations(&mut stats_main)` 按列表顺序执行（声明在前、登记项在后）。`hammer-runtime` 提供 `main heap`、`stat segment`（同 `config/stats.rs`）；`hammer-ipc` 提供 api segment 的三个 region 堆（`binary_api/memory_shared.rs`），静态项列在 `hammer-service` 的镜像里。不新增中心 `Mem` 结构体、不新增 `memory_heaps`/`add_memory_heap`/`sample_memory_heaps`/`publish_*`/`MEMORY_HEAPS` 之类框架，也不新增任何 family helper 类型或函数 |
| A6 | `/sys` 三条新条目各自 owner 侧的登记：worker 数量 gauge、主循环两个 vector 的行宽与它们的采集者；owner `hammer-runtime`，消费者是 `/sys/*` 与客户端 | `Sys` 宏只声明条目（`install` 建项），gauge 取值与 `register_collector` 没有 owner 侧的登记项；把它们写进 `init_stats_main` 等于让 stats 初始化函数承担 family 步骤 | `Sys` 只增加 `main_loop_count_per_worker: SimpleCounter` 与 `loops_per_worker: SimpleCounter`（都不带 `bootstrap`，由宏的 `install` 创建，路径叶子名即 D3 的名字，行宽由字段属性 `columns = ThreadMain::global().worker_count()` 在 `install` 里 `validate(index, 0, columns - 1)`）；主循环新增 `#[stats_collect_registration] fn register_worker_main_loop`（生成 `__STATS_COLLECT_REGISTRATION_REGISTER_WORKER_MAIN_LOOP`，A12）：各挂一个本域的采集者类型（`MainLoopCountCollector`/`LoopsPerSecondCollector`，各自 `impl Collector`，内部读同一份 `ThreadMain::global()`）；VPP `vector_rate_collector_fn` 用一条函数加 `private_data` 服务两条条目，`init.c:16-56`、`:110-131`）。`/sys/num_worker_threads` 由 worker thread 域自己负责：`thread_main.rs` 新增 `WorkerThreadCount` 的 `#[derive(Stats)]` 声明（`path = "/sys/num_worker_threads"`，`Gauge`），值由该域在自己的生命周期点 `start_workers`（本域的 `main_loop_enter_function`）里 `set_gauge(index, ThreadMain::worker_count())` 写一次——它是"声明 + 本域自己写一次"，**不是采集者，因此不产生采集登记项、不进采集者表**（VPP 的 create 与 set 都在 `vlib_thread_init` 里，`threads.c:221-225,404`；此时已 `configure`，`WorkerCountZero` 保证非零）。`init_stats_main` 只剩 `StatsMain::create` → `Sys::bootstrap` → listener/FileMain → `run_stats_registrations(&mut stats_main)` → `StatsMain::set` |
| A7 | runtime 拥有每线程主循环计数与速率；owner `hammer-runtime` | `DataPlaneMain.main_loop_count` 是 worker 私有非原子值，`statseg-collector-process` node 不可读；跨线程直读是数据竞争。速率同理：collector 不能读 worker 的本地窗口 | `WorkerThread` 的每线程记录增加两个 cache-line 隔离的 `AtomicU64`：累计计数与 loops/s 发布值（备选：`ThreadMain` 中同布局的并行 `Vec`，但两套 index 需要证明一致，默认选前者；`DataPlaneMain` 持有 `&'static AtomicU64` 的方案见 6.3 的否决理由）；窗口状态直接放在 `DataPlaneMain` 上，字段名与 VPP `main.h:209-215` 逐一对应（`loops_this_reporting_interval`、`loop_interval_start`、`loop_interval_end`、`loops_per_second`、`damping_constant`），**不新增包装类型**；删除 `DataPlaneMain.main_loop_count`；`increment_main_loop_count(&mut self)` 保留 `&mut self`，做 VPP 相邻的两步（`main.c:1685` 的 `fetch_add(1, Relaxed)` 与 `:1689-1714` 的速率块），窗口到期时把阻尼值 `store` 进 `ThreadMain::worker_loops_per_second`（Relaxed，V13 语义）。不新增 wrapper 类型暴露原子地址 |
| A8 | 轮次由机制遍历采集者登记表，`statseg-collector-process` node 保持薄驱动；owner `hammer-stats`（表与遍历），登记项来自所有堆的 owner 与 `/sys` owner；消费者是这些家族与后续 family | 该 node 现在只设置 boottime 并每轮调用 `StatsMain::collect()`，而 `collect()` 只推进 heartbeat（H4）；没有登记入口，任何 family 只能硬编码进 `collect()` 或由调用者在外面拼一条链（上一版就是后者：runtime 的 `run_stat_segment_updates` 与 `init_stats_main` 的两行堆清单）。把家族写进 node 循环体会让 graph node 认识 `/mem`、`/sys` 与 worker 计数（VPP 用 `sm->collectors` 避开这一点，V14、V21、V26） | 机制新增 `Collector` trait、`StatsMain.collectors: Vec<Box<dyn Collector>>` 与 `StatsMain::register_collector(&mut self, impl Collector + 'static)`（ADR-0019 §6.6 的接口按本 ADR 修订，见 §9）；`StatsMain::collect(&self)` 改为 VPP `do_stat_segment_updates`：按登记顺序取出每个条目的 `&DirectoryEntry`（轮次不取锁，它就是 `collect` 的条目参数，D9.3），调用该表行，最后取 heartbeat 条目把值加一（D9.4）；两个函数都不返回 `Result`（A2、D4）。登记在启动期完成：每个堆的采集者由该堆 owner 的 `#[stats_collect_registration]` 项经 `register_collector` 挂进表（A5、A11、A12，旧的 `mem::register_mem_heap` 已删除），主循环的两个 `/sys` vector 由 `register_worker_main_loop` 登记两条采集者（A6）；此后表只读（本次登记集合：5 个堆各一条 + 主循环两条 = 7 条；`/sys/num_worker_threads` 不是采集者，因此不在表里——它由 worker thread 域在 `start_workers` 里写一次）。`stat_segment_collector_process` 的循环体保持 `stats_main.collect()`（不再有 `?`），boottime/取 interval/sleep 不变。不新增第二个 process node、不新增 Process 类型、不新增第二套 `dyn` 分发或注册表（表行那一处 `Box<dyn Collector>` 在 D9.2 里说明）、不在 runtime 里保留轮次函数、不把 runtime 计数搬进 stats 段作为唯一副本 |
| A9 | 固定容量堆耗尽不再建模为可恢复错误，段内分配也不再由调用点选堆；owner `hammer-stats`，消费者是目录注册、ring 注册与之后所有段内分配者 | 两件既有事实：`StatsError::HeapExhausted`（`lib.rs:99-103`，Display 在 `lib.rs:140-147`）把"固定堆装不下"变成 Err，调用者拿到它也没有合法恢复动作（段已发布、容量不能增长、声明过的目录不能撤销一半）；而段内分配现在是调用点手工选堆（`add_ring_buffer` 先 `activate()` 再 `segment_heap.allocate`，`allocate_vector` 干脆只靠 `heap.allocate`），可是 `activate()` 之后活动堆就是段堆、普通分配已经落在那里（H13、V24） | **删除 `StatsError::HeapExhausted`**（含 Display/Error impl 分支），并把段内分配统一为"`activate()` 窗口 + 窗口内普通分配"：`segment.rs:294-296` 的 ring payload 与 `segment.rs:961-1000` 的 `allocate_vector` 改成 `alloc::alloc_zeroed` + `alloc::handle_alloc_error`（V25 的 abort 路径，不是调用点的错误处理），`MemHeap::allocate` 从 stats 调用点消失、只为释放与向量头保留 `heap` 引用；`segment.rs:974`（尺寸溢出）与 `segment.rs:990`（长度超过 u32）改为 `StatsError::InvalidShape`。不保留兼容变体、不改名为 `CapacityExhausted`、不新增 `terminate_*`/`abort_*` helper、不新增诊断函数 |
| A10 | 客户端仓把"底层交互"与 family 语义分成两层，并给出标准家族的 typed report；owner `netsystem-client`（Rust binding），消费者是调用方与 CLI | 现有客户端只有 `connect`/`list`/`read -> MetricValue`；把 `memory_heap_usages`、`main_loop_counts` 这类家族方法直接加在 `StatsClient` 上，会让底层交互认识 `/mem`/`/sys` 语义，每加一个 family 都要改客户端核心，家族投影也无法在 fixture 上单测 | 新增 `StatsReader`（`names()`/`read()`，`StatsClient` 实现）与 `StatsProvider`（`const PREFIX`、`type Report`、`fn report<R: StatsReader>(reader: &R) -> Result<Self::Report, Error>`）；新增零尺寸 provider `MemoryStatsProvider`/`SystemStatsProvider` 与 typed report `MemoryStats`（含 `heap(name)` 查找）、`MemoryHeapUsage`、`SystemStats`（含 `main_loop_rates_per_second`）；`StatsClient` 只新增泛型入口 `report::<P>()`，**不加家族方法**；泛型静态分发，不加 `dyn`、不加注册表、不新增协议类型（签名见 6.4）。由客户端仓批准与实现 |
| A11 | `/mem` 家族契约有唯一 owner：`hammer-stats` 的家族模块 `mem`（VPP `vlib/stats/provider_mem.c` 的对应物），只承载家族列序；条目注册与采集者登记由各堆 owner 的声明/登记项表达 | 目录操作虽然都在，但"mem 条目 = 1 行 × 7 列 + total/used/free 三个别名"这套家族语义没有 owner 侧操作；留给每个堆自己写会复制列序与别名规则（VPP 把它们放在 `provider_mem.c` 一处）。而把它做成 `StatsSegment`/`StatsMain` 的方法等于把家族契约塞进底层类型：段对象会认识 `/mem`、列序与 `HeapUsage` | 新增模块 `crates/hammer-stats/src/mem.rs`，一个 `pub` 自由函数（不是 `StatsSegment`/`StatsMain` 的方法）：`update_mem_usage(entry: &DirectoryEntry, usage: HeapUsage)`——唯一的七列映射（VPP `stat_provider_mem_usage_update_fn` 的列映射部分，`provider_mem.c:26-39`，V2），写该条目的第 0 行，不返回 `Result`。条目名、行宽与三个别名是各自 owner 的 `#[derive(Stats)]` 声明；采集者登记是各自 owner 的 `#[stats_collect_registration]` 项。旧的 `mem::register_mem_heap` 删除（条目创建不再由机制代码做，V3 的机制部分按 `provider_mem.c` 的两半拆开）。家族模块不保存堆引用、不认识 `MemHeap`、不知道堆名，也不新增 family 类型；`StatsSegment`/`StatsMain` 上不出现任何 `/mem` 痕迹（6.2 的隔离边界） |
| A12 | 声明层新增 `#[stats_collect_registration]` 属性宏（现有 `#[stats_registration]` 的改名，A12）：owner 用属性宏声明一条采集登记项（在条目装好后 `register_collector` 挂上自己的采集者对象，不建条目），而不是手写 `static`；owner `hammer-component-macros`，消费者是 runtime 镜像、`hammer-service` 镜像与所有插件 | 现有 `#[derive(Stats)]` 只为"宏能表达的结构体字段"生成 `__STATS_REGISTRATION_<Struct>`（条目注册语义）；采集登记项是**函数**（`fn(&mut StatsMain) -> RuntimeResult<()>`），手写 `static` 会把静态项名字、`name` 字段与函数名各写一遍，也和 `#[init_function]`/`#[main_loop_enter_function]` 的声明方式不一致 | 新增无参数属性宏 `#[stats_collect_registration]`（与 `#[main_loop_enter_function]` 同形，实现见 6.5）：校验签名（safe/同步/非泛型/无 receiver/恰好一个 `&mut StatsMain` 参数/返回 `RuntimeResult<()>`）；生成 `#[doc(hidden)] pub static __STATS_COLLECT_REGISTRATION_<函数名大写>: StatsRegistration { name: stringify!(函数名), register: 函数 }`（`pub` 而不是 `pub(crate)`：hammer-service 的 image 要列 hammer-ipc 的项，跨 crate 可见性是必需的；静态项本身 `#[doc(hidden)]`）；把 `#[cfg]`/`#[cfg_attr]` 复制到静态项（与 `init_function` 相同）。登记项进入 image 的方式不变：内置 crate 在 `__declare_registration_image!(stats_registrations = [...])` 列出，插件在 `#[plugin(stats_registrations = [...])]` 列出。不新增 linkme 片段/全局注册表/构造器，不给宏加排序 qualifier、`bootstrap`、`collector = <fn>` 或堆名 |

### 6.1 hammer-infra：`HeapUsage` 与 `MemHeap::usage`

```rust
// crates/hammer-infra/src/mem/mod.rs

/// 一次 `mspace_mallinfo` 得到的堆使用事实。
///
/// 七个字段来自同一次调用；它拥有数值，不借用堆、不保存 mspace，也不暴露
/// allocator 内部表示。字段按事实命名（V8）：`free_chunk_count` 是个数，
/// `releasable_bytes` 是字节数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapUsage {
    pub total_bytes: u64,          // arena
    pub used_bytes: u64,           // uordblks
    pub free_bytes: u64,           // fordblks
    pub used_mmap_bytes: u64,      // hblkhd
    pub max_allocated_bytes: u64,  // usmblks
    pub free_chunk_count: u64,     // ordblks
    pub releasable_bytes: u64,     // keepcost
}

impl MemHeap {
    /// 采样一次堆使用。
    ///
    /// 只读 mspace 元数据：不分配、不取 mspace 锁（V19），因此可与分配并发；
    /// 返回的是一组同源字段，不承诺跨字段原子快照。
    pub fn usage(&self) -> HeapUsage {
        let info = unsafe { mspace_mallinfo(self.mspace) };
        HeapUsage {
            total_bytes: info.arena as u64,
            used_bytes: info.uordblks as u64,
            free_bytes: info.fordblks as u64,
            used_mmap_bytes: info.hblkhd as u64,
            max_allocated_bytes: info.usmblks as u64,
            free_chunk_count: info.ordblks as u64,
            releasable_bytes: info.keepcost as u64,
        }
    }

    // 删除：pub(crate) fn free_space(&self) -> usize   （mod.rs:862-864）
}
```

| 项 | 位置 | 动作 | 说明与迁移 |
| --- | --- | --- | --- |
| `HeapUsage` | `crates/hammer-infra/src/mem/mod.rs` | 新增（`pub`、`Copy`） | ADR-0019 §6.7 已把同一类型列入 A1 并排在它的 M4；本文把落地提前到本设计的 M0，是同一个类型，不并存两套 |
| `MemHeap::usage` | 同上（`size()`/`free_space()` 旁，`mod.rs:858-864`） | 新增 | 唯一对外采样操作；只读 `mspace`，不改变堆状态 |
| `MemHeap::free_space` | `mod.rs:862-864` | **删除** | 同一事实只留一个采样操作 |
| `svm/ssvm.rs` 调用点 | `crates/hammer-infra/src/svm/ssvm.rs:232-236` | 修改 | `let usage = segment.heap().expect("private SSVM heap is initialized").usage(); rnd_size = usize::try_from(usage.free_bytes).expect("heap free bytes fit the mapping size");`。`free_bytes <= MemHeap::size(): usize` 是同一不变量，所以这是断言而不是新的错误路径，不新增 `SsvmError` 变体 |
| 采样边界 | — | — | 不新增 `mallinfo()`/`mspace()` 或逐字段 getter；`DlMallInfo`（`mod.rs:38-49`）保持私有 |

### 6.2 hammer-stats：通用机制（写路径、段内分配、采集者登记表、固定堆耗尽）与 `mem` 家族模块

机制侧本轮的改动是写路径、段内分配、错误边界与**采集者登记表**：段对象本身
（`StatsSegment` 的字段、目录布局、epoch 语义）不变，新增的状态只有进程内的
`StatsMain.collectors`——VPP 把同一张表放在 `vlib_stats_segment_t` 里并注明
不进共享内存（`stats.h:58-62`）。`StatsMain::collect()` 不再是"只推进
heartbeat"：它成为 `do_stat_segment_updates` 的对应物（遍历登记 → heartbeat，
V14、V21、A8）。写值 API 不再返回 `Result`：它们是"按已发布索引写一个值"
（V20），和 VPP 的 `vlib_stats_set_gauge`/`vlib_stats_set_timestamp` 一样没有
错误路径。段的锁按 D10：一把只保护目录结构变更与 `in_progress`/`epoch` 发布的段锁
（VPP `stat_segment_lockp` 的对应物，`stats.h:72`、`stats.c:11-52`），结构操作取
`&self` + 这把锁，值写取 `&self` 直接落映射地址、不取锁，轮次整轮不取锁。表本身是**进程私有、无锁**的普通集合（D9.1、V29）：登记只发生在
单线程启动期，写者是 `&mut StatsMain`，轮次只按下标借用切片；段也不在锁里（值写取 `&self`，V29）。

```rust
// crates/hammer-stats/src/lib.rs（机制：登记表与轮次）

/// 一个采集者 = VPP `vlib_stats_collector_t` 表行的一行（`stats.h:51-57`）。
///
/// VPP 那一行把"更新逻辑"（`fn`）与"实例状态"（`private_data`）分成两格；这里由
/// **一个具体类型**承担两者：方法体是 `fn` 那一格，类型自己的字段是 `private_data`
/// 那一格（VPP 用 `u64` 存堆下标/池下标/gauge 索引，Hammer 用引用或下标，字段名按
/// 领域取，D9.2）。表是同质的 `Vec<Box<dyn Collector>>`；没有函数指针登记项、没有
/// 额外的数据参数、没有 `u64` 当指针、没有 `&'static ()` 擦除、没有手写 vtable
/// （D9.2 的 `dyn` 例外理由）。
pub trait Collector {
    /// 本行负责的目录条目（VPP 表行的 `entry_index`）。
    fn entry_index(&self) -> DirectoryIndex;

    /// 轮次里的那一次调用（VPP `c->fn (&data)`，`collector.c:137-146`）；参数是该
    /// 条目的共享借用（VPP 的 `data.entry`），采集者只写自己那几列单元格。
    fn collect(&self, entry: &DirectoryEntry);
}

impl StatsMain {
    /// 新增字段：`collectors: Vec<Box<dyn Collector>>`（私有、进程内、无锁）。
    ///
    /// 登记一个采集者；对应 VPP `vlib_stats_register_collector_fn`。
    ///
    /// 只在启动期（owner 发布之前、`statseg-collector-process` node 与 worker
    /// 启动之前）调用，因此取 `&mut self`：表在这里增长一次，之后只读，不需要
    /// 与轮次同步（V29 的表在 VPP 里同样不取锁）。没有可恢复失败：装箱与表增长
    /// 都是 Main Heap 上的普通分配，失败走分配器的 abort 路径（与 A9 同一条
    /// 失败路径，与段堆无关）。`+ 'static` 说的是采集者活到进程退出（表是进程级
    /// 的），不是"状态要擦成一个指针"。
    pub fn register_collector(&mut self, collector: impl Collector + 'static);

    // 这里没有 family 方法：`/mem` 的条目形状与列序属于家族自己的契约，
    // 放在 `crates/hammer-stats/src/mem.rs`（见下），不挂在本类型上（A11）。

    /// 一轮；对应 VPP `do_stat_segment_updates`（V14）。
    ///
    /// 按登记顺序"取条目 → 调用该登记的入口"，最后取 heartbeat 条目
    /// 把它的值加一（与 VPP 的最后一句同形，D9.4）——两者都是同一种"取条目 →
    /// 写值"，heartbeat 没有额外步骤。表不取锁（它只被启动期写过一次），段也
    /// 不取锁（值写都是映射地址上的裸写，与 VPP 的 `do_stat_segment_updates`
    /// 一致，V29）。实现里不出现任何 family 名字；
    /// family 只以登记参与。
    pub fn collect(&self);
}
```

`collect()` 的循环就是 VPP 那个循环（"取一条登记 → 解析条目 → 调用"，V14），
差别只在 Rust 用共享借用表达 VPP 的裸指针 `entry`、用 trait 方法表达裸函数指针：

```rust
impl StatsMain {
    pub fn collect(&self) {
        // 不取锁：写的是共享目录里的值，表只被启动期写过、段只被借用（VPP 同形，V29）。
        for collector in &self.collectors {
            // VPP: data.entry = sm->directory_vector + c->entry_index
            let entry = self
                .segment
                .entry(collector.entry_index())
                .expect("a registered collector names a live directory entry");
            // VPP: `c->fn (&data)`（collector.c:137-146）：表行在自己的
            // `Collector` 实现里写自己那几列（D9.2）。
            collector.collect(entry);
        }
        // 最后一条语句：取 heartbeat 条目写 value+1（V14、V28、D9.4）。与循环体里
        // 每个采集者同形——"取条目 → 写值"；现有的私有方法 `advance_heartbeat`
        // （`segment.rs:150-154`）就是这两行，保留它或直接内联都等价，都不新增
        // API、都不取锁。
        self.segment.advance_heartbeat();
    }
}
```

```rust
// crates/hammer-stats/src/segment.rs

impl StatsSegment {
    /// 写 simple counter vector 的一个单元格；行宽必须已由 `validate` 建立。
    ///
    /// 取 `&self`：写的是该行已映射的单元格地址，不取段锁（V29）；不分配、不 I/O、
    /// 不 await、不返回错误。调用者（runtime 的 owner 函数）只按索引写单元格，
    /// 机制不需要认识该单元格的语义；索引/类型/行列失配是本模块的 bug，按断言
    /// 终止（消息带 index、type、row、column）。
    pub fn set_simple_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        value: u64,
    );

    /// 本段自己的堆：控制块在段映射内，段由进程级 `StatsMain` 拥有且没有
    /// 运行期销毁路径，因此可以借出。是否把它采集成 `/mem/...` 由 owner 决定
    /// （D1），机制只借出事实。返回 `&'static MemHeap`：段映射与 VPP 的 stats
    /// 段一样活到进程退出、没有运行期 unmap/destroy 路径，`MemMain::main_heap()`
    /// 用的是同一个生命周期模型（A3）——采集者只拿到条目借用、不取锁也不查
    /// 全局，所以段堆的借用通常在登记期取得、放进该登记 owner 自己的槽里
    /// （VPP 的 `memory_heaps_vec` 是同一处；D9.2）。
    pub fn heap(&self) -> &'static MemHeap;  // 修改：私有 → pub，且借用是 'static（A3）

    /// 目录项的只读借用；不再返回拷贝（A2）。
    fn entry(&self, index: DirectoryIndex) -> StatsResult<&DirectoryEntry>;

    fn entry_of_type(
        &self,
        index: DirectoryIndex,
        expected: DirectoryType,
    ) -> StatsResult<&DirectoryEntry>;

    // 没有 `entry_mut*`：值写走共享借用 + 映射地址（`set_gauge`/`set_timestamp`/
    // `set_simple_counter` 都是 `&self`，见 A2）。改目录结构的操作
    // （`add_*`/`validate`/`add_symlink`/`remove_entry`/`rename_symlink`/
    // `set_name`/`grow_directory`）也取 `&self`：它们只经 `stat_segment_lock()`
    // 拿到的 guard 改结构（D10），所以 `&mut self` 在 API 上不出现。

    /// 与 `set_simple_counter` 同一条写路径（A2）；两者都取 `&self`、都不返回
    /// `Result`：写的是已映射的标量槽，与 VPP 的 `…value = value` 同形（V29）。
    pub fn set_gauge(&self, index: DirectoryIndex, value: u64);
    pub fn set_timestamp(&self, index: DirectoryIndex, value: u64);
}
```

**写路径（A2）：一次改完，不保留拷贝写分支。**

```rust
// 现状（写丢）：`segment.rs:194-204` 在 `entry_of_type` 返回的拷贝上写，
// 而 `protocol.rs:448-451` 的 `set_scalar_value(&mut self)` 只改这个临时值。
self.entry_of_type(index, expected)?.set_scalar_value(value);

// 拟议：读写都取 `&DirectoryEntry` 借用，`set_scalar_value` 也改为 `&self`、
// 按该条目的映射地址落盘（标量写 `data.value`，单元格写行内的第 column 个
// `u64`，与现有 `set_simple_counter` 用 `AtomicU64::from_ptr(…)` 的写模型同
// 一条）；`DirectoryEntry` 不再实现 `Copy`，所以"拷贝一份再写"在类型上不再
// 成立。返回类型同时去掉 `Result`：索引/类型是声明期的常量，写不上去是本模块
// 的 bug（VPP 的 setter 也没有错误路径，V20）。
self.entry_of_type(index, expected).set_scalar_value(value);
```

借用化会暴露原先被拷贝掩盖的借用冲突，处理方式是**先提取被拷贝的事实**，再取
可变借用：`entry_of_type(...)` 读 `directory_type()`/`name_bytes()`/
`data_pointer()`（三者都是 `Copy` 的小值），随后 `directory_mut()` 换名或换
数据指针；`remove_entry`、`rename_symlink`、`set_name` 都按这个形状迁移。这不
新增 API，只是把原来隐式的拷贝变成显式的局部值。

`set_simple_counter` 写的单元格不在目录里，而在已发布的 counter vector 行内，
因此它不推进 `epoch`：目录项的指针没有变化，改变的是数据列。客户端读取规则见
D7 第 4 条。

**`/mem` 家族契约放在它自己的模块 `crates/hammer-stats/src/mem.rs`。**VPP 的
七列映射（`stat_provider_mem_usage_update_fn`，V2）与别名创建（V3）都在
`vlib/stats/provider_mem.c` 这个独立文件里，不在通用段操作里。Hammer 照同一
划分：`StatsSegment`/`StatsMain` 只有通用目录与写操作，`/mem` 的列序是家族契约，
放在家族模块里，以自由函数出现——不是 `StatsSegment`/`StatsMain` 的方法，所以
底层类型里不出现 `/mem`、`HeapUsage` 或 `stat_mem_usage_e` 的任何痕迹。条目
形状（名字、行宽、三个别名）是**注册语义**，由各堆自己的 `#[derive(Stats)]`
声明表达（D9.5），不在这里。

```rust
// crates/hammer-stats/src/mem.rs

/// 把一次堆采样写进该条目的第 0 行：列序 = VPP `stat_mem_usage_e`
/// （V1，D2），与 VPP `stat_provider_mem_usage_update_fn`（V2）是同一处映射。
///
/// VPP 的更新函数写的是 `counter_t **counters = d->entry->data;
/// counter_t *cb = counters[0];`（`provider_mem.c:33-39`），行固定为 0；Hammer
/// 照此写 `entry` 的第 0 行，不写段、不返回 `Result`（与 A2 同一条写路径）。
/// 参数是已采到的 `HeapUsage` 值，不是堆对象；调用者是每个堆自己的采集者
/// （在 owner 模块里）。条目名、行宽、别名不在这里（它们在 owner 的声明里）。
pub fn update_mem_usage(entry: &DirectoryEntry, usage: HeapUsage);
```

**隔离边界（实现时按此验收）：**`mem` 模块可以调用的只有条目级写
（`DirectoryEntry::set_simple_counter_cell`）；它不认识 `MemHeap` 对象、不知道
有哪些堆、堆叫什么名字，也不持有堆引用或 runtime 状态，不建条目、不登记采集者。
反过来，`StatsSegment`/`StatsMain` 不出现 `/mem`、`HeapUsage`、`stat_mem_usage_e`
的任何痕迹。

| 项 | 位置 | 动作 | 说明 |
| --- | --- | --- | --- |
| `Collector` | `crates/hammer-stats/src/lib.rs` | 新增（`pub` trait，方法 `entry_index`/`collect`） | VPP `vlib_stats_collector_t` 表行的一行（`stats.h:51-57`）：`entry_index()` 是那一行的条目身份，`collect (&self, entry: &DirectoryEntry)` 是那一行的 `fn`。实现者是一个**具体类型**，它自己的字段就是 VPP `private_data` 那一格（`&'static MemHeap`、`&'static ThreadMain`、`&'static ApiMain`；D9.2 含 `dyn` 例外说明） |
| `StatsMain.collectors` | `crates/hammer-stats/src/lib.rs:31-34` | 新增（私有字段） | 进程内登记表（VPP `sm->collectors`，`stats.h:58-62` 明确不进共享内存）；进程私有的普通集合、不取锁、不进 `StatsSegment`，启动期由 `&mut StatsMain` 写入、轮次只读（D9.1） |
| `register_collector` | `crates/hammer-stats/src/lib.rs` | 新增（`pub`，`&mut self`，`impl Collector + 'static`） | VPP `vlib_stats_register_collector_fn`：把采集者装箱后追加进表；只在启动期、owner 发布与 node/worker 启动之前调用；返回 `()`（装箱与表增长是 Main Heap 上的普通分配，A9）。`+ 'static` 说的是采集者活到进程退出，不是"把状态擦成一个指针" |
| 采集者与声明的边界 | `crates/hammer-stats/src/lib.rs` | 新增的表面边界 | `#[derive(Stats)]` 不出现采集者或实例状态参数（注册与更新分离，D9.5）；实例状态由采集者类型自己的字段携带（VPP 在登记点写 `r.private_data = …`，`provider_mem.c:69`） |
| `set_simple_counter` | `segment.rs` | 新增（`pub`，取 `&self`） | 单元格写（已映射地址），返回 `()`；只接受已 `validate` 的行宽，不隐式扩容，失配即断言（index/type/row/column） |
| `register_mem_heap` | `crates/hammer-stats/src/mem.rs` | **删除** | 条目/行宽/别名改由各堆的 `#[derive(Stats)]` 声明表达（注册语义，D9.5）；采集者登记由各堆 owner 的 `#[stats_collect_registration]` 项表达（更新语义）。机制里的 `/mem` 创建代码消失 |
| `update_mem_usage` | `crates/hammer-stats/src/mem.rs`（新模块） | 新增（`pub` 自由函数） | `/mem` 家族契约；VPP `stat_provider_mem_usage_update_fn`（`provider_mem.c:26-39`）的列映射部分（A11；V1、V2）：`update_mem_usage (entry: &DirectoryEntry, usage: HeapUsage)` 是唯一的七列映射（列序 = D2 = VPP `stat_mem_usage_e`），写 `entry` 的第 0 行，不返回 `Result`，不了解段、不建条目、不登记采集者（那些在 owner 的声明与登记项里） |
| `set_gauge` | `segment.rs:194-198` | **修改实现与签名** | 改为按映射地址落盘（`&self`）并返回 `()`（V20、V29） |
| `set_timestamp` | `segment.rs:200-204` | **修改实现与签名** | 同上；`advance_heartbeat`（`segment.rs:150-154`，取 `&self`）就是轮次最后那句“取 heartbeat 条目 → 写 value+1”（D9.4），与采集者同一条写路径，既不是第二条路径，也不是一个锁步骤 |
| `entry`、`entry_of_type` | `segment.rs:843-867` | **修改返回类型** | 返回 `&DirectoryEntry`；值写走共享借用，改目录结构前先提取 `Copy` 事实 |
| `StatsMain.segment` | `crates/hammer-stats/src/lib.rs` | **修改** | 从 `SpinLock<StatsSegment>` 改回普通字段 `segment: StatsSegment`（保持 `pub`：owner 的登记项要读 `stats_main.segment.heap()` 把它存进采集者自己的字段）：锁移进段内、只覆盖结构变更（D10）；值写取 `&self`，所以 `StatsSegment` 要按映射的共享写模型实现 `Sync`，`StatsMain` 才能提供 `&self` 的轮次 |
| `Directory`、`StatsSegment::stat_segment_lock`、`StatSegmentLock` | `crates/hammer-stats/src/segment.rs` | 新增/修改（均私有）。`stat_segment_lock` 即 VPP `stat_segment_lockp` 的对应物（`stats.h:72`、`stats.c:11-52`）：只保护目录结构变更与 `in_progress`/`epoch` 发布，值写、轮次、任何采集者都永不进入该临界区（D10、V22、V32） | D10 的 Rust 形式：锁保护的真实状态是名字表 + 空闲链头（从 `StatsSegment:38-39` 的裸字段搬入），字段名照搬 VPP（`directory_vector_by_name`、`dir_vector_first_free_elt`）；`stat_segment_lock(&self) -> StatSegmentLock<'_>` 是唯一取锁入口（`stats.c:11-30` 的两步：spinlock → `in_progress = 1`）；`StatSegmentLock` 持有 `SpinLockGuard`，`Drop` 里按 `stats.c:32-52` 的顺序（`epoch++` → release store 清 `in_progress` → 放 spinlock）发布，私有辅助函数改为 `*_locked(&mut StatSegmentLock<'_>, …)`。不新增公共表面、不复制 VPP 的 `n_locks` 重入计数 |
| `DirectoryEntry` | `protocol.rs:385-390` | **删除 `Copy` derive** | 让"拷贝目录项"不再是可用的形状；布局与 `Clone` 不变，`size/align` 断言（`protocol.rs:772-776`）不受影响 |
| `heap` | `segment.rs:868-872` | 修改可见性与返回借用 | 私有 → `pub`，返回 `&'static MemHeap`（A3）：段映射活到进程退出、没有运行期 unmap/destroy 路径（与 `MemMain::main_heap()` 同一生命周期模型）；采集者只拿条目借用、不取锁也不查全局，所以段堆的引用在登记期取一次、存进该采集者自己的字段。不新增别名、不新增第二个 heap getter |
| `StatsMain::collect` | `crates/hammer-stats/src/lib.rs:75-78` | **修改签名与实现** | 成为 VPP `do_stat_segment_updates` 的对应物：按登记顺序取出每个条目的 `&DirectoryEntry`（不取锁）并调用表行（VPP 的 `data.entry = … + c->entry_index`），最后取 heartbeat 条目写它的值 +1（同一形状，D9.4）；返回 `()`（写值不失败，A2）。实现里不出现任何 family 名字，文档注释不再与实现不一致（H4） |
| `StatsError` | `crates/hammer-stats/src/lib.rs:88-123` | **删除一个变体（A9）** | 删除 `HeapExhausted`（`lib.rs:99-103` 与 Display 分支 `lib.rs:140-147`）；其余复用 `Protocol`/`DuplicateName`/`MetricTypeMismatch`/`InvalidShape`/`CapacityTooSmall`，不新增变体 |
| 段内分配点 | `segment.rs:234,294-296,428,533,562,809,961-1000` | **修改** | 统一为"`activate()` 窗口 + 窗口内普通分配"：ring payload（`:294-296`）与 `allocate_vector`（`:961-1000`）改调 `alloc::alloc_zeroed`（失败即 `alloc::handle_alloc_error`），`heap` 只留在向量头与释放路径；调用点不再出现 `MemHeap::allocate`；只为释放或为手工选堆而开的活动堆 guard（`:340,529,679`）删除（见下方 A9） |

**mem family 的安装事务由各 owner 的声明与登记项承载。**每个堆的注册语义是
一条 `#[derive(Stats)]` 声明（`path`/`columns`/`symlinks`），内部只用现成操作：
`add_simple_counter` → `validate(index, 0, columns - 1)` → 三个 `add_symlink`；
更新语义是该堆自己的一个采集者类型 + 一条 `register_collector` 登记项。两者的
执行时机都由 owner 自己 image 的 `stats_registrations` 列表决定（D6、A5、A6、
A8、A11）。因此 `StatsSegment`/`StatsMain` 上不出现 `add_memory_heap`/
`set_heap_usage` 这类 family 方法，`init_stats_main` 与轮次里不出现任何堆名；
新 family/新堆都按同一方式接入：一条声明（注册）+ 一条采集者登记（更新）。

**固定堆耗尽与"谁来选堆"（A9）：**段堆是固定容量映射，注册期装不下不是可恢复
错误：已发布的目录不能撤销一半，容量也不能增长，因此调用者拿不到合法恢复动作。
VPP 在这条路径上有两个形状，Hammer 现在两个都只做了一半：

- **分配点不携带错误路径。**stats 段的分配都发生在 `clib_mem_set_heap (sm->heap)`
  窗口里，窗口内的 `clib_mem_alloc_aligned` / `clib_mem_heap_realloc_aligned`
  失败即 `os_out_of_memory()` → `os_panic()` → `abort()`，调用点没有任何返回值
  可检查（V24、V25）。Hammer 的同一形态是 `MemHeap::activate()` 窗口 + 窗口内
  普通分配：`MemMain` 把 `alloc`/`alloc_zeroed` 路由到活动堆，也就是这个段堆
  （H13），所以 `alloc::alloc` 返回空指针时调用 `alloc::handle_alloc_error`
  （abort）就是耗尽路径——既不需要 `segment_heap.allocate`，也不需要
  `Result`/`Option` 建模。
- **窗口只覆盖分配本身。**窗口内不做别的分配（诊断字符串、`Vec` 都不进这个
  窗口，避免误落在段堆里）；释放**不**需要窗口：`heap.deallocate`
  （`mod.rs:826-848`）以 owning heap 为接收者、自己就是 free 边界检查，
  与 C 侧 `vec_free` 必须靠活动堆（`stats.c:155-180` 的 `set_heap`）不是一回事。
  因此现有 `segment.rs:340,529,679` 那几个只为释放/为手工选堆而开的活动堆 guard
  一并删除。

```rust
// crates/hammer-stats/src/segment.rs（改后的分配形状，ring payload 与
// allocate_vector 内部各开一次窗口，调用点不出现堆选择）
let layout = Layout::from_size_align(requested, alignment).expect("vector layout is valid");
let active_heap = heap.activate(); // 窗口：活动堆 = 段堆
// SAFETY: `layout` has non-zero size; exhaustion is the allocator's failure path.
let pointer = unsafe { alloc::alloc_zeroed(layout) };
let Some(pointer) = NonNull::new(pointer) else {
    alloc::handle_alloc_error(layout) // 段已发布、容量不可增长：就此终止
};
drop(active_heap);
```

`allocate_vector` 仍以 `heap` 为参数，但参数只用于向量头里的 owning heap 与
`free_vector` 的归还路径；分配本身走上面的形状，窗口开在函数内部、每次分配一次，
`add_name_vector`/`set_name`/`validate`/`grow_directory` 都不再关心堆。
`MemHeap::allocate`/`allocate_zeroed` 保留在 infra（全局分配器自身、
`HeapBoxed`、bihash、hammer-ipc 的 API region 仍用它），本次只改 `hammer-stats`
的调用形状。

| 现有构造点 | 现状 | 删除 `HeapExhausted` 后 |
| --- | --- | --- |
| `segment.rs:294-296`（ring payload） | `activate()` 之后手工 `segment_heap.allocate` | 同一个窗口内 `alloc::alloc_zeroed` + `handle_alloc_error`（V24、V25） |
| `segment.rs:981`（vector 分配） | **没有窗口**的手工 `heap.allocate` | 窗口开在 `allocate_vector` 内，分配走普通分配器（H13） |
| `segment.rs:974`（`element_size * count + prefix` 溢出） | `HeapExhausted { requested: usize::MAX, .. }` | `StatsError::InvalidShape`（形状不可表示，不是容量事实） |
| `segment.rs:990`（元素数超过 u32 长度域） | `HeapExhausted` | `StatsError::InvalidShape` |

`StatsError::CapacityTooSmall` 保留：它比较的是配置尺寸与"header 页 + 初始
目录"的最小需求，发生在段创建、发布之前，调用者可以据此报告一条启动配置错误；
它不描述运行期的堆余量，也不允许在已发布段上重试。

**`set_simple_counter` 的写入前提（自己模块的 bug，用断言而不是 `Result`）：**

| 前提 | 不成立时 |
| --- | --- |
| `index` 在当前目录内，且 `directory_type()` 是 `CounterVectorSimple` | 断言失败：索引/类型与声明不一致 |
| `row`/`column` 在 `validate` 建立的已发布形状内，`data_pointer()` 非空 | 断言失败：写到了没有建立的行列（不隐式 `validate`、不分配） |
| 命中时 | 从共享借用的 `entry` 取 `data_pointer()`（`protocol.rs:453-468`）→ 外层向量取第 `row` 行 → 以 `AtomicU64` 写第 `column` 个 `u64`（与 `…value = value` 同一条裸写模型，V29）；单次 `u64` 存储，客户端不得据此声称跨列快照（D7 第 4 条） |

### 6.3 hammer-runtime / hammer-ipc：堆登记、`/sys` 条目与每线程计数

mem 家族没有中心结构体，也没有把 `/mem` 契约挂到机制类型上：每个堆的 owner 在
自己的模块里给出**注册语义（一条 `#[derive(Stats)]` 声明：名字、行宽、别名）**
与**更新语义（一个采集者类型 + 一条 `register_collector` 登记项）**，两步都
列进自己的 image（A11、A12、D9.5）。`main heap`/`stat segment` 的声明与采集者
登记项在 `hammer-runtime` 的 `config/stats.rs`（与 `Sys`、
`statseg-collector-process` node 同模块），api segment 的三个 region 堆在
`hammer-ipc` 的 `binary_api/memory_shared.rs`（生成的静态项由 `hammer-service`
的镜像列出）；`init_stats_main` 里不出现任何堆名或用量步骤。

`/sys` 的三条新增条目按 VPP 的归属分开写：worker 数量 gauge 属于 worker thread
域（VPP 在 `threads.c:221-225` 创建 gauge、`:404` 设置 `n_vlib_mains - 1`），
登记项放在 `crates/hammer-runtime/src/thread_main.rs`；两个 per-worker vector
属于主循环的速率发布（VPP 在 `stats/init.c:110-131` 由 stats owner 创建并登记
collector），登记项与 collector 放在 `config/stats.rs`。

```rust
// crates/hammer-runtime/src/config/stats.rs

/// 固定 /sys 槽 + 主循环自己的两个 per-worker vector。
///
/// 宏把结构体名小写为命名空间并按 `/sys/<字段名>` 生成路径
/// （`component-macros/src/lib.rs:240,560`）；既有注释与 `#[allow(dead_code)]`
/// 保持不变（`config/stats.rs:86-96`）。`/sys/num_worker_threads` 不在这里：
/// worker 数量属于 worker thread 域，条目由它自己的声明创建、值由它自己的生命
/// 周期点写入（VPP 由 `threads.c` create + set，见下）。
#[derive(Stats)]
pub(crate) struct Sys {
    #[stats(bootstrap = hammer_stats::STAT_COUNTER_HEARTBEAT)]
    heartbeat: Timestamp,
    #[stats(bootstrap = hammer_stats::STAT_COUNTER_LAST_STATS_CLEAR)]
    last_stats_clear: Timestamp,
    #[stats(bootstrap = hammer_stats::STAT_COUNTER_BOOTTIME)]
    boottime: Timestamp,
    /// 每个 Data Worker 一列的累计主循环次数（权威累计值）。
    /// 路径取默认（`/sys/<字段名>`）；行宽是启动期事实（worker 数）。
    #[stats(columns = ThreadMain::global().worker_count())]
    main_loop_count_per_worker: SimpleCounter,  // 新增 → /sys/main_loop_count_per_worker
    /// 每个 Data Worker 一列的 loops/s（VPP 同名速率投影，V13）。
    #[stats(columns = ThreadMain::global().worker_count())]
    loops_per_worker: SimpleCounter,            // 新增 → /sys/loops_per_worker
}

// ---- 堆采集者：一个类型 + 每堆一个登记项（VPP 注册点的后一半） ----

/// VPP `stat_provider_mem_usage_update_fn`（`provider_mem.c:33-45`，V26）的对应物：
/// 一个类型服务所有堆——实例的两个字段就是 VPP `vlib_stats_collector_reg_t` 的
/// `entry_index` 与 `private_data` 两格（V27、V30；`provider_mem.c:69` 的
/// `r.private_data` 是 provider 自己集合里的堆下标）。七列映射在
/// `hammer-stats::mem` 只有一份（A11），所以采集者只有一个行为，也不返回
/// `Result`（A2、D4）。
struct HeapCollector {
    entry_index: DirectoryIndex,
    heap: &'static MemHeap,
}

impl Collector for HeapCollector {
    fn entry_index(&self) -> DirectoryIndex {
        self.entry_index
    }

    fn collect(&self, entry: &DirectoryEntry) {
        hammer_stats::mem::update_mem_usage(entry, self.heap.usage());
    }
}

/// 主堆的注册语义：`/mem/main heap` = 1 行 × 7 列 + `total`/`used`/`free`
/// 三个别名（`provider_mem.c:44-63`）。
#[derive(Stats)]
pub(crate) struct MainHeapUsage {
    #[stats(
        path = "/mem/main heap",
        columns = STAT_MEM_RELEASABLE + 1,
        symlinks = [("total", STAT_MEM_TOTAL), ("used", STAT_MEM_USED), ("free", STAT_MEM_FREE)],
    )]
    usage: SimpleCounter,
}

/// 主堆的采集者登记项。就是 VPP 注册点的最后几句：
/// `r.private_data = …; r.collect_fn = …; vlib_stats_register_collector_fn (&r)`
/// （`provider_mem.c:66-71`，V30、D9.2）。
#[stats_collect_registration]
fn register_main_heap(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    stats_main.register_collector(HeapCollector {
        entry_index: MainHeapUsage::global().usage.index,
        heap: MemMain::main_heap(),   // VPP: r.private_data（:69）
    });
    Ok(())
}

/// `/mem/stat segment`：与主堆只差"哪个堆"——这正是 `HeapCollector` 存在的理由
/// （VPP 也只有一条采集函数、每个堆注册一次）。段没有运行期销毁路径，引用在登记期
/// 取一次（V4、V18、A3）。
#[derive(Stats)]
pub(crate) struct StatSegmentUsage {
    #[stats(
        path = "/mem/stat segment",
        columns = STAT_MEM_RELEASABLE + 1,
        symlinks = [("total", STAT_MEM_TOTAL), ("used", STAT_MEM_USED), ("free", STAT_MEM_FREE)],
    )]
    usage: SimpleCounter,
}

#[stats_collect_registration]
fn register_stat_segment_heap(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    stats_main.register_collector(HeapCollector {
        entry_index: StatSegmentUsage::global().usage.index,
        heap: stats_main.segment.heap(),   // 段堆，'static（A3）
    });
    Ok(())
}

// ---- worker 主循环的两个 /sys vector：本域自己的采集者 ----

/// 每个值都来自该 worker 自己的每线程发布原子，轮次只复制，不重算、不做差分；
/// 列 `slot` 即 Data Worker `slot`（runtime thread index `slot + 1`，与
/// `start_workers.rs:25-26` 的约定一致），采样不分配、不返回 `Result`。
/// VPP 用一条函数加文件静态/`private_data` 同时服务两条条目（`init.c:16-56`、
/// `:118-131`）；Hammer 两条条目各一个采集者类型，读同一个 `ThreadMain::global()`。
struct MainLoopCountCollector {
    threads: &'static ThreadMain,
}

impl Collector for MainLoopCountCollector {
    fn entry_index(&self) -> DirectoryIndex {
        Sys::global().main_loop_count_per_worker.index
    }

    fn collect(&self, entry: &DirectoryEntry) {
        for slot in 0..self.threads.worker_count() {
            let thread_index = slot + 1;   // 线程 0 是 main thread，不跑主循环（H7）
            let value = self
                .threads
                .worker_main_loop_count(thread_index)
                .load(Ordering::Relaxed);
            entry.set_simple_counter_cell(0, slot, value);
        }
    }
}

struct LoopsPerSecondCollector {
    threads: &'static ThreadMain,
}

impl Collector for LoopsPerSecondCollector {
    fn entry_index(&self) -> DirectoryIndex {
        Sys::global().loops_per_worker.index
    }

    fn collect(&self, entry: &DirectoryEntry) {
        for slot in 0..self.threads.worker_count() {
            let thread_index = slot + 1;
            let value = self
                .threads
                .worker_loops_per_second(thread_index)
                .load(Ordering::Relaxed);
            entry.set_simple_counter_cell(0, slot, value);
        }
    }
}

/// 主循环两条 per-worker vector 的登记项：行宽已经由 `Sys` 声明的 `columns`
/// 建立（D9.5），这里只登记两个采集者。VPP 在 `stats/init.c:118-131` 也是同一个
/// 形状（先建条目与 `validate`，再 `vlib_stats_register_collector_fn`）。
#[stats_collect_registration]
fn register_worker_main_loop(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    let threads = ThreadMain::global();
    stats_main.register_collector(MainLoopCountCollector { threads });
    stats_main.register_collector(LoopsPerSecondCollector { threads });
    Ok(())
}

// crates/hammer-runtime/src/thread_main.rs（worker thread 域的 owner 侧）

/// `/sys/num_worker_threads`：条目的名字与值都由本域给出（VPP 在
/// `threads.c:223-225` 创建 gauge、`:404` 设置 `n_vlib_mains - 1`）。列数仍然
/// 不写死：客户端从两个 per-worker vector 的长度推导（D3 第 1 条）。它不是每轮
/// 变化的量，所以**没有采集者**：只有一条 `#[derive(Stats)]` 声明，值由本域在
/// 自己的生命周期点写入。
#[derive(Stats)]
pub(crate) struct WorkerThreadCount {
    #[stats(path = "/sys/num_worker_threads")]
    count: Gauge,
}

// crates/hammer-runtime/src/start_workers.rs（本域的 main_loop_enter_function）

#[hammer_component_macros::main_loop_enter_function]
fn start_workers(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    // ... 既有的 worker 启动步骤 ...
    // worker 集合在这里定稿并启动，所以本域在这里写自己声明的槽（VPP 的同一句在
    // `vlib_thread_init` 里，`threads.c:404`：create 与 set 在同一个函数）。它走
    // A2 的"按索引写一个值"，不是采集者，也不进采集者表。
    StatsMain::global()?.segment.set_gauge(
        WorkerThreadCount::global().count.index,
        u64::from(ThreadMain::global().worker_count()),
    );
    Ok(())
}
```

`#[stats_collect_registration]` 的静态项名 = `__STATS_COLLECT_REGISTRATION_<函数名大写>`，可见性
是 `#[doc(hidden)] pub`（与 `init_function` 的 `pub(crate)` 不同：api segment 的
项要由 `hammer-service` 的 image 跨 crate 列出；与 `#[init_function(name = …)]`
的 `__INIT_FN_<name 大写>` 命名规则同源，A12）；`#[derive(Stats)]` 生成的声明项
名 = `__STATS_REGISTRATION_<结构体名>`。七个项列进
`crates/hammer-runtime/src/lib.rs:15-35` 的 `stats_registrations`（条目来自两个
模块，列表顺序 = 执行顺序；`Sys::install` 必须在前，每个采集者登记项必须排在它
引用的声明项之后——`register_*` 读的是声明的 `global()` 索引）：

```rust
stats_registrations = [
    config::stats::__STATS_REGISTRATION_Sys,                        // 注册：固定槽 + 两条 /sys vector
    config::stats::__STATS_REGISTRATION_MainHeapUsage,              // 注册：/mem/main heap
    config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_MAIN_HEAP,         // 更新：该堆的采集者
    config::stats::__STATS_REGISTRATION_StatSegmentUsage,           // 注册：/mem/stat segment
    config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_STAT_SEGMENT_HEAP, // 更新：该堆的采集者
    config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_WORKER_MAIN_LOOP,  // 更新：两条 /sys 采集者
    thread_main::__STATS_REGISTRATION_WorkerThreadCount,            // 注册：/sys/num_worker_threads
];
```

`init_stats_main`（`config/stats.rs:145-176`）因此只剩机制与固定槽，没有堆清单、
没有 family 步骤。owner 在登记遍历期间**尚未发布**（`collectors` 需要 `&mut`），
所以遍历传的是本地 owner，发布是最后一步：

```rust
#[hammer_component_macros::init_function(name = "stats_main_init")]
fn init_stats_main(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    let config = stats_config();
    let mut stats_main = StatsMain::create(/* 现有参数不变 */)?;  // 未发布
    Sys::bootstrap(&stats_main.segment)?;                         // 固定槽 0/1/2（H11）
    let listener = bind_listener(&config.socket_name)?;          // 现有逻辑不变
    FILE_MAIN
        .get()
        .expect("FileMain is initialized before stats startup")
        .add(File::new(/* 现有实参不变 */))?;
    // 遍历所有 image 的登记项：注册（条目）与更新（采集者）都在这条路上
    // （A5、A6、A11、A12）；这期间只有本线程，没有轮次。
    crate::init::run_stats_registrations(&mut stats_main)?;
    StatsMain::set(stats_main);   // 发布（OnceLock::set）；此后按值不再可变
    Ok(())
}
```

`worker_count - 1` 不下溢：`WorkerCountZero` 在 `configure` 时就拒绝 0
（`config/worker.rs:115-117`、`thread_main.rs:257-259`），而 `init_stats_main`
运行在 `configure` 之后；`MemMain` 也已在进程启动时创建
（`crates/hammer/src/main.rs:70`）。

**api segment 的三条堆登记（`hammer-ipc` 定义，`hammer-service` 的镜像列出）：**
`api-segment` 配置由 early config function 解析并在这里 `ApiMain::install()`
（`hammer-service/src/binary_api.rs:667-690`），`binary_api_init`（`:700-759`）
创建 root region（`/global_vm`，NODATA）并映射 api region（DATA_HEAP）：root
region 先 push、api region 随后 push，`primary_rp` 指向 api region
（`memory_shared.rs:865-869`）。两个 region 由 `ApiMain.mapped_shmem_regions`
持有，退出时 `unmap_shared_regions()`（`hammer-service/src/binary_api.rs:548-549`，
实现 `memory_shared.rs:882-890`）。`ApiMain::root_region()`/`primary_region()`
就是这两个存储的只读借用（A5），不是新包装。三个堆各有：一条注册声明（名字按
region 名 + 角色派生）+ 一条采集者登记；采集者也是那个唯一的 `HeapCollector`，
堆身份放在实例自己的 `heap` 字段里（VPP 把同一件事放在 `private_data` 里，V26、
V30、D9.2）。`HeapCollector` 的字段是 `&'static MemHeap`，所以这三条只在**登记期**
判一次 `ApiMain::is_mapped()`——`binary_api_init` 先映射再让登记跑到（见下面的
顺序），映射一定在；此后轮次里不再判、也不取 region 锁（D2）。

```rust
// crates/hammer-ipc/src/binary_api/memory_shared.rs（api segment 的 owner 侧）

// ---- 三条注册声明（注册语义）：名字按 region 名 + 角色派生（D2） ----
//
// root region 名是 `/global_vm`；api region 名来自 `ApiSegmentConfig`（默认
// `/vpe-api`，叶子形式 `vpe-api`，early config function 写入 `ApiMain`，H8），
// 所以 `path` 是 install 期求值的表达式——VPP 的名字同样是注册期格式化出来的
// （`add_counter_vector ("/mem/%U", format_clib_mem_heap_name, heap)`，V3）。
// `api_region_leaf()` 是本 owner 自己的名字派生函数（VPP 的
// `format_clib_mem_heap_name` 的对应物）；名字形式由本 owner 定。

#[derive(Stats)]
pub(crate) struct RootRegionPvtUsage {
    #[stats(
        path = "/mem/global_vm pvt",
        columns = STAT_MEM_RELEASABLE + 1,
        symlinks = [("total", STAT_MEM_TOTAL), ("used", STAT_MEM_USED), ("free", STAT_MEM_FREE)],
    )]
    usage: SimpleCounter,
}

#[derive(Stats)]
pub(crate) struct ApiRegionPvtUsage {
    #[stats(
        path = format!("/mem/{} pvt", api_region_leaf()),
        columns = STAT_MEM_RELEASABLE + 1,
        symlinks = [("total", STAT_MEM_TOTAL), ("used", STAT_MEM_USED), ("free", STAT_MEM_FREE)],
    )]
    usage: SimpleCounter,
}

#[derive(Stats)]
pub(crate) struct ApiRegionDataUsage {
    #[stats(
        path = format!("/mem/{} data", api_region_leaf()),
        columns = STAT_MEM_RELEASABLE + 1,
        symlinks = [("total", STAT_MEM_TOTAL), ("used", STAT_MEM_USED), ("free", STAT_MEM_FREE)],
    )]
    usage: SimpleCounter,
}

// ---- 三条堆实例的采集者登记项（VPP 对每个堆各调一次 `vlib_stats_register_mem_heap`） ----

/// 采集者还是那个唯一的 `HeapCollector`（D2、D9.2）：三条登记只差"哪个堆"。
/// region 的映射存在性只判一次——没有映射就没有堆可采，三条条目保持 0（D6）；
/// 有映射时取到的 `&'static MemHeap` 在采集期间一直有效：本 init 函数里映射先于
/// 登记，而唯一解除映射的 `unmap_shared_regions()` 在 main-loop exit 函数
/// （`exit_binary_api`）里跑，那已是最后一个轮次之后。
#[stats_collect_registration]
fn register_api_segment_heaps(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    let api_main = ApiMain::current();
    if !api_main.is_mapped() {
        return Ok(());
    }
    stats_main.register_collector(HeapCollector {
        entry_index: RootRegionPvtUsage::global().usage.index,
        heap: api_main.root_region().pvt_heap(),
    });
    stats_main.register_collector(HeapCollector {
        entry_index: ApiRegionPvtUsage::global().usage.index,
        heap: api_main.primary_region().pvt_heap(),
    });
    stats_main.register_collector(HeapCollector {
        entry_index: ApiRegionDataUsage::global().usage.index,
        heap: api_main
            .primary_region()
            .data_heap()
            .expect("mapped API region has a Data Heap"),
    });
    Ok(())
}

```

三条 `#[derive(Stats)]` 声明生成的静态项与那一条采集登记项（在 `hammer-ipc` 里
都是 `pub`）由 `crates/hammer-service/src/lib.rs:3-46` 的
`__declare_registration_image!` 列进它的 `stats_registrations`（与
`hammer-service` 自己的 `init_functions` 同一份镜像；`hammer-ipc` 是它的依赖，
不需要新镜像）：

```rust
stats_registrations = [
    hammer_ipc::binary_api::memory_shared::__STATS_REGISTRATION_RootRegionPvtUsage,
    hammer_ipc::binary_api::memory_shared::__STATS_REGISTRATION_ApiRegionPvtUsage,
    hammer_ipc::binary_api::memory_shared::__STATS_REGISTRATION_ApiRegionDataUsage,
    hammer_ipc::binary_api::memory_shared::__STATS_COLLECT_REGISTRATION_REGISTER_API_SEGMENT_HEAPS,
];
```

**顺序：**登记项在 `run_stats_registrations()` 里执行时要"region 已映射"与"三条
声明已安装"，两件事都由列表顺序保证。映射那件事今天已经成立：`hammer-service`
的 `#[init_function(name = "binary_api_init")]` 带
`runs_before = ["stats_main_init"]`
（`crates/hammer-service/src/binary_api.rs:700-703`），因此 `init_stats_main`
做登记遍历时 region 一定已映射；若这条初始化没跑（api 未映射），登记项里的
`is_mapped()` 判一次就返回，三条条目保持 0（D6，不是错误）。

owner 侧需要的只是 `ApiMain` 已持有 region 的只读借用：实施时给 `ApiMain` 加
crate-private 的 `root_region() -> &SvmRegion` 与 `primary_region() -> &SvmRegion`
（`pub(crate)`、返回真实借用、不新增 wrapper、不缓存指针；`hammer-ipc` 的
`mapped_shmem_regions`/`primary_rp` 是同一份状态）。

**一个 infra 小口子（需批准）：**region 的堆引用现在只能在 `RegionLock` 上取
（`crates/hammer-infra/src/svm/region.rs:402,408`），返回的借用绑在 guard 上、
只有 `SvmRegion::lock()` 能给出 guard——而 guard 活不过一轮，所以那个借用存不进
采集者的字段。要像 VPP 那样"登记期拿一次堆指针"，需要在 `SvmRegion` 上加一对只读
入口 `pvt_heap() -> &MemHeap` 与 `data_heap() -> Option<&MemHeap>`：它们读的是
region header 里创建期写一次、此后不再变的两个指针，与 region 互斥量保护的状态
（client 列表/子 region）无关，因此不需要取锁；现有 `RegionLock::{pvt_heap,
data_heap}` 保持不动（`remove_exited_clients` 等仍在用）。这样 api 的三条采样与
VPP 的 `stat_provider_mem_usage_update_fn` 完全同形：只读堆指针、不取任何锁
（`mspace_mallinfo` 本身不加 mspace 锁，见 6.1、V19），轮次里也没有跨进程互斥量。
**退路：**若不加这对入口，api segment 就得另有一个采集者类型，每轮在
`is_mapped()` 之后借 region 锁、复制出 `HeapUsage` 再写条目（一轮多取一次跨进程
互斥量，且那一条与 VPP 不同形）——"只有一个采集者类型"与"不新增 infra 入口"
之间必须选一个。

```rust
// crates/hammer-runtime/src/worker_thread.rs

#[repr(C)]
pub struct WorkerThread {
    cacheline0: CacheLineAlignMark,
    barrier: OnceLock<crate::WorkerBarrier>,
    cacheline1: CacheLineAlignMark,
    thread_index: u32,
    // ... 既有只读描述字段不变 ...
    join_handle: OnceLock<JoinHandle<RuntimeResult<()>>>,
    cacheline2: CacheLineAlignMark,   // 新增：两个计数与描述字段不同行
    main_loop_count: AtomicU64,       // 新增：累计计数，本线程 fetch_add、轮次读
    loops_per_second: AtomicU64,      // 新增：阻尼速率发布值；VPP 写 counter_t 时也截断成整数
}

const _: () = {
    // 既有断言（worker_thread.rs:33-42）不变，并新增：
    assert!(core::mem::offset_of!(WorkerThread, main_loop_count) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(WorkerThread, loops_per_second) % CACHE_LINE == 0);
};

impl WorkerThread {
    /// 本线程的主循环计数。
    #[inline]
    pub(crate) fn main_loop_count(&self) -> &AtomicU64 {
        &self.main_loop_count
    }

    /// 本线程最近一次窗口的 loops/s（截断为 u64，与 VPP 写 `counter_t` 相同）。
    #[inline]
    pub(crate) fn loops_per_second(&self) -> &AtomicU64 {
        &self.loops_per_second
    }
}
```

```rust
// crates/hammer-runtime/src/thread_main.rs

impl ThreadMain {
    /// `thread_index` 对应 Data Worker 的主循环计数。
    ///
    /// `configure_fields` 先 push thread zero、再按 thread index 升序 push 每个
    /// Data Worker（`thread_main.rs:155-193,213-246`），`register_thread` 只追加
    /// （`thread_main.rs:327-361`），因此 thread index 直接索引描述符；不新增
    /// 第二份 index 映射。
    #[inline]
    pub(crate) fn worker_main_loop_count(&self, thread_index: u32) -> &AtomicU64 {
        self.worker_descriptor(thread_index).main_loop_count()
    }

    /// `thread_index` 对应 Data Worker 最近发布的 loops/s。
    #[inline]
    pub(crate) fn worker_loops_per_second(&self, thread_index: u32) -> &AtomicU64 {
        self.worker_descriptor(thread_index).loops_per_second()
    }

    /// 两个访问器共用的解析规则：thread index 直接索引描述符。
    #[inline]
    fn worker_descriptor(&self, thread_index: u32) -> &WorkerThread {
        self.worker_threads
            .get(thread_index as usize)
            .filter(|descriptor| descriptor.thread_index() == thread_index)
            .expect("Data Worker descriptors are stored by thread index")
    }
}
```

```rust
// crates/hammer-runtime/src/data_plane/main.rs（worker 本地、非共享）

/// VPP 把这些字段直接放在 `vlib_main_t` 上（`main.h:209-215` 的 "Dispatch loop
/// time accounting"），Hammer 同样直接放在 worker 自己的 `DataPlaneMain` 上：
/// 没有包装类型、没有第二份状态，只有拥有它的 worker 读写，不需要同步原语。
/// 字段名与 VPP 逐一对应；VPP 另有 `seconds_per_loop`（`main.h:214`），Hammer
/// 没有任何读者（VPP 只给 show 命令用），因此不新增该字段。
pub struct DataPlaneMain {
    // ... 既有字段不变 ...
    /// 本报告间隔内完成的主循环次数（VPP `loops_this_reporting_interval`）。
    loops_this_reporting_interval: u64,
    /// 当前报告间隔的起点；`None` = 还没有完整窗口（VPP 用 0 作同一状态的哨兵）。
    loop_interval_start: Option<Instant>,
    /// 当前报告间隔的终点（VPP `loop_interval_end`）。
    loop_interval_end: Instant,
    /// 最近一次窗口算出的阻尼 loops/s（VPP `loops_per_second`）。
    loops_per_second: f64,
    /// 构造时算一次 `exp(-1.0/20.0)`；VPP 在 `main.c:1974` 算一次后存进
    /// `vlib_main_t`（`damping_constant`）。
    damping_constant: f64,
}
```

**窗口与阻尼常量的出处（VPP 注释与代码不一致）：**VPP 的 `damping_constant`
是 `exp (-1.0 / 20.0)`，初始化处的注释写"每 20ms 采样一次、半衰期 1 秒"
（`main.c:1965-1975`），但主循环实际把下一个窗口边界设为 `now + 2e-4`
（`main.c:1713`，即 200µs）。Hammer **逐字复制代码里的这一对值**（200µs 窗口 +
`exp(-1/20)` 阻尼），既不按注释"修正"成 20ms，也不改用与 200µs 匹配的常数：
读数因此与 VPP 的 `/sys/loops_per_worker` 可比。若将来 VPP 改正这个不一致，
按本 ADR 的修订流程同步。本项是已决定的"复制 VPP 代码值"，§9 第 9 条只写它的
验收口径，不重新决定窗口大小。

```rust
// crates/hammer-runtime/src/data_plane/worker.rs

impl DataPlaneMain {
    /// VPP 主循环里相邻的两步：`vlib_increment_main_loop_counter`
    /// （`main.h:426-434`，调用点 `main.c:1685`）与"报告间隔到期就更新
    /// `loops_per_second`"的块（`main.c:1689-1714`）。累计计数发布进每线程记录
    /// （Hammer 的 collector 在别的线程读，必须是原子），速率窗口留在
    /// `DataPlaneMain` 自己的字段里；两个值都只由拥有它的 worker 写。
    #[inline]
    pub(crate) fn increment_main_loop_count(&mut self) {
        let threads = ThreadMain::global();
        threads
            .worker_main_loop_count(self.thread_index)
            .fetch_add(1, Ordering::Relaxed);
        self.loops_this_reporting_interval += 1;                 // main.c:1687

        let now = Instant::now();
        if now < self.loop_interval_end {
            return;                                              // main.c:1692
        }
        if let Some(start) = self.loop_interval_start {
            let elapsed = now.duration_since(start).as_secs_f64();
            if elapsed > 0.0 {
                let interval_rate = self.loops_this_reporting_interval as f64 / elapsed;
                self.loops_per_second = self.loops_per_second * self.damping_constant
                    + (1.0 - self.damping_constant) * interval_rate;
                threads
                    .worker_loops_per_second(self.thread_index)
                    .store(self.loops_per_second as u64, Ordering::Relaxed);
            }
        }
        self.loop_interval_start = Some(now);                    // main.c:1712
        self.loop_interval_end = now + Duration::from_micros(200);   // main.c:1713
        self.loops_this_reporting_interval = 0;
    }
}
```

| 项 | 位置 | 动作 | 说明 |
| --- | --- | --- | --- |
| `Sys::{main_loop_count_per_worker, loops_per_worker}` | `config/stats.rs:85-97` | 新增字段 | 非 bootstrap 字段由宏的 `install` 创建（`add_simple_counter`，`component-macros/src/lib.rs:325-353`），路径叶子名即 D3 的名字；`/sys/num_worker_threads` 不由 `Sys` 声明（它的 owner 是 worker thread 域，VPP 同样在 `threads.c` 创建） |
| `Sys.{main_loop_count_per_worker, loops_per_worker}` 的 `columns` 属性 | `config/stats.rs:85-97` | 新增属性 | 行宽 = `ThreadMain::global().worker_count()`（install 期求值），对应 VPP `vlib_stats_validate (reg.entry_index, 0, vlib_get_n_threads ())`（`init.c:130-132`）；不再由登记项手工 `validate` |
| `init_stats_main` | `config/stats.rs:145-176` | 修改 | 删掉原来的 gauge/`validate`/别名与 mem 步骤；只剩 `StatsMain::create` → `Sys::bootstrap` → listener/FileMain → `run_stats_registrations(&mut stats_main)` → `StatsMain::set`；`Sys::bootstrap` 与 listener 顺序不变（A5、A6、A11、D9.1） |
| `stat_segment_collector_process`（`statseg-collector-process` process node 的工厂） | `config/stats.rs:99-121` | 修改（只去掉 `collect()` 的 `?`；循环体、节点与注册不变） | `stats_main.collect()?` → `stats_main.collect()`；boottime/取 interval/sleep 不变；node 里没有 `collect_*`，不新增第二个 process node |
| `update_main_loop_count`、`update_loops_per_second` | `config/stats.rs` | 新增（两个模块私有采集者类型 `MainLoopCountCollector`/`LoopsPerSecondCollector`，字段 `threads: &'static ThreadMain`） | 每个 per-worker vector 一个采集者类型（VPP `vector_rate_collector_fn` 用一条函数加文件静态服务两条条目，`init.c:16-56,118-131`；Hammer 的实例状态是同一个 `ThreadMain`，D9.2）；各登记一次（A8）；不新增 API surface、不返回 `Result` |
| `MainHeapUsage`、`StatSegmentUsage` 声明 | `crates/hammer-runtime/src/config/stats.rs`；宏生成的静态项列入 `crates/hammer-runtime/src/lib.rs:15-35` | 新增（两条 `#[derive(Stats)]`，注册语义） | `/mem/main heap` 与 `/mem/stat segment`：`path` + `columns = STAT_MEM_RELEASABLE + 1` + 三个 `symlinks`（V1、V3、V30） |
| `register_main_heap`、`register_stat_segment_heap`、`register_worker_main_loop` | `crates/hammer-runtime/src/config/stats.rs`；宏生成的静态项列入 `crates/hammer-runtime/src/lib.rs:15-35` | 新增（三个 `#[stats_collect_registration]` 函数，A12，更新语义） | 每条登记只做 `register_collector (该 owner 的采集者)`；不建条目（条目来自声明）；image 列表顺序 = 执行顺序，登记项排在声明项之后 |
| `WorkerThreadCount` 声明 + `start_workers` 里的一次性设置 | `crates/hammer-runtime/src/thread_main.rs`（声明）与 `crates/hammer-runtime/src/start_workers.rs`（设置）；宏生成的声明项列入 `crates/hammer-runtime/src/lib.rs:15-35` | 新增（一条 `#[derive(Stats)]`，A6；外加本域 `start_workers` 里的一句 `set_gauge`） | 本域的声明建 `/sys/num_worker_threads`，本域在自己的生命周期点 `set_gauge` 一次（VPP `threads.c:221-225,404` 同处 create+set）；**没有采集登记项**、不登记采集者 |
| `HeapCollector` | `config/stats.rs` | 新增（模块私有采集者类型，字段 `entry_index` + `heap: &'static MemHeap`，A8、D2） | **一个类型服务所有堆**——VPP 也只有一条 `stat_provider_mem_usage_update_fn`（V26、V30），堆身份在实例字段里（VPP 的 `private_data`）；每堆一个实例：主堆取 `MemMain::main_heap()`（V5），段堆取 `stats_main.segment.heap()`（A3），api 的三个取各自 region 的堆（下一行）；不返回 `Result` |
| `hammer-ipc` 的三条 api segment 声明与一条采集者登记 | `crates/hammer-ipc/src/binary_api/memory_shared.rs`（宏生成的静态项 `pub`，列进 `crates/hammer-service/src/lib.rs:3-46` 的 `stats_registrations`） | 新增（三条 `#[derive(Stats)]` + 一条 `#[stats_collect_registration]` 登记项，该登记项注册三个 `HeapCollector` 实例，A12） | 名字由 region 名派生（`/mem/global_vm pvt`、`/mem/<region> pvt`、`/mem/<region> data`）；登记期判一次 `ApiMain::is_mapped()`（A5、D2）；实例字段是 `&'static MemHeap`（来自 region 的 `pvt_heap`/`data_heap`），轮次里不取锁（D9.2、6.3 的 infra 小口子） |
| `SvmRegion::{pvt_heap, data_heap}` | `crates/hammer-infra/src/svm/region.rs:402,408` | 新增（`pub`，只读 header 里创建期写一次的堆指针，不取 region 互斥量） | 让 owner 在登记期取到能存进采集者字段的 `&MemHeap`/`Option<&MemHeap>`；现有 `RegionLock::{pvt_heap, data_heap}` 保持不动（6.3） |
| `ApiMain::{root_region, primary_region}` | `crates/hammer-ipc/src/binary_api/api.rs` | 新增（`pub(crate)`，返回 `&SvmRegion`） | 只读借用 `mapped_shmem_regions`/`primary_rp` 里的 region；不新增 wrapper、不缓存指针 |
| `hammer-ipc` 依赖 | `crates/hammer-ipc/Cargo.toml` | 新增 `hammer-stats` | 声明与采集者要命名 `StatsMain`/`Collector`/`DirectoryIndex`，并调用 `mem::update_mem_usage`；与 `hammer-runtime → hammer-stats` 同向，无环（AGENTS.md 依赖表同步补这一行） |
| `WorkerThread.{cacheline2, main_loop_count, loops_per_second}` | `worker_thread.rs:14-31` | 新增字段 | 每线程记录在 worker 启动前构造（`configure_fields`/`register_thread`）；worker 只访问自己 thread index 的条目 |
| `WorkerThread::{main_loop_count, loops_per_second}` | `worker_thread.rs` | 新增（`pub(crate)`） | 只读借用；不提供 `store`/`swap` 等第二套入口 |
| `ThreadMain::{worker_main_loop_count, worker_loops_per_second, worker_descriptor}` | `thread_main.rs:321` 之后 | 新增（`pub(crate)`） | 两个 per-worker 采集者的唯一解析入口；不复用线性查找 `thread_by_index`（`thread_main.rs:321-325`） |
| `DataPlaneMain.main_loop_count` | `data_plane/main.rs:43` 与 `Debug`（`main.rs:73`）；初始化 `buffer_pool.rs:127` | **删除** | 避免"本地计数 + 每线程原子计数"两份漂移 |
| `DataPlaneMain::increment_main_loop_count` | `data_plane/worker.rs:20-23` | 修改（签名保持 `&mut self`） | `wrapping_add` → 每线程 `fetch_add(1, Relaxed)`，再就地做 VPP 的速率块（`main.c:1689-1714`），窗口到期时发布 loops/s（Relaxed）；调用点 `main_loop.rs:137` 不变 |
| `DataPlaneMain.{loops_this_reporting_interval, loop_interval_start, loop_interval_end, loops_per_second, damping_constant}` | `data_plane/main.rs` | 新增字段（worker 本地私有） | 字段名与 VPP `main.h:209-215` 逐一对应，**不新增包装类型**；非共享、无锁；不在热路径分配、不访问共享段 |
| `main_loop.rs` 步骤注释 | `main_loop.rs:83` | 修改 | 说明该计数由 runtime 拥有的每线程原子计数承担 |

**否决的备选（不写进实现）：**

- **`DataPlaneMain` 持有 `&'static AtomicU64`：**那是把 `ThreadMain` 拥有的存储
  地址缓存进 worker 本地状态；该借用只有在 `worker_threads` 不再增长时才成立，
  而 `register_thread`（`thread_main.rs:327`）是追加型 `&mut self` API，没有
  类型级保证。每轮多做一次 `ThreadMain::global()` 读取与循环里既有的
  `crate::barrier::global()`（`main_loop.rs:91`）同形；若 M2 的 release/LTO
  基准显示这次读取可测，要改回缓存指针必须重新审批。
- **`ThreadMain` 里另开并行 `Vec<AtomicU64>`：**会引入"描述符数组 + 计数数组"
  两套 index，需要额外证明一致；计数放进每线程描述符本身只需要一条
  `worker_main_loop_count` 解析规则。
- **让 worker 直接写 stats 单元格（VPP 的直接写法）：**见 D4 与 ADR-0019
  §6.5；worker 行安装是另一个未决所有权问题，本 ADR 不预置它的替代实现。
- **速率基线放在采样函数侧（collector 保存上一轮计数 + 时间戳）：**那是把
  owner 的窗口状态放进机制的表或某个静态量，既与 VPP 的"worker 主循环维护
  `loops_per_second`、collector 只复制"（V13）不符，也会让"谁保存基线"成为新的
  未决所有权问题；速率窗口属于 worker 自己，必须是它的本地状态。
- **每 N 次循环才读一次时钟：**VPP 每个循环都做一次时间比较（`main.c:1692`），
  窗口边界因此不会被循环次数掩盖；改成固定 N 会让窗口随循环速率漂移。若 M2 的
  release/LTO 基准显示 `Instant::now()` 可测，再单独批准降频策略，不能默认引入。
- **给累计计数换一个 VPP 名字（例如把 `/sys/main_loop_count_per_worker` 直接叫
  `loops_per_worker`）：**同名不同语义会让以 VPP 为参照的使用者读错；两个条目
  都发布，各自语义写进 D3/D7。

### 6.4 stats client（外部仓）

分层见 §4 D7；本仓不新增客户端类型，也不新增协议类型。以下是跨仓契约的候选
签名：由 `netsystem-client` 仓批准与实现，名字可按该仓风格调整，但"底层不认识
family、provider 只经 `StatsReader`、泛型静态分发"这三条边界不随名字变化。

```rust
// hammer-stats-client（外部仓的 Rust binding）

/// 底层交互对 provider 暴露的全部能力：家族枚举与原始读取。
///
/// `StatsClient` 是唯一生产实现；内存 fixture 也实现它，因此家族投影
/// 不需要 socket 或映射即可单测。
pub trait StatsReader {
    fn names(&self) -> Result<Vec<String>, Error>;
    fn read(&self, name: &str) -> Result<MetricValue, Error>;
}

/// 一个 family 的客户端投影。
pub trait StatsProvider {
    /// 家族目录前缀，例如 `"/mem"`、`"/sys"`。
    const PREFIX: &'static str;
    /// 一次完整读取的结果。
    type Report;

    fn report<R: StatsReader>(reader: &R) -> Result<Self::Report, Error>;
}

impl StatsReader for StatsClient {
    // 既有 `list`/`read` 直接满足这两个方法。
    fn names(&self) -> Result<Vec<String>, Error>;
    fn read(&self, name: &str) -> Result<MetricValue, Error>;
}

impl StatsClient {
    // 既有：连接与只读映射。
    pub fn connect(socket_path: &Path) -> Result<Self, Error>;

    /// 泛型入口：`P` 编译期单态化；没有注册表、没有 `dyn`。
    pub fn report<P: StatsProvider>(&self) -> Result<P::Report, Error> {
        P::report(self)
    }
}

pub struct MemoryStatsProvider;
pub struct SystemStatsProvider;

pub struct MemoryHeapUsage {
    pub name: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub used_mmap_bytes: u64,
    pub max_allocated_bytes: u64,
    pub free_chunk_count: u64,
    pub releasable_bytes: u64,
}

pub struct MemoryStats {
    pub heaps: Vec<MemoryHeapUsage>,
}

impl MemoryStats {
    /// 按堆名（去掉 `/mem/` 前缀后的剩余部分）查找。
    pub fn heap(&self, name: &str) -> Option<&MemoryHeapUsage>;
}

impl StatsProvider for MemoryStatsProvider {
    const PREFIX: &'static str = "/mem";
    type Report = MemoryStats;
    fn report<R: StatsReader>(reader: &R) -> Result<MemoryStats, Error>;
}

pub struct SystemStats {
    pub worker_thread_count: u64,
    /// `/sys/main_loop_count_per_worker`：累计计数，权威值（可差分）。
    pub main_loop_counts: Vec<u64>,
    /// `/sys/loops_per_worker`：服务端阻尼后的 loops/s 当前值（不可差分，V13）。
    pub loop_rates_per_second: Vec<u64>,
    pub heartbeat: u64,
    pub boottime_unix_seconds: u64,
    /// clear baseline 落地前服务端写 0；客户端映射成 `None`，调用者不能
    /// 对 0 做减法（D7 规则 5、ADR-0019 D4.7）。
    pub last_stats_clear_unix_seconds: Option<u64>,
    /// 本 report 的产生时刻（客户端本地单调时钟），服务累计列的窗口派生。
    pub sampled_at: Instant,
}

impl SystemStats {
    /// 从两次**累计**采样派生每 worker 主循环速率（次/秒），长度与
    /// `main_loop_counts` 相同；两样本无法相减（时刻相同或长度不同）返回
    /// `None`。服务端速率列不参与派生（D5）。
    pub fn main_loop_rates_per_second(&self, earlier: &SystemStats) -> Option<Vec<f64>>;
}

impl StatsProvider for SystemStatsProvider {
    const PREFIX: &'static str = "/sys";
    type Report = SystemStats;
    fn report<R: StatsReader>(reader: &R) -> Result<SystemStats, Error>;
}
```

**客户端仓的验收边界：**

- `StatsClient` 不加任何家族方法：`memory_heap_usages`、`worker_thread_count`
  这类访问器属于 provider，加回底层就是把 family 语义塞进机制层。
- `/sys/loops_per_worker` 只读当前值：provider 把它放进
  `SystemStats::loop_rates_per_second`，不重算、不对它差分；需要别的窗口时用
  `main_loop_rates_per_second(earlier)` 从累计列派生。
- provider 每个基条目只读一次：`/mem/<heap>` 一次读得到 7 列，不逐列读、不跨
  条目拼"快照"；形状或类型不符（列数不对、`num_worker_threads` 与
  `main_loop_count_per_worker` 长度不一致）返回客户端仓的类型化错误，不 panic、
  不静默裁剪。
- provider 不持有连接/映射，不缓存 `DirectoryIndex`，不保存上一轮基线；速率派生
  是两份 report 的纯函数。
- 新增 family（M4 的 node 计数、session 的 `/sys/session/*`）只加 provider 与
  typed report，不改 `StatsClient`、不改协议 crate。
- 客户端仓内部分文件（Rust binding）：`StatsClient`/`StatsReader`/`StatsProvider`
  属于客户端核心（transport 与其能力契约），两个标准 provider 与各自 report 放
  provider 模块，一个 family 一处；新增 family 不触碰客户端核心文件。

本仓为客户端契约提供的互操作证据是只读映射 fixture（M3）。

### 6.5 hammer-component-macros：`#[stats_collect_registration]`（A12）

**现有的同类实现（`#[init_function]`）：**属性宏本体只做参数解析与签名校验，
展开函数生成三样东西：签名的 adapter 适配函数、lifecycle callback 的
`AtomicUsize` 索引，以及 `__INIT_FN_<NAME>` 静态项（`component-macros/src/lib.rs:2914-2978`）：

```rust
#[proc_macro_attribute]
pub fn init_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as InitFnArgs);        // name / runs_before / runs_after
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    expand_registered_function(args, fn_item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_registered_function(args: InitFnArgs, function: ItemFn) -> Result<TokenStream2> {
    validate_init_function_qualifiers(&function)?;            // safe/同步/非泛型/无 receiver
    let arguments = init_arguments(&function)?;               // 只接受 &mut DataPlaneMain
    validate_unit_init_output(&function)?;                    // RuntimeResult<()>
    let function_name = &function.sig.ident;
    let adapter_name = format_ident!("__hammer_init_adapter_{}", function_name);
    let static_ident = init_function_static_name(&args.name); // __INIT_FN_<NAME 大写>
    let conditional_attributes = /* 原样复制 #[cfg]/#[cfg_attr] */;
    Ok(quote! {
        #function
        #(#adapter_attributes)*
        fn #adapter_name(__hammer_main: &mut DataPlaneMain) -> RuntimeResult<()> { /* 适配参数 */ }
        #(#registration_attributes)*
        static #callback_index_ident: AtomicUsize = AtomicUsize::new(usize::MAX);
        #(#registration_attributes)*
        pub(crate) static #static_ident: InitFunction =
            InitFunction { name: #name, runs_before: &[..], runs_after: &[..],
                            func: #adapter_name, callback_index: &#callback_index_ident };
    })
}
```

`#[main_loop_enter_function]`/`#[main_loop_exit_function]` 是无参数版本：
`name` 直接取函数名（`:3310-3345`）。

**新增宏（A12）：**与 `#[main_loop_enter_function]` 同形，无参数、无 adapter、
无 callback index（采集登记没有排序 qualifier，也没有"只调用一次"的标记，
顺序由 image 列表决定）。它只承载**采集者登记**这一半（在条目装好后调
`register_collector` 挂上自己的采集者，不建条目）；
`#[derive(Stats)]` 承载**条目注册**那一半（D9.5）。一次性写值不是采集登记，
不进这张表：`/sys/boottime` 由 node 设一次，`/sys/num_worker_threads` 由
worker thread 域在 `start_workers` 设一次，两者都走 A2 的"按索引写一个值"。

```rust
// crates/hammer-component-macros/src/lib.rs

/// 把一个 `fn(&mut StatsMain) -> RuntimeResult<()>` 声明为一条 stats 采集登记项。
///
/// 生成 `__STATS_COLLECT_REGISTRATION_<函数名大写>`，由所在 crate/插件的
/// `__declare_registration_image!` 或 `#[plugin(...)]` 列进 `stats_registrations`。
#[proc_macro_attribute]
pub fn stats_collect_registration(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return Error::new(Span::call_site(), "stats_collect_registration takes no arguments")
            .to_compile_error()
            .into();
    }
    let function = parse_macro_input!(input as syn::ItemFn);
    expand_stats_collect_registration(function)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_stats_collect_registration(function: ItemFn) -> Result<TokenStream2> {
    // 校验：safe、同步、非泛型、无 receiver、恰好一个 &mut StatsMain 参数、
    // 返回 RuntimeResult<()>（与 init_function 的 validate_* 同一组规则）。
    let signature = &function.sig;
    let function_name = &signature.ident;
    let name = LitStr::new(&function_name.to_string(), function_name.span());
    let static_ident = format_ident!(
        "__STATS_COLLECT_REGISTRATION_{}",
        function_name.to_string().to_ascii_uppercase()
    );
    let conditional_attributes = /* 原样复制 #[cfg]/#[cfg_attr] */;
    Ok(quote! {
        #function

        #(#conditional_attributes)*
        #[doc(hidden)]
        pub static #static_ident: ::hammer_runtime::registration::StatsRegistration =
            ::hammer_runtime::registration::StatsRegistration {
                name: #name,
                register: #function_name,
            };
    })
}
```

**登记项怎么进 `RegistrationImage`：**沿用既有唯一通道
（`hammer-runtime/src/registration.rs:194-300`）。内置 crate 直接列在
`__declare_registration_image!` 里（宏最终展开成
`RegistrationImage::new_with_stats(…, &[&__STATS_REGISTRATION_…, …])`），插件在
`#[plugin]` 参数里列出（`component-macros/src/lib.rs:3431,3831`）：

```rust
// crates/hammer-runtime/src/lib.rs（内置运行时 image）
crate::__declare_registration_image!(
    init_functions = [config::stats::__INIT_FN_STATS_MAIN_INIT];
    // ... 其它目录不变 ...
    stats_registrations = [
        config::stats::__STATS_REGISTRATION_Sys,                        // 注册
        config::stats::__STATS_REGISTRATION_MainHeapUsage,              // 注册
        config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_MAIN_HEAP,         // 更新
        config::stats::__STATS_REGISTRATION_StatSegmentUsage,           // 注册
        config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_STAT_SEGMENT_HEAP, // 更新
        config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_WORKER_MAIN_LOOP,  // 更新
        thread_main::__STATS_REGISTRATION_WorkerThreadCount,            // 注册（值由本域在 start_workers 写一次）
    ];
);

// 插件（未来的堆 owner）：一个模块上的 #[plugin]，stats_registrations 列出
// 本插件里 #[stats_collect_registration] 生成的静态项
#[hammer_component_macros::plugin(
    name = "my-plugin",
    load_after = ["service"],
    stats_registrations = [__STATS_COLLECT_REGISTRATION_REGISTER_MY_HEAP],
)]
mod my_plugin;
```

`run_stats_registrations()`（`init.rs:209-225`）按 `PluginMain` 的 image 顺序
遍历每个 image 的 `stats_registrations`，对每项调用
`(registration.register)(&mut stats_main)`——这条遍历是本次唯一"发现新家族/新堆"
的入口，机制与 node 都不改；声明项与更新项都由它执行，列表顺序就是执行顺序
（每个采集者登记项排在它引用的声明项之后）。

**`#[derive(Stats)]` 的三个新字段属性（注册语义，需批准）：**`path = <表达式>`
（install 期求值的完整目录名）、`columns = <表达式>`（行宽，install 期
`validate (index, 0, columns - 1)`）、`symlinks = [(name, column), …]`
（VPP 注册三连的后两步：`validate` 与 `add_symlink`，`provider_mem.c:52-63`）。
现有 `bootstrap` 语义不变（固定槽专用，`/mem` 声明不用它），`install` 仍然
只建条目、不登记采集者。

**不进入设计的形状：**不给宏加 `runs_before`/`runs_after`（登记没有拓扑需求）、
不给 `#[stats_collect_registration]` 加 `bootstrap`（固定槽是 `#[derive(Stats)]` 的语义）、
不给 `#[derive(Stats)]` 加 `collector = <fn>`/`private_data = <expr>`（更新语义
不在声明宏里，D9.5）、不生成 linkme 片段或全局注册表（image 列表是唯一载体，
`RegistrationImage` 的文档明确不使用 DSO 构造器）、不在宏里写堆名或列序（宏只搬
函数与名字/属性表达式）。

### 6.6 全部动作总表

| 文件 | 类型 / 方法 / 字段 | 动作 |
| --- | --- | --- |
| `crates/hammer-infra/src/mem/mod.rs` | `HeapUsage`、`MemHeap::usage` | 新增 |
| `crates/hammer-component-macros/src/lib.rs` | `#[stats_collect_registration]` 属性宏（A12，签名 `fn(&mut StatsMain) -> RuntimeResult<()>`，生成 `__STATS_COLLECT_REGISTRATION_<函数名大写>` 静态项） | 新增（实现见 6.5；与 `#[main_loop_enter_function]` 同形，无参数、无 adapter；现有代码里的 `#[stats_registration]` 随之改名） |
| `crates/hammer-component-macros/src/lib.rs` | `#[derive(Stats)]` 的三个字段属性 `path = <表达式>`、`columns = <表达式>`、`symlinks = [(name, column)]` | 新增（注册语义，6.5；`path`/`columns` 在 install 期求值，现有 `bootstrap` 语义不变；不加 `collector`/`private_data`） |
| `crates/hammer-infra/src/mem/mod.rs:862-864` | `MemHeap::free_space` | **删除** |
| `crates/hammer-infra/src/svm/ssvm.rs:232-236` | `free_space()` 调用点 | 修改为 `usage().free_bytes` |
| `crates/hammer-stats/src/segment.rs` | `set_simple_counter`（取 `&self`，返回 `()`）、`StatsSegment` 的 `Sync` 写模型 | 新增 |
| `crates/hammer-stats/src/mem.rs` | `update_mem_usage (entry: &DirectoryEntry, usage: HeapUsage)` | 新增（家族模块的自由函数，不是 `StatsSegment`/`StatsMain` 的方法；VPP `provider_mem.c:26-39` 的列映射部分，A11） |
| `crates/hammer-stats/src/mem.rs` | `register_mem_heap` | **删除**（条目/行宽/别名改为 owner 的 `#[derive(Stats)]` 声明，采集者登记改为 owner 的 `#[stats_collect_registration]` 项，D9.5） |
| `crates/hammer-stats/src/segment.rs:843-867` | `entry`、`entry_of_type` | 修改返回类型（借用，不再拷贝） |
| `crates/hammer-stats/src/protocol.rs:385-390` | `DirectoryEntry` 的 `Copy` derive | **删除** |
| `crates/hammer-stats/src/segment.rs:294-296,961-1000` | ring payload 与 `allocate_vector` 的分配点 | 修改（在 `activate()` 窗口内用 `alloc::alloc_zeroed` 分配，失败走 `alloc::handle_alloc_error`；不再调用 `MemHeap::allocate`，不返回 `HeapExhausted`；不新增函数） |
| `crates/hammer-stats/src/segment.rs:340,529,679` | 只为释放/为手工选堆而开的活动堆 guard | **删除**（窗口只覆盖分配；释放由 `heap.deallocate` 以 owning heap 为接收者完成） |
| `crates/hammer-stats/src/segment.rs:974,990` | 尺寸溢出、长度超过 u32 | 修改为 `StatsError::InvalidShape` |
| `crates/hammer-stats/src/lib.rs:99-103`、`lib.rs:140-147` | `StatsError::HeapExhausted` 变体与 Display 分支 | **删除** |
| `crates/hammer-stats/src/segment.rs:194-204` | `set_gauge`、`set_timestamp` | 修改实现与签名（写回共享槽，返回 `()`） |
| `crates/hammer-stats/src/segment.rs:868-872` | `heap` | 修改（私有 → `pub`，返回 `&'static MemHeap`：段映射活到进程退出；登记期取一次放进该采集者的字段，采集者只拿条目借用、不取锁也不查全局，见 A3） |
| `crates/hammer-stats/src/lib.rs` | `Collector` trait、`StatsMain.collectors: Vec<Box<dyn Collector>>`、`register_collector (&mut self, impl Collector + 'static)` | 新增（A8、D9.1、D9.2；表行就是 VPP `vlib_stats_collector_t` 的一行——一个具体类型同时承担 `fn` 与 `private_data` 两格；表进程私有、无锁、不进 `StatsSegment`；唯一的 `dyn` 处，理由见 D9.2）；`/mem` 的列序不在这里，在 `mem.rs`（A11） |
| `crates/hammer-stats/src/lib.rs:75-78` | `StatsMain::collect` | 修改（签名与实现：取每个条目的 `&DirectoryEntry`（不取锁）→ 调该表行（它自己写自己那几列）→ 最后取 heartbeat 条目写 value+1，返回 `()`；实现里没有 family 名字） |
| `crates/hammer-stats/src/lib.rs` | `StatsMain::init` 拆成"创建（未发布）→ 遍历登记 → 发布" | 修改（`StatsMain::create(...) -> StatsResult<StatsMain>` 与 `StatsMain::set(owner)`；采集者表需要 `&mut StatsMain`，所以登记必须在发布之前完成，D9.1） |
| `crates/hammer-runtime/src/config/stats.rs` | `MainHeapUsage`/`StatSegmentUsage` 两条 `#[derive(Stats)]` 声明、三个采集者类型（`HeapCollector`/`MainLoopCountCollector`/`LoopsPerSecondCollector`，各带自己的实例状态字段；`HeapCollector` 有主堆与段堆两个实例）、`register_main_heap`/`register_stat_segment_heap`/`register_worker_main_loop` 三个 `#[stats_collect_registration]` 项；`Sys` 两个 per-worker 字段的 `columns` 属性 | 新增/修改（A5、A6、A11、A12，与 `Sys` 同模块，不新增文件） |
| `crates/hammer-runtime/src/lib.rs:15-35` | `stats_registrations` 增加六条静态项（`MainHeapUsage`、`StatSegmentUsage`、`WorkerThreadCount` 三条声明 + 三条采集登记项），列表共七条（`Sys` 保持首位） | 修改（列表顺序 = 执行顺序；每个采集登记项排在其声明项之后） |
| `crates/hammer-runtime/src/config/stats.rs:85-97` | `Sys.main_loop_count_per_worker`、`Sys.loops_per_worker` | 新增字段（累计列 + 速率列，各带 `columns = ThreadMain::global().worker_count()`；`num_worker_threads` 由 `thread_main.rs` 的声明创建、由该域在 `start_workers` 里设置一次，不占采集登记项） |
| `crates/hammer-service/src/binary_api.rs` | `binary_api_init` 加 `runs_before = ["stats_main_init"]` | 修改（既有 init qualifier：api segment 映射先于 stats 登记；见 6.3 的顺序说明） |
| `crates/hammer-ipc/Cargo.toml` | 依赖 `hammer-stats`（`hammer-ipc → hammer-stats`） | 新增（登记与采集者要命名 `StatsMain`/`Collector`/`DirectoryIndex` 并调用 `mem` 模块；无环，AGENTS.md 依赖表同步） |
| `crates/hammer-runtime/src/config/stats.rs:99-121` | `stat_segment_collector_process`（`statseg-collector-process` process node 的工厂） | 修改（只去掉循环体里 `stats_main.collect()` 的 `?`；节点与注册不变，家族步骤不进 node） |
| `crates/hammer-runtime/src/config/stats.rs:145-176` | `init_stats_main` | 修改（删掉 gauge/`validate`/别名与 mem 步骤；只剩 `StatsMain::create` → `Sys::bootstrap` → listener/FileMain → `run_stats_registrations(&mut stats_main)` → `StatsMain::set`） |
| `crates/hammer-runtime/src/thread_main.rs`、`crates/hammer-runtime/src/start_workers.rs` | `WorkerThreadCount` 声明（`#[derive(Stats)]`，`/sys/num_worker_threads`）列入 runtime image；值由 `start_workers`（本域的 `main_loop_enter_function`）`set_gauge` 一次 | 新增（没有采集登记项：它不是采集者） |
| `crates/hammer-ipc/src/binary_api/memory_shared.rs`、`crates/hammer-service/src/lib.rs:3-46` | 三条 `#[derive(Stats)]` 声明（`RootRegionPvtUsage`/`ApiRegionPvtUsage`/`ApiRegionDataUsage`，`path` 由 region 名派生）、一条 `#[stats_collect_registration]` 项（`register_api_segment_heaps`，注册三个 `HeapCollector` 实例，字段是 `&'static MemHeap`）；四条 `pub` `StatsRegistration`；镜像的 `stats_registrations` 列表 | 新增（A5、D2；名字由 region 名派生，登记期判一次 `ApiMain::is_mapped()` 并取 region 堆引用） |
| `crates/hammer-runtime/src/worker_thread.rs:14-31` | `WorkerThread.cacheline2`、`WorkerThread.main_loop_count`、`WorkerThread.loops_per_second` 与两个访问器 | 新增 |
| `crates/hammer-runtime/src/thread_main.rs` | `ThreadMain::{worker_main_loop_count, worker_loops_per_second, worker_descriptor}` | 新增（`pub(crate)`） |
| `crates/hammer-runtime/src/data_plane/main.rs:43,73`；`buffer_pool.rs:127` | `DataPlaneMain.main_loop_count` | **删除** |
| `crates/hammer-runtime/src/data_plane/worker.rs:20-23` | `increment_main_loop_count`（保持 `&mut self`） | 修改（累计 `fetch_add` + VPP 的速率块，`main.c:1685,1689-1714`） |
| `crates/hammer-runtime/src/data_plane/main.rs` | `DataPlaneMain.{loops_this_reporting_interval, loop_interval_start, loop_interval_end, loops_per_second, damping_constant}` | 新增字段（VPP `main.h:209-215` 同名，不新增类型） |
| `crates/hammer-runtime/src/main_loop.rs:83` | 步骤注释 | 修改 |
| 外部 `netsystem-client` | `StatsReader`、`StatsProvider`、`MemoryStatsProvider`、`SystemStatsProvider`、`MemoryHeapUsage`、`MemoryStats`、`SystemStats`（含 `loop_rates_per_second` 与 `main_loop_rates_per_second(earlier)`）、`StatsClient::report::<P>()` | 新增（由客户端仓批准；`StatsClient` 不加家族方法；速率列只读、不重算） |

**明确删除/修改项（避免留下旧 surface）：**

- `MemHeap::free_space`（crate-private）由 `usage().free_bytes` 取代，`ssvm.rs:232-236`
  的调用点随之迁移；同一事实只保留一个采样操作。
- `DataPlaneMain.main_loop_count` 删除，避免"每线程原子计数 + 本地计数"两份
  计数漂移。
- `entry`/`entry_of_type` 的拷贝返回被借用返回取代，`DirectoryEntry` 的 `Copy`
  derive 同时删除：不保留"拷贝目录项再写"的旧形状，也不为兼容再包一层
  `store`/`get_copy`；`set_gauge`/`set_timestamp`/`set_simple_counter` 都走
  共享借用 + 映射地址（取 `&self`）。
- `StatsError::HeapExhausted` 删除（`lib.rs:99-103`、`lib.rs:140-147`），4 个
  构造点按 6.2 的表格重新归类：两个分配点改成"`activate()` 窗口 + 窗口内普通
  分配"，耗尽即走分配器的 abort 路径（V25），两个不可表示的形状返回
  `InvalidShape`；不改名为 `CapacityExhausted`，不保留兼容 variant，也不新增
  `terminate_*`/`abort_*` helper 或诊断函数。
- `StatsSegment` 不新增任何 family 字段或方法：`memory_heaps`、
  `MemoryHeapCounters`、`add_memory_heap`、`sample_memory_heaps`、
  `StatsMain::add_own_memory_heap` 都不进入实现（D1）。新增的状态只有进程内的
  采集者登记表（`StatsMain.collectors`）——它是通用机制，不含 family 类型、不
  进共享内存、不进 `StatsSegment`（VPP 的 `sm->collectors` 同样如此，V21）。
- 家族步骤不进入 `statseg-collector-process` node，也不进入
  `StatsMain::collect` 的实现：node 保持薄驱动（boottime + 每轮一次
  `StatsMain::collect()` + sleep），机制只做"取出该条目的共享借用 → 调表行
  （表行在自己的 `Collector` 实现里写自己那几列）→ 最后写 heartbeat"，
  家族只以启动期
  的声明项与采集者登记项参与（runtime image：
  `Sys`、`MainHeapUsage`、`StatSegmentUsage`、`WorkerThreadCount` 四条声明项 +
  `register_main_heap`/`register_stat_segment_heap`/`register_worker_main_loop`
  三条采集登记项（`/sys/num_worker_threads` 不是采集者：它由 worker thread 域在
  `start_workers` 里写一次，因此没有采集登记项）；hammer-service image 的三条
  api segment 注册项 + `register_root_region_pvt_heap`/`register_api_region_pvt_heap`/
  `register_api_region_data_heap` 三条更新项）；
  `init_stats_main` 里没有堆名、没有 `validate`/别名步骤。不新增第二个 process
  node、不新增 Process 类型。
- `Sys.last_stats_clear` 保留现有不变实现；clear baseline 落地前它保持 0，并在
  文档中明确"未实现"，不得用客户端减法冒充。
- 不引入 `StatsHeap`、`StatsMapping`、`MemoryHeapView`、`WorkerCounterHandle`、
  通用 per-thread container 或第二套 metrics registry：mem 家族是"每堆一条声明
  （注册）+ 一条采集者登记（更新）"，两者都由 `#[derive(Stats)]` /
  `#[stats_collect_registration]` 生成（A12、6.5），列进同一个 image 的
  `stats_registrations`；system 家族用 `#[derive(Stats)]` 声明的字段加每线程原子
  计数；登记表是唯一定义采集者集合的地方。**唯一的 trait 对象是 D9.2 的表行**
  （`Box<dyn Collector>`，VPP `(fn, private_data)` 对的表达）：没有第二套 `dyn`
  分发做 family 查找，没有注册表，也没有按名字的运行时查找。
- `#[derive(Stats)]` 不承载更新语义：`collector = <fn>` 与 `private_data = <expr>`
  不进入该宏（D9.5、6.5）；采集者是独立一步 `register_collector`，实例状态就是
  采集者类型自己的字段（登记期取一次，此后由采集者自己持有）。

## 7. 迁移与执行顺序

| 阶段 | 范围 | 完成证据 |
| --- | --- | --- |
| M0 infra | `HeapUsage` + `MemHeap::usage()`；`ssvm` 调用点迁移；删除 `free_space` | 真实堆上 used/free/total/free_chunk_count 与已知分配一致；采样不分配 |
| M1 stats（机制一次改完） | 目录访问借用化 + 删除 `Copy`（A2 的 H10 根因）、`set_gauge`/`set_timestamp` 落盘并改为不返回 `Result`、新增 `set_simple_counter`（返回 `()`）、`heap()` 改为 `pub` 且返回 `&'static MemHeap`（A3）、段内分配改为"`activate()` 窗口 + 窗口内普通分配"并删除只为释放而开的 guard、`StatsError::HeapExhausted` 删除与 4 个构造点归类（A9）、新增 `/mem` 家族模块（只留 `STAT_MEM_*` 与 `mem::update_mem_usage (entry, usage)`，A11）、新增进程私有采集者表（`Collector` trait/`StatsMain.collectors: Vec<Box<dyn Collector>>`/`register_collector (&mut self, impl Collector + 'static)`，D9.1、D9.2）、`StatsMain::create`/`set` 拆分、`StatsMain.segment` 改回普通字段且 `set_gauge`/`set_timestamp`/`set_simple_counter`/`advance_heartbeat` 都取 `&self`（A2、V29）、`StatsMain::collect` 改为"取条目 → 调表行 → 最后写 heartbeat"（A8） | 写后读回可见（gauge/timestamp/单元格）；heartbeat 随 `collect()` 轮次递增；写值调用没有错误分支；段内分配落在段堆而不是 main heap；空登记表的 `collect()` 仍是合法的 heartbeat 轮；`segment.rs:974,990` 的"形状不可表示"仍返回 `InvalidShape`；堆耗尽走分配器的 abort 路径而不是返回 Err；表与段在轮次里都没有锁、没有手写擦除（`dyn` 只在 D9.2 的表行这一处）、没有 payload；采集者只见自己那一条条目的共享借用（不取段锁、不查全局） |
| M2 macros + runtime + hammer-ipc（声明与业务一次接入） | `hammer-component-macros`：`#[derive(Stats)]` 三个新字段属性 + `#[stats_collect_registration]`（现有 `#[stats_registration]` 改名，含其生成的静态项前缀改为 `__STATS_COLLECT_REGISTRATION_`）签名改为 `fn(&mut StatsMain)`（A12）；runtime：`Sys` 两个新字段与 `columns`、两条 `#[derive(Stats)]` 堆声明、`WorkerThreadCount` 声明（值由 `start_workers` 写一次）、三个 `#[stats_collect_registration]` 项、三个采集者类型（`HeapCollector`（主堆与段堆两个实例）、`MainLoopCountCollector`、`LoopsPerSecondCollector`，实例状态分别取 `MemMain::main_heap()`、`stats_main.segment.heap()`、`ThreadMain::global()`）、node 循环体（只剩去掉 `?`）、`WorkerThread` 两个每线程原子与 `ThreadMain` 解析、`DataPlaneMain` 的五个 VPP 同名字段与 `increment_main_loop_count`、runtime image 的 `stats_registrations` 列表；hammer-ipc：`hammer-stats` 依赖、`ApiMain::{root_region, primary_region}`、三条堆声明与一条采集者登记项（登记三个 `HeapCollector` 实例，堆引用在登记期从 region 取）、`hammer-service` 镜像列表与 `binary_api_init` 的 `runs_before` | 声明项与登记项被 image 列出、遍历按列表顺序执行（登记项在其声明项之后）；worker 计数正确、两个 worker 的列独立增长、复制值与原子值一致；`/sys/loops_per_worker` 列在稳定负载下接近实测 loops/s 且随阻尼收敛（与 VPP 语义一致）；五个堆的 7 列 + 3 别名齐备（`/mem/main heap`、`/mem/stat segment`、`/mem/global_vm pvt`、`/mem/<region> pvt`、`/mem/<region> data`），且每个堆的名字来自它自己的声明、实例来自它自己的 owner 模块（5 个实例同一个 `HeapCollector` 类型）；api 未映射时没有 api 采集者、三条条目保持 0；名字/类型失败不留半个条目；启动顺序不允许半个家族被读到；节点数量不变（仍只有一个 process node），node 里没有家族步骤 |
| M3 client（外部仓） | `StatsReader`/`StatsProvider` 契约、`MemoryStatsProvider`/`SystemStatsProvider` 与 typed report（速率列直接读、累计列可派生）、客户端错误与测试；本仓提供只读映射互操作 fixture | `names()` 含 `/mem/*` 与 symlink；provider 用内存 fixture 单测、再对真实映射跑一次且 report 一致；base vector 与 symlink 列读取一致；epoch 变化重试；列数不符报错；`StatsClient` 上没有任何家族方法 |
| M4 后续（另行批准） | `/sys/vector_rate`/`/sys/vector_rate_per_worker`（随 node 计数）、SVM/SSVM 堆纳入 `/mem/*`、node 计数与 `/sys/node/*`、memory/runtime ctl 命令 | 各自 ADR 或批准记录；本 ADR 不预置 surface。`/sys/loops_per_worker` **不在** M4（M2 交付），只有它依赖 node 计数的兄弟条目被推迟 |

每一阶段都是一个完整候选改动：定义、导出、调用点、测试与文档一起更新；不保留
旧入口的转发 wrapper，不用改名保留旧所有权。

## 8. 验证矩阵

| 验证项 | 方法 | 期望 |
| --- | --- | --- |
| 堆采样正确性 | 在真实 `MemHeap` 上分配/释放已知大小，比较 `usage()` 前后 | `used_bytes`/`free_bytes` 变化与分配一致，`total_bytes` 不倒退，`free_chunk_count` 合理 |
| 采样不分配 | 采样前后比较同一堆的 `total/used/free` 与 heap inventory | 采样只读 mspace 元数据；不新增映射/heap |
| 目录事务原子性 | 重复名字、超长名字、类型/形状不匹配注入 | 返回既有具体错误；目录与名字表无半个家族、无悬空 symlink |
| 固定堆耗尽崩溃与分配落点 | 独立子进程：在真实段堆上请求超过剩余容量的目录项与 vector；另跑一次正常注册并在注册前后比较五个堆（`main heap`、`stat segment`、api segment 的 root/pvt/data）的用量 | 耗尽时进程在分配器的失败路径上非 0 退出（`handle_alloc_error`/abort），不返回 `StatsError`，不留半个家族；正常注册的字节计入段堆（`StatsSegment::heap()`）而不是段外 heap；`segment.rs:974,990` 的"形状不可表示"仍返回 `InvalidShape` |
| 写路径可见性（A2） | 写 `set_gauge`/`set_timestamp`/`set_simple_counter` 后，从同一映射（另一进程只读）读回 | 读回值等于写入值；heartbeat 随轮次单调递增（修复前为常量 0） |
| 列序与 symlink | 读取 `/mem/<heap>` 与三个别名 | 7 列语义与 D2 表一致；别名指向正确列并保留别名名字 |
| 五个堆齐备且名字来自 owner | 启动后枚举 `/mem/*` 基条目，并逐个核对 owner | 五个基条目齐备：`/mem/main heap`（runtime）、`/mem/stat segment`（runtime）、`/mem/global_vm pvt`、`/mem/<region> pvt`、`/mem/<region> data`（hammer-ipc）；每个都有 7 列 + 3 别名；`init_stats_main` 与 `hammer-stats` 里不出现任何堆名；五个堆的 `used/free/total` 与各自 `MemHeap::usage()` 一致 |
| api segment 未映射/退出后采样 | `ApiMain` 未映射时做一次登记（三个条目保持 0、没有 api 采集者）；映射后再跑若干轮，再在 `unmap_shared_regions()` 之后跑一轮 | 未映射时不注册 api 采集者，条目保持 0；映射期间的轮次读的是登记期取到的 region 堆引用；`unmap_shared_regions()` 只在 main-loop exit（`exit_binary_api`）里跑，即最后一个轮次之后，因此不存在"unmap 后还有一轮"的读；同一轮里 runtime 的两个堆与 `/sys` 正常更新 |
| 重复堆名/名字冲突 | 测试注入第二个 owner 声明同名堆（两条声明用同一 `path`） | 第二条声明返回既有 `DuplicateName`（`create_entry` 已有的检查），先登记的堆保持可读可更新；不覆盖、不留半个条目 |
| 新堆只加两条项 | 加一个测试堆：owner 侧一条 `#[derive(Stats)]` 声明（`path`/`columns`/`symlinks`）+ 一条 `#[stats_collect_registration] fn`（`register_collector (该 owner 的采集者)`），并把宏生成的两个静态项列进该 image 的 `stats_registrations` | 条目与三个别名出现、每轮更新；`hammer-stats`、`StatsMain::collect()`、`init_stats_main`、node 与其它 image 都不改；插件用 `#[plugin(stats_registrations = [...])]` 得到同样结果 |
| 采样轮次与登记表 | 启动后推进可控轮次：node 每轮调用 `StatsMain::collect()`，机制按 `run_stats_registrations` 执行登记项的顺序，为每条登记取出其条目的共享借用（不取锁）并调用该表行（表行在自己的 `Collector` 实现里写自己那几列；当前 4 条采集登记项、7 个采集者实例、3 个采集者类型：`HeapCollector` 5 个实例（main heap、stat segment、api segment 的 root/pvt/data）、`MainLoopCountCollector` 与 `LoopsPerSecondCollector` 各 1 个；`/sys/num_worker_threads` 不是采集者，不在表里），最后取 heartbeat 条目把值加一（D9.4） | 顺序 = 登记项执行顺序，`stats_registrations` 列表顺序即执行顺序（V14 的"collectors → heartbeat"对应）；每堆列更新且 `heartbeat` 同轮 +1；采集者与 `collect()` 都没有 `Result`，轮次里没有错误分支；`collect()`、`init_stats_main` 与 node 函数体里都没有 family 名字或堆名；把某条登记去掉后只有对应条目停止更新，轮次代码不变；空表也推进 heartbeat（V28） |
| 新 family 只加两条项 | 用测试/后续 family 的形态检查机制 surface | 新增 family = owner 侧一条声明（注册）+ 一条或多条采集者登记项（更新；家族内有多个实例时每个实例一个采集者，实例状态由该采集者类型自己的字段携带），并把静态项列进 owner image；`StatsSegment`/`StatsMain`、`StatsMain::collect()`、`init_stats_main` 与 node 都不需要改 |
| 写值断言 | 对未建立行列/错误类型的索引调用 `set_simple_counter`（仅测试注入） | panic 且消息带 index/type/row/column；不写越界、不静默丢弃 |
| 宏声明与 image 收录 | 在 runtime 与 hammer-ipc 上各加一个临时 `#[stats_collect_registration] fn`，并（只）把它列进各自 image 的 `stats_registrations`；另加一条临时 `#[derive(Stats)]` 声明并只列它的静态项 | 分别生成 `__STATS_COLLECT_REGISTRATION_<函数名大写>`（采集登记）与 `__STATS_REGISTRATION_<结构体名>`（条目声明）；不列入 image 就不执行，列入即按列表顺序执行；`#[stats_collect_registration]` 对带参数、错误签名（缺 `&mut StatsMain`、返回非 `RuntimeResult<()>`、泛型、receiver）编译失败；`#[derive(Stats)]` 的 `path`/`columns`/`symlinks` 在 install 期求值并落到目录（条目、行宽、别名）；`#[cfg]` 被复制到静态项 |
| `/sys` 条目齐备与速率语义 | 配置 N 个 worker 跑一段时间后读 `/sys/*` | `/sys/num_worker_threads` = N；两个 vector 长度都是 N；`main_loop_count_per_worker` 单调递增（累计）；`loops_per_worker` 在稳定负载下接近同一窗口内实测 loops/s、在不活动 worker 上收敛到 0，且**不**随 `collect()` 次数累加（速率不是累计量）；判据按实现的两个常量写（200µs 窗口、`K = exp(-1/20)`，V13、§9 第 9 条），不按 VPP 注释里的 20ms 收敛时间断言；两列都不含 main thread |
| 换进程直接读速率 | 客户端只读映射读 `/sys/loops_per_worker` | 直接得到与上一轮 worker 发布值相同的整数 loops/s；客户端不需要两次采样即可显示速率；provider 不对该列做差分（D7 规则 5） |
| 机制/业务边界 | 单独 `cargo check -p hammer-stats`，并检查该 crate 不新增业务依赖 | 机制 crate 在没有 `Sys`、worker 计数、任何堆名的情况下独立编译；`StatsSegment`/`StatsMain` 的公共签名不出现 `MemHeap`/`HeapUsage`/`SvmRegion`/`ThreadMain`/`ApiMain`（堆只以不透明 `DirectoryIndex` 出现），家族模块 `mem` 只接受 `DirectoryEntry` 与 `HeapUsage`；`collect()` 与 `mem::update_mem_usage` 的实现里没有 family 名字或堆名（新增家族/新堆只加声明与登记项即可，不改机制）；采集者表是私有 `Vec<Box<dyn Collector>>`（唯一的 `dyn` 处，D9.2）、不经 `SpinLock`、不进共享段 |
| worker 计数 | 配置 N 个 worker 启动 | `/sys/num_worker_threads` = N；`/sys/main_loop_count_per_worker` 与 `/sys/loops_per_worker` 长度都是 N |
| 投影一致性 | 两个 worker 跑主循环，比较投影轮次前后的 atomic 值与共享列 | 列 `i` = Data Worker `i`；数值单调；worker 退出不产生第二份计数 |
| 客户端读取（跨进程） | 独立进程只读映射：`names()`、读 base vector、读 symlink、读 system 条目 | 名字/类型/列裁剪正确；epoch 变化有界重试；畸形映射在解引用前拒绝 |
| 客户端 provider 分层 | 用内存 fixture 实现 `StatsReader` 跑两个 provider，再对真实映射跑同一批用例 | fixture 与真实映射得到相同 report；`StatsClient` 的公共 surface 不出现 `/mem`/`/sys` 家族方法；新增 family 只加 provider；形状不符返回类型化错误而不是 panic |
| 启动期失败 | 在 `Sys::install`、某个堆声明的 `path`/行宽 `validate`/别名（名字/类型注入）或某条采集者登记项里注入错误 | 启动以 Err 结束（`StatsMain` 未发布、未进入 main loop、worker 未启动），不存在可被客户端读到的半家族；其它已声明堆保持完整；轮次不执行 |
| 并发读取 | 客户端与 `statseg-collector-process` node 交错读取/写入 | 不越界、不读到未初始化单元格；不声称原子快照 |
| 布局 | 编译期 size/align 断言与真实映射 fixture | `STAT_SEGMENT_VERSION` 仍为 2；新旧条目共用既有布局 |

测试只在最终 pre-commit gate 执行，遵循仓库测试时机规则；不做源文本断言。
本轮没有执行任何测试、fixture 或性能测量。

## 9. 审查结果与未决项

**已核实：**mem 家族的目录形状（7 列 + 3 别名）、字段来源、`validate` 顺序
与"每个堆一条登记 + 每轮遍历"在 VPP 中有直接对应；VPP 的 mem provider 是
`vlib/stats/provider_mem.c` 这个独立文件（V2 的静态列映射、V3 的登记函数），
它对**每个堆**重复"建条目 → validate → 三个 symlink →
`vlib_stats_register_collector_fn`"（V3、V20、V21、V26）。Hammer 照同一顺序把它
拆成两条**同类声明通道**：注册是每个堆 owner 自己的一条 `#[derive(Stats)]` 声明
（`path`/`columns`/`symlinks`），更新是该 owner 的一个采集者类型
与它所在的 `#[stats_collect_registration]` 登记项（`register_collector`）；两条都由
`#[init_function]`/`#[main_loop_enter_function]` 同源的那种"宏生成静态项"形状
产生，静态项仍由 `RegistrationImage` 唯一承载（6.5、D9.5）。家族模块
`hammer-stats::mem` 只留列序（`update_mem_usage`）；堆名与采集者都是各 owner
的事，机制（`StatsSegment`/`StatsMain`）里不出现 `/mem` 的列序、`HeapUsage`
或任何一个堆名（`init_stats_main` 只剩 `run_stats_registrations()` + 发布）。
轮次的通用遍历放在机制（`StatsMain::collect` = VPP `do_stat_segment_updates`，
V14），`statseg-collector-process` node 每轮只调用它一次（V23、H12），全部家族
（`Sys`、主循环的两个 `/sys` vector 与五个堆）以各 owner image 里的登记项参与，
不再是写死的步骤链；因此以后新增的插件堆与 api segment 的三个 region 堆走同一条路，不需要
改机制或引擎。system 家族在 VPP 中同时包含累计/occupancy 统计与速率（V11、V12、
V13）：Hammer 除 worker 数量与累计主循环计数外，也按 VPP 的名字与窗口发布
`/sys/loops_per_worker`（采集者只复制 worker 主循环维护的阻尼值，V13）。主循环
的窗口状态直接放在 `DataPlaneMain` 上、字段名与 VPP `main.h:209-215` 相同，不新增
包装类型。`/sys/vector_rate*` 依赖每 worker 的 internal node calls/vectors 计数，
Hammer 还没有这两个计数，推迟到 ADR-0019 M4；本 ADR 不写零值占位。由于 Hammer 的
main thread 不跑 data-plane 主循环，两个 per-worker 列都只覆盖 Data Worker，
不能照抄 VPP 包含 thread 0 的列语义。H10 的写路径缺陷与两家族在同一批交付里解决
（M1 机制、M2 声明与业务），没有独立前置阶段。

**客户端侧与服务端同构：**服务端 D1 的三层划分在客户端有对应物：`StatsClient`
（机制）、`StatsProvider`（family 语义）、调用方/格式化（使用方）。因此
`StatsClient` 不获得任何家族方法，新增 family 只加 provider（A10、6.4）。

**本次授权：**只写 ADR。A1–A12、D0–D9 的全部 API 变更、客户端契约与客户端分层
（`StatsClient`/`StatsReader`/`StatsProvider`，A10）、`#[stats_collect_registration]`
宏（A12，签名 `fn(&mut StatsMain)`）、`hammer-stats` 的 `/mem` 家族模块（A11：
`mem::update_mem_usage` 一个自由函数，不是 `StatsSegment`/`StatsMain` 的方法）、
采集者模型的六处修订（D9：无锁进程私有表、**每个采集者是一个实现 `Collector`
的具体类型**（方法体是 VPP 的 `fn` 那一格，类型自己的字段是 `private_data` 那一格）
与表行 `Box<dyn Collector>`、堆采集者收敛成**一个** `HeapCollector`（D2：一个类型
+ 每堆一个实例，VPP 的 `stat_provider_mem_usage_update_fn` + `private_data` 形状）、轮次"取条目 → 调用 → 最后写 heartbeat"、
注册与更新分离、`StatsMain::create`/`set` 拆分、`advance_heartbeat` 只是同一条
写路径）、段锁重设计（D10：`StatsSegment` 上照 VPP 命名的一把
`stat_segment_lock: SpinLock<Directory>`，只覆盖结构变更与 `in_progress`/`epoch`
发布，取锁入口 `stat_segment_lock(&self) -> StatSegmentLock<'_>` 是唯一一处，
值写与轮次不取锁；两个新类型都私有）、宏改名（`#[stats_registration]` →
`#[stats_collect_registration]`，
生成的静态项前缀随之为 `__STATS_COLLECT_REGISTRATION_`，6.5）、
`/sys/num_worker_threads` 的"声明 + 本域在 `start_workers` 写一次、不占采集登记
项"（A6）、
`DataPlaneMain` 的 VPP 同名窗口字段（A7）、速率决策（发布 `/sys/loops_per_worker`，
D5，含 CONTEXT 的 Runtime Statistic 条目修订）、写路径改建（A2）、`heap()`
可见性与 `&'static MemHeap` 借用（A3）、`#[derive(Stats)]` 的三个新字段属性
（6.5）、`SvmRegion::{pvt_heap, data_heap}` 这对只读入口（6.3 的 infra 小口子：
api segment 的堆引用要在登记期取一次，现有的 guard 借用活不过一轮）与
`StatsError::HeapExhausted` 删除（A9）都需要明确批准后才能实施。

**与 ADR-0019 的接口（实施时必须同步修订，不能两个 ADR 各留一种说法）：**

1. ADR-0019 §6.8 的 `StatsError` 表把
   `HeapExhausted { requested, alignment, capacity }` 列为新增，并写明它"控制
   caller 回滚/降低容量需求"；本文 A9 删除该变体，理由是固定容量段堆没有合法
   恢复动作，段内分配本来就在活动堆窗口里走普通分配、耗尽即 abort（V24、V25）。
2. ADR-0019 §6.6 的 collector 注册接口在本设计中**落地**（A8、D9），并按本
   ADR 的决定修订五处：轮次没有可恢复失败（返回 `()`，D4）；`entry_index` 用
   `DirectoryIndex` 而不是裸 `u32`（与 A2 的写路径一致）；采集者表挂在
   `StatsMain` 上、是**进程私有的无锁集合**（`&mut StatsMain` 只见于启动期，
   不是 ADR-0019 §6.2 的 `Pool<Collector>`，也不放进 `StatsSegment`）；表行是
   VPP `vlib_stats_collector_t` 的一行，但 Hammer **不设 `fn` 与 `private_data`
   两格**，而是让每个采集者实现 `Collector`（方法体是 `fn`，类型自己的字段是
   `private_data`；参数是该条目的共享借用，表达 VPP 的 `d->entry`，采集者只写
   payload 单元格）；`vector_index` 不设（VPP 只存储与透传它，V26 末段）。表是
   `Vec<Box<dyn Collector>>`（`(fn, private_data)` 对就是 fat pointer，D9.2）。
   实施时必须同步改写 ADR-0019 §6.6，不能两个 ADR 各留一种说法。
3. `HeapUsage` 的归属：ADR-0019 §6.1/§6.7 把它排在 ADR-0019 M4，本文按 M0
   落地，实施时以本文的 M 序列为准，类型定义只保留一份。

**尚需决定/验证：**

1. CONTEXT 的 Runtime Statistic 条目要在实施时同步修订（新增 "Runtime Rate
   Projection"，并把 `_Avoid_: stored rate` 限定为"没有 owner、没有固定窗口、
   由查询临时计算的速率"）；如果不同意修订，就必须撤回 `/sys/loops_per_worker`
   这一条（只保留累计列），而不是让它以"累计计数"的名义存在（D5）。
2. 还有哪些堆纳入 `/mem/*`：当前已覆盖五个（`main heap`、`stat segment`、
   api segment 的 root/pvt/data），每个都由它自己的 owner 声明并登记采集者。
   进一步纳入 SVM/SSVM/session/classify 等堆时，需要该堆的 owner 给出唯一稳定
   的名字与采集者——机制已经支持（owner 侧一条 `#[derive(Stats)]` 声明 + 一条
   `#[stats_collect_registration]` 项，再列进自己的 image，机制与 `init_stats_main`
   不改），但本设计不提供运行时动态加入/注销路径：登记表在启动期构建，堆生命
   周期必须长于进程或由 owner 自己保证存活（api segment 的三个 region 堆在登记期
   取引用、只判一次映射，就是这条约束的举例）。
3. **实例状态的选择（每个 owner 自己定）：**需要实例状态的采集者把它放进自己的
   字段（就是 VPP `private_data` 那一格，D9.2）：堆采集者放
   `entry_index` + `&'static MemHeap`（五个堆同一个类型、同一个形状）、主循环
   采集者放 `&'static ThreadMain`。
4. `/sys/main_loop_count_per_worker` 是否要包含 main thread 列：本设计选择不
   包含，因为 Hammer 的 main thread 没有 data-plane 主循环计数。
5. 每线程原子计数的 cache-line 布局，以及主循环里每轮一次 `Instant::now()` +
   窗口分支的热路径成本，需要在 M2 用 release/LTO 汇编与基准验证；本轮没有性能
   数据。若时钟读取可测，降频方案要单独批准（6.3 的否决项），不能默认引入。
6. 客户端仓的接口命名、provider 抽象的实现形态（trait 形状、report 类型布局）与
   发布节奏需要与 `netsystem-client` 协调；本 ADR 固定共享内存契约与分层边界
   （底层不认识 family、provider 经 `StatsReader`、泛型静态分发），不固定命名。
7. api segment 的三个堆在 exit 路径上的行为需要在 M2 验证：登记发生在
   `binary_api_init`（`runs_before = ["stats_main_init"]`）之后，因此登记期取到的
   region 堆引用在采集期间有效；`unmap_shared_regions()`
   （`memory_shared.rs:882-890`）只在 `exit_binary_api` 里跑，而 main-loop exit
   函数在 `run_main_until` 返回、`stop_processes()` 之后执行
   （`crates/hammer-runtime/src/main_loop.rs:58-62`），即最后一个轮次之后。M2 要在
   exit 路径上跑一轮确认 unmap 之后不再有轮次；若以后新增运行期 unmap 路径，它必须
   先停掉持有该 region 堆引用的采集者。
8. `StatsMain` 只拥有一个段（普通字段，不是 `SpinLock<StatsSegment>`）；段内那把锁
   （D10）只被结构变更取用，值写与轮次不取。`MemHeap::usage()` 自身只读 `mspace`
   元数据、不取 mspace 锁（V19），所以采样不需要任何锁；若以后有人要把**整个段**
   放回一把锁里（而不是按 D10 只给结构变更加锁），那是新的设计，必须单独批准。
   契约不变：共享列的唯一写者是登记表里的采样函数。
9. VPP 的速率窗口常量与注释不一致（代码 `now + 2e-4` = 200µs，注释写 20ms；
   V13、6.3）。本设计逐字复制代码值，因此这里不需要再"决定"窗口大小；但
   `/sys/loops_per_worker` 的验收标准（第 8 节）要按 200µs 窗口与
   `K = exp(-1/20)` 的实际收敛时间写，不能照抄 VPP 注释里的 20ms 说法。
10. 运行期结构变化（M4 的 `/sys/node/*` 开关重建、插件热加堆、在线 `remove_entry`）
   走 D10 那把段锁，不需要第二把锁：VPP 在 `vlib_stats_validate` 扩容、`remove_entry`
   与 node 计数器重建时都是这么做的（`stats.c:481-524`、`collector.c:61-91`，V32）。
   M4 要验证的是两件配套的事：结构变更发生在 thread zero，或先由 `WorkerBarrier`
   停住 Data Worker；以及扩容（唯一的锁内分配）与客户端 seqlock 重试在真实并发下的
   持锁时长。

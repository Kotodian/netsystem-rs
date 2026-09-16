# ADR-0019: Stats Segment 的 VM、Heap 与计数所有权对齐

Status: proposed — 源码审查与设计草案，尚未批准实现
Date: 2026-09-16

## 1. 范围与结论

**用户要求：**直接查看 vendored VPP 源码，不调用技能，检查 Hammer stats
是否还有未对齐之处，先产出 ADR。本次只新增本文，不修改实现，不执行测试。
没有关联 issue，也没有把历史 ADR 的批准视为本次新增 API 的批准。

**用户后续明确要求：**heap 尚未对齐，共享内存也必须走 MemMain 的 VM map。
本 ADR 据此把 **MemMain VM → 映射内 MemHeap → stats 分配** 列为首要迁移，
不是可推迟的 allocator 优化。该要求明确了架构方向，不代表本次开始实现。

**用户追加要求：**补齐 Rust 类型、字段、方法的新增/修改/删除清单；删除
`StatsSegmentState`。第 6.1–6.9 节给出具体候选 surface；该类型的删除是明确要求，
不能把它换个名字继续放在 Arc/锁后面。本次仍只修改 ADR。

**已核实的项目事实：**当前 stats 的共享内存创建绕过 MemMain 的 VM 登记，
heap 仍是旧 Segment/Talc，二者都未对齐；此外还有计数更新所有权、完整目录操作
与 collector 的差异。部分共享布局和 fd 交付已有对齐。另有两个可由源码直接推导的
互操作问题：Gauge 写入浮点位模式，以及客户端对齐向量 header 起点计算错误。
不能仅凭 `STAT_SEGMENT_VERSION = 2` 或结构大小断言与 VPP 客户端兼容。

本次审查基线：

- Hammer HEAD：`53025b14180c61932e8c52f65ba2a2039fbad823`。
- `third_party/vpp` HEAD：`629fe2764bd997189fedd2d98cbe8dc9189c1ec3`；审查时该树无修改。
- 工作区已有 `AGENTS.md` 修改，本次保留，不纳入本文变更。
- 相关契约：`CONTEXT.md` 的 Stats Metric、Stats Owner、Runtime Statistic、
  Statistic Clear Baseline；ADR-0002 的 WorkerBarrier；ADR-0009 的注册归属；
  ADR-0010 的 worker 直接借用与删除第二套通用 metrics；ADR-0012 的 MemHeap。

文中“事实”指本次源码核实；“拟议决定”均需要后续批准；“待验证”不是实现授权。
VPP 是所有权与行为参照，不能把 C 的裸指针访问直接当作 Rust 安全证明。

## 2. VPP 源码证据

以下路径相对于仓库根，行号对应上述基线。

| 编号 | 源码与符号 | 核实的行为 |
| --- | --- | --- |
| V1 | `third_party/vpp/src/vlib/stats/shared.h:8`；`stats/stats.h:16` | directory type 0–9；entry 144 字节、名字 128 字节；共享头含 version/base/epoch/in_progress/directory_vector；version 为 2。 |
| V2 | `third_party/vpp/src/vlib/stats/init.c:51`，`vlib_stats_init` | 调用 `clib_mem_vm_create_fd` → ftruncate → `clib_mem_vm_map_shared` → `clib_mem_create_heap`，而非 stats 自建 mmap/allocator；共享映射首系统页放 stats header，后续范围建 locked heap；初始目录预留三个系统指标。 |
| V3 | `third_party/vpp/src/vlib/stats/stats.c:11`，`vlib_stats_segment_lock/unlock` | 结构更新使用可嵌套的 stats segment 锁；开始设置 in_progress，最外层结束增加 epoch 并 release 清除 in_progress；这是客户端结构校验协议。 |
| V4 | `third_party/vpp/src/vlib/counter.h:17,57,124`；`counter.c:52,94` | counter main 保存每线程、每对象的二维计数；validate 建立 stats 存储连接，正常增量直接更新当前线程行；跨线程求和若无 barrier 不保证精确瞬时值。 |
| V5 | `third_party/vpp/src/vlib/stats/stats.c:252,264,273,285` | Gauge setter 接收 `u64`；Timestamp setter 接收 `f64`，但赋值目标是 `uint64_t value`，执行数值转换，**不是** `f64` 位模式存储。 |
| V6 | `third_party/vpp/src/vpp-api/client/stat_client.c:225`，`copy_data`；`stat_client.h:32` | Scalar/Gauge 都把 entry.value 数值转换到客户端 `double scalar_value`；symlink 递归解析目标，按 index2 提取跨线程的一列。 |
| V7 | `third_party/vpp/src/vppinfra/vec_bootstrap.h:20,44,59,72` | 紧邻 vector data 的 `vec_header_t` 为 8 字节；`hdr_size * VEC_MIN_ALIGN` 表示整个前缀长度；allocation/header 起点不一定等于 data−8。 |
| V8 | `third_party/vpp/src/vlib/stats/stats.c:129,294,320,472,528,563` | remove 回收 entry/payload、维护空槽；name vector 支持设置和释放；validate 扩行列；symlink 支持创建与改名。 |
| V9 | `third_party/vpp/src/vlib/stats/collector.c:38,132,154` | Process 周期更新 node clocks/vectors/calls/suspends、名字与 symlink，并调用 collectors，最后更新 heartbeat；node 采样调用明确传 `barrier sync = 0`。 |
| V10 | `third_party/vpp/src/vlib/stats/init.c:15,123,138,177,245`；`provider_mem.c:26,50` | 注册 vector rate、loops 和 heap usage collector；early config 段名为 statseg，支持 socket-name、size、page-size、per-node-counters、update-interval；默认 32 MiB、普通系统页、node off、10 秒；socket 缺省取 runtime directory/stats.sock。 |
| V11 | `third_party/vpp/src/vpp-api/client/stat_client.c:88`；`stat_client.h:94,107,155` | Unix SEQPACKET 接收 fd，任意地址只读 mmap；用 server base 重定位指针；access start/end 校验 epoch/in_progress，可配置等待超时。C 实现的 volatile 不等于 Rust acquire。 |
| V12 | `third_party/vpp/src/vlib/stats/init.c:184,207,233` | File readiness 接受连接、发送 fd 后关闭连接；main-loop-exit unlink socket；stats 不走 Binary API 请求/响应。 |
| V13 | `third_party/vpp/src/vlib/stats/stats.c:607,717,753,784,809` | ring 每线程 head/sequence，覆盖式生产，无共享 consumer tail；发布 sequence 用 release。consume 函数注明仅测试/调试；不能把它解释为可靠队列。 |
| V14 | `third_party/vpp/src/vlib/node_cli.c:628`；`error.c:346` | clear runtime 在 barrier 中同步并保存 stats_last_clear，再更新时间；clear errors 保存 counters_last_clear。普通 counter.c 另有直接归零 API，不能声称 VPP 所有 clear 都只改 baseline。 |
| V15 | `third_party/vpp/src/vppinfra/mem.c:109`；`linux/mem.c:308,414` | `clib_mem_vm_map_shared` 进入统一 VM map；MAP_SHARED payload 前另有进程私有 VM header 页，加入 mem main 的 first_map/last_map；unmap 从同一链表摘除并解除映射。 |
| V16 | `third_party/vpp/src/vppinfra/mem_dlmalloc.c:41,325` | 在已映射范围建立 heap，识别 VM 页大小、创建固定 mspace、将 heap 控制块放在该范围，登记 mem main heaps；heap destroy 与外部 mapping unmap 是不同生命周期。 |
| V17 | `third_party/vpp/src/vppinfra/mem_dlmalloc.c:265` | 一次 mspace_mallinfo 采样构造 usage；free chunks 是数量，releasable 是字节；不按 C 历史字段名把数量误称字节。 |

## 3. Hammer 现状与差异

以下是源码事实；优先级表示拟议迁移次序，不代表本次已修复。

| 编号 / 优先级 | 当前 Hammer 证据 | 差异及后果 | 拟议处理 |
| --- | --- | --- | --- |
| H1 / 保留 | `crates/hammer-stats/src/protocol.rs:986,994,1079` | version/type code/entry/header/Counter 的主要布局与 V1 一致，已有 64 位 layout 断言；不构成端到端互操作证明。 | 保留布局校验，补真实 C/Rust 互操作。 |
| H2 / P0 | `crates/hammer-stats/src/segment.rs:648`；`metric.rs:68`；`protocol.rs:279` | `Gauge::store(f64)` 写 `to_bits()`，客户端 Gauge 却是 `u64`。例如写 1.0，VPP 读到的是 4607182418800017408 的数值。 | Gauge 改为整数值；删除此路径的浮点位模式转换。 |
| H3 / P0 | `segment.rs:38,206`；`protocol.rs:594`；`crates/hammer-ipc/src/stats_client.rs:343` | server 的向量前缀通常 64 字节，client 却把 data−8 传给要求 allocation 起点的 `vector_element_offset`；非空 simple/combined/histogram/name vector 不能按当前代码正常遍历。 | 复用 server 已有的完整前缀计算语义，修正全部 client 调用点。 |
| H4 / P0 | `segment.rs:337`；`stats_client.rs:181,250,727` | writer 仅对 in_progress 使用原子 store，其余共享头按字节复制；reader 整块 read_volatile，没有匹配的 Rust acquire 访问；读取中大量 `?` 在最终 epoch 校验前返回。 | 先定义并证明 mmap 并发访问，再修发布顺序、重试与错误分类。 |
| H5 / P1 | `segment.rs:363,658,682,706`；`metric.rs:116,153` | 每次增量都经过同一个 `Arc<SpinLock<StatsSegmentState>>`，查目录、类型及行列，返回 Result；调用者可以给任意 row；没有当前 worker 独占行的 API 约束。 | 建立 owner/current-thread 直接更新路径，把查找与扩容移出热路径。 |
| H6 / P1 | `segment.rs:724,819,868,1159`；`lib.rs:275` | remove 仅 crate-private，StatsMain 没有完整删除接口；NameVector 只有创建，Symlink 只有协议识别；Arc strong_count 被当作 teardown 的 worker quiescence 判断。 | 补生命周期闭环，worker 停止由 runtime 证明，引用计数不能替代 barrier acknowledgement。 |
| H7 / P1 | `crates/hammer-runtime/src/config/stats.rs:42,49`；`node.rs:437` | 唯一 `derive(Stats)` 使用点为 Sys；collector 只写 boottime/heartbeat；NodeMain 有 error descriptor/index 表，但该路径没有 stats 导出。 | 先接 runtime 的真实累计事实与 clear baseline，再加入具体 owner collector。 |
| H8 / P0，迁移首项 | `segment.rs:459`；`crates/hammer-infra/src/segment.rs:59,71`；`crates/hammer-infra/src/mem/mod.rs:601,1152,1432,1808` | stats 经旧 Segment 直接 memfd/ftruncate/mmap，未进入 MemMain VM map 链；payload 用 Talc，未登记为 MemHeap；主目录另有私有 Vec 副本。 | backing/map/unmap 全部经 MemMain；映射内建 MemHeap、登记 heaps；stats 使用该 heap。不能仅替换 Talc 而保留独立 mmap。 |
| H9 / P1 | `crates/hammer-runtime/src/config/stats.rs:90`；`crates/hammer-stats/src/lib.rs:132`；`crates/hammer-runtime/src/lib.rs:26` | StatsMain 先进入 OnceLock，再 FileMain 注册和逐项注册指标；后续失败会留下已发布 Main；unlink API 无生产调用，runtime exit inventory 为空。 | init 内部回滚、后续启动失败终止和清理、exit hook 纳入同一迁移；不以 Main 已存在判定启动完成，见 D6。 |
| H10 / P1 | `crates/hammer-stats/src/lib.rs:96,114,317,450`；`segment.rs:523` | StatsError 不实现 source 链；协议具体错误被丢弃为 Protocol；清理错误可覆盖启动错误；持 spin lock 分配控制缓冲并 sendmsg。 | 保留 owner-local error 类别和原始 source，移除锁内 I/O，明确主错误优先级。 |
| H11 / P2 | `metric.rs:35,221,235`；`stats_client.rs:659` | Ring 有 config/schema/allocation，却没有实际 produce/reserve/commit；client 返回所有槽原始字节，不读 head/sequence，不区分未写槽或覆盖。 | 标为部分实现；完成用途定义之前不承诺 ring 消费语义。 |
| H12 / P0（容量/页大小） | `crates/hammer-runtime/src/config/stats.rs:13,33` | 32 MiB 与 10 秒默认相符；size 固定，无 page-size/per-node 开关；必须提供 socket_path，而 VPP 可默认 runtime-dir/stats.sock。 | 第 6.6.1 节完整列出 statseg 配置迁移；size/page_size 随 M0 接通，不留到收尾；缺省 socket 路径差异显式保留。 |
| H13 / P1 | `crates/hammer-component-macros/src/lib.rs:263,510`；`crates/hammer-runtime/src/init.rs:209` | 宏逐字段注册，DuplicateName 时按类型 bind，失败没有整组撤销；并非 collector callback 注册。 | 保留声明机制，区分同一声明重入与不同 owner 抢名，整组安装失败原子化。 |
| H14 / P0 | `segment.rs:38,257`；VPP `vec.c:20` / `stats.c:294,498` | directory/vector 前缀宣称显式 heap 却没有 heap 指针，且对不同 vector family 统一前缀/对齐。 | 随 M0 按 D7.3 修复 heap 位、heap pointer、user header、完整前缀与 allocation 对齐。 |
| H15 / P1 | `protocol.rs:834,910`；VPP `stats.h:180` / `stats.c:607` | metadata 固定 stride=64，但 ring_layout 允许传更大 cache_line_bytes；不能据此宣称支持对应 C ABI。 | 按 D7.1/D7.4 复用 CacheLineAlignMark/CACHE_LINE，移除虚假的可变 cache-line 参数并补字段偏移断言。 |

### 3.1 向量读取错误的具体推导

设映射内某 counter vector allocation 起点为 `A`，数据在 `A + 64`：

1. `allocate_vector` 在 `A + 56` 写 8 字节头，记录 `hdr_size = 8`，数据按 64 对齐。
2. `StatsClient::read` 读取 `A + 56` 的头是正确的，但继续把这个地址当成
   `vector_element_offset` 的 `header_offset`。
3. 被调用函数要求 `header_offset + hdr_size * 8 == vector_offset`，实际成为
   `A + 56 + 64 == A + 64`，因此返回错误。
4. server 的 `StatsSegmentState::vector_element` 已使用 `data - hdr_size * 8`
   作为该参数，是可直接复用的语义参照。

这是源码可推导的逻辑问题，本文未执行运行时复现。修复需要真实映射读写测试，
不能增加读取源码并匹配字符串的“架构测试”。

### 3.2 Scalar 不能按类型名猜测

VPP 此版本共享 union 的 value 为 `uint64_t`。`set_timestamp(f64)` 会丢掉
小数部分，heartbeat 则直接递增同一整数槽。Hammer 当前 `Timestamp::store(u64)`
对整数秒的存储并没有反向编码问题；差异在 API 输入与 `ScalarBits` 的浮点转换
能力。拟保留整数存储及整数 heartbeat，消除把 ScalarIndex 自动当作 IEEE-754
bitcast 的含义。是否提供接受浮点秒且明确范围/截断规则的入口，另列审批。
不为了模仿 C 签名引入无用途的转换 API。

## 4. 拟议决定

### D0. 首先对齐 MemMain VM 与映射内 MemHeap 的完整存储链

**明确的用户要求：**共享 stats 内存必须走 MemMain 的 VM map，heap 必须对齐。
**源码依据：**V2/V15/V16。后续 counter/collector 设计以此为前提。

VPP 实际调用链为：

```text
vlib_stats_init
  clib_mem_vm_create_fd(page_size, "stat segment")
  ftruncate(fd, memory_size)
  clib_mem_vm_map_shared(NULL, memory_size, fd, 0, "stat segment")
    clib_mem_vm_map_internal
      reserve → MAP_SHARED → private VM header → mem main map list
  clib_mem_create_heap(base + system_page_size,
                       memory_size - system_page_size, locked = 1)
    fixed mspace → heap control block → mem main heaps
  stats directory/counter/string allocation from this heap
```

Hammer 的目标沿用现有权威和方法，不能在 stats 再实现一遍：

```text
StatsMain initialization
  MemMain::vm_create_backing(PageSize, "stat segment") → OwnedFd
  size backing file to checked, page-rounded mapping length
  MemMain::vm_map(None, length, PageSize, Some(fd), 0, alignment, name)
    MemVmMapHeader → MemMain first_map/last_map
  MemHeap::create_at(base + system_page_size,
                     length - system_page_size, true, "stat segment")
    fixed mspace → MemHeap control block → MemMain heaps
  stats shared directory and payload allocations from that MemHeap
```

上述是目标调用关系，不是声称当前跨 crate 可以编译的示例；可见性和只读映射
能力按 A1 定稿。`MemMain::vm_map` 当前传入 backing 即使用 MAP_SHARED，已有
reservation、页大小、保护页、VM header 和 map-list 登记，不缺第二套 vmap owner。
stats backing fd 属于 StatsMain 的存储生命周期，VM header 内的 fd 是记录，
不能同时承担第二份 close 所有权。

#### D0.1 两种 header 页必须分清

令 `B` 为 MemMain VM 返回的共享 payload 基址，`P` 为系统页大小：

```text
B - P .. B             MemMain 私有 VM header 页（非 fd 内容；正常 PROT_NONE）
B     .. B + P         stats SharedHeader 所在共享首系统页（fd offset 0）
B + P .. B + length    stats MemHeap：mspace、heap 控制块、directory、payload
```

`SharedHeader.base = B`，对外发送的是覆盖 `[B, B + length)` 的 backing fd。
VM 私有 header 不传给客户端、不占 fd offset zero，也不能被当成 stats 共享头。
若 backing 使用 huge page，VM reservation/length 按实际 backing 页大小对齐，
stats header 的保留长度仍按 VPP 的**系统页**契约核实；不得混用这两种页大小。

#### D0.2 heap 与分配归属

- MemMain 拥有 VM 平台策略和 mapping inventory；MemHeap 拥有固定 mspace 分配。
  StatsMain 拥有该 stats backing/mapping/heap 的使用生命周期和协议根。
- stats heap 的控制块和 allocator 元数据放在 stats 映射里，payload 从同一
  heap 分配；heap 必须出现在 MemMain 的 heaps inventory，map 必须出现在 VM
  inventory。只有“mmap 成功”或“heap 在给定地址”不算对齐完成。
- 共享目录、计数行、名字向量及字符串、ring payload 从 stats heap 分配；名称
  查找表、声明、collector 注册等进程私有集合仍走进程 Main Heap。
- 必须使用已有 `MemHeap::allocate/deallocate` 或有界 `activate()` scope；
  普通 Rust Vec 的内存布局不能冒充 C stats vector header。布局适配只属于 stats
  协议，不是新 allocator。激活 scope 不跨 `.await`，对应释放使用同一 heap。
- mapping、heap 和 shared payload 均固定容量，耗尽向 owner 返回具体错误，
  不回退 Main Heap、不以新 mmap 扩 heap、不再引入 stats-specific Talc/mspace。
- 清除 stats 对旧 `Segment`、`SegmentAllocation`、Talc 路径的依赖；只迁移此
  使用点，不顺带宣称全仓旧 Segment 已删除。

#### D0.3 创建失败、unmap 与进程退出

创建按 backing → VM mapping → heap → directory/payload → owner publication
推进。失败按相反顺序释放已完成的资源，并保留最初错误和可观察的清理错误：

1. heap 创建前失败：MemMain VM 路径回滚 map inventory/映射，关闭 backing。
2. 未发布 heap 上的目录创建失败：释放已构造 payload，使用既有
   `MemHeap::destroy` 移除 heap inventory，再 `MemMain::vm_unmap(B)` 移除 map
   inventory、解除 payload 与 VM 私有页，最后关闭 fd。当前 create 在 heap
   建立之后只写共享头（不可失败），该规则因此没有可达调用点；`MemHeap::destroy`
   仍只服务发布前回滚，不为它先造一个假调用者。
3. 发布后 heap 按当前 runtime 的 process lifetime 存活；正常最终退出先停 worker、
   collector 和 listener/unlink，随后进程退出释放映射。不能把目前只允许
   “未发布 heap 回滚”的 `destroy` 当成已支持运行期删除。
4. 若后续要求发布后的显式销毁，必须证明没有 live allocation/借用/active-heap
   selector/collector 引用，再扩展既有 owner 的销毁契约；不能直接 libc::munmap
   留下 MemMain.heaps/map-list 悬空项，不能增加另一份 stats 注册表代偿。

#### D0.4 StatsClient 的只读映射

现有 `StatsClient::connect` 的 `memmap2::MmapOptions::map` 也绕过客户端进程的
MemMain VM inventory。为落实共享内存统一走 MemMain 的要求，Hammer 客户端同样
经 MemMain 的映射/解除映射入口，**保留 PROT_READ 和任意 VA**；不从该映射重建
heap，不在客户端登记一份服务端 heap，不进入映射中的 allocator。

当前 `MemMain::vm_map` 硬编码 READ|WRITE，因此要在这个既有通用入口补齐只读
权限选择，并迁移 StatsClient 的 mapping 字段、connect 和关闭路径；不能先以
可写映射冒充只读，也不能因为缺一个参数就继续保留 memmap2 的平行 authority。
该权限 surface 属于 A1，复用既有 MemError；客户端启动保证 MemMain 平台初始化。
拟由现有 StatsClient 直接保留收到的 OwnedFd，先经 MemMain 解除映射，再关闭 fd，
保证 VM header 的 fd 记录在登记期间仍有效；不增加 fd/mapping wrapper。

VPP 外部 C stat client 自己调用只读 mmap 是 V11 的事实；Hammer 客户端统一进入
MemMain 是本项目的明确要求，二者不要混为相同实现。对外 stats ABI 不因此改变。

### D1. 区分三种更新，不把 stats 锁放进包路径

**拟议决定，依据 V3/V4/V9 与现有 worker 契约：**

| 操作 | 执行者与所有权 | 同步与观察语义 |
| --- | --- | --- |
| simple/combined counter 正常增量 | 当前 Data Worker，只访问自身 thread index 对应行；指标语义属于产生它的 owner | 通过真实 owner 的 `&mut` 借用更新；无分配、目录查找、全局锁或每包 recoverable Result。 |
| node/heap 等周期采样与派生值 | Main Thread 的 collector，调用各 owner 已明确允许的采样操作 | 不跨线程借用仍在执行的普通 Rust 可变状态；数值允许近似观察，不能用 stats epoch 声称精确快照。 |
| 注册、扩容、删除、clear baseline | Main Thread/control owner | 影响 worker 存储生命周期时由 runtime 使用既有 WorkerBarrier；stats 对外结构变更另执行 epoch/in_progress 协议。 |

counter 的 row/column 只是共享表示。对于包计数其含义是 thread/object；对于
VPP `/sys/loops_per_worker`，布局却是单行、线程作为列。不能把所有二维 metric
都硬编码为同一 worker-container 抽象。

扩容顺序：预检参与者与容量 → 必要的 worker barrier → 分配/准备替换存储 →
校验完成 → 发布目录与更新 owner 的现有存储关系 → 在借用全部结束后回收旧存储 →
释放 barrier。失败不得留下半更新的名字表、payload owner 或 worker 行。
不允许长期 `&'static mut` 指向可扩容行；不能只把现有 SpinLock 删除。

第 6.5 节限定 worker 行的所有权，不预设另一套控制面 row getter；runtime 对
worker 可见地址的安装与借用结束点仍须给出真实调用链，不能推导任意 worker 访问权。
复用既有 owner Vec/slice/Pool，不新增观察 wrapper、指针缓存 carrier、通用 TLS
容器或另一个原子发布句柄。当前 `&self + 任意 row` 更新接口不能作为最终替代。

VPP 的 stats 结构锁与 WorkerBarrier 面向不同参与者。StatsMain 的 SpinLock
直接保护 StatsSegment 实际状态；持锁后的嵌套调用传递 &mut StatsSegment，不再
重新获取锁。现有 `hammer-runtime -> hammer-stats` 依赖下，stats 不得反向依赖
runtime 或复制通用锁；同步原语的实际归属冲突见第 6.2 节，同步编排仍由
runtime/control 入口负责。`in_progress` 是外部 reader 校验字段，不是第二个
Data Worker 完成协议。

### D2. 共享目录的版本与内存安全是单独的契约

**拟议决定，依据 V1/V3/V7/V11：**继续区分结构一致性与数值采样。

- epoch 标识结构变化；in_progress 表示正在改结构。普通 counter 增量不改 epoch。
- writer 完成结构发布后 release 清除 in_progress；reader 必须通过匹配的 acquire
  和最终版本复检建立正确观察顺序。开始写入、epoch 和 directory pointer 的访问
  粒度及重排约束必须整体证明，不能宣称“加一个 acquire 就修完”。
- 不再把同时变化的 SharedHeader 整体 memcpy/read_volatile 当作原子快照。
  volatile 不提供线程同步，epoch 校验也不能补救已经发生的越界解引用或 Rust 数据竞争。
- 共享数据每一次解引用都先验证完整 span、溢出和必要对齐；ring 也必须先验证完整
  header 长度，再读取 header，不能只判断其起点在 mapping 内。
- 一次读取在本地得到成功或具体错误后，都先校验结构版本；结构变化则重试，稳定
  结构下才返回缺失、类型错误或畸形映射错误。读取前的最小 header/版本检查仍需即时拒绝。
- 将 header 本体 `data−8` 与完整前缀起点 `data−hdr_size*8` 分别计算，复用现有
  `vector_element_offset`，不为此新增 borrowed View 或指针 wrapper。
- 删除和重用必须保证旧 reader 在整个尝试期间只可能访问仍映射且已检查的范围；
  reader 使用旧 epoch 得到的索引不得静默指向同槽新对象。不能用服务器 Arc 计数
  代表外部客户端存活，也不能把无限保留旧目录当成无成本的回收方案。

完整的 Rust mmap 并发访问证明属于 A3 的实现前条件：限定支持的平台、字段宽度与
原子访问规则；外部进程修改的内存不暴露为可缓存的普通共享引用；同进程读取
worker 的非原子计数必须停住 worker 或由其 owner 提供合法快照。不能混用同一个
标量的原子与非原子并发访问来规避数据竞争。

VPP 自身的 C reader、heap 回收及 ring overwrite 做法只是证据，不是 Rust 安全
证明。本文不引入新的 RCU、reader registration 或共享锁协议；若最终证明需要
改变 ABI/回收模型，须先修订 A3，不得在实现阶段悄悄添加。

### D3. 内存边界复用限制

**事实：**当前 `crates/hammer-infra/src/mem/mod.rs:589` 已有 `MemHeap`，
`create_at` 在 :601，`activate` 在 :735；前者以及 MemMain 的 mapping 方法仍是
crate-private。不能因旧 CONTEXT/ADR 的迁移描述，误判该实现不存在或已可跨 crate 调用。

**落实 D0：**既有 MemMain VM 路径和 MemHeap 是唯一复用方向；本节只界定边界，
不把它降格为后续优化。VM 记录、heap 记录、fd/映射/heap 生命周期同时对齐。

这是共享 stats 存储用途，不把任意普通生产分配改为新的 heap 例外。
客户端只读、不操作 heap，仍可映射到不同地址；不能套用 SvmRegion 固定 VA
attach，或插入 SSVM header 改变 stats header 的 offset zero。Stats Segment 与
Binary API region、App Session FIFO segment 是不同协议。

需要按 A1 开放既有 MemMain/MemHeap 的最小通用调用边界，并补只读权限与明确
释放契约；不再新造 generic mapping owner 或 stats heap wrapper。
现有 public SVM owner 不能在不改变 header/attach 语义的情况下直接替代；现有
crate-private 方法也不能未经批准整体提升 public。旧 Segment 的全仓删除不属于
本 ADR，但本次迁移后的 stats 引用必须归零。

### D4. 保留声明机制，补齐真正的 collector 与 owner 生命周期

**拟议决定，依据 V8–V10/V14：**

1. `derive(Stats)` 继续表示 owner 的指标声明；`StatsRegistration` 继续是安装
   声明，不伪装成周期 collector，也不成为第八类 GlobalMain lifecycle hook。
2. 核心三项的固定槽位只声明一次：Sys 的 `#[stats(bootstrap = STAT_COUNTER_*)]`
   字段就是 VPP `foreach_stat_segment_counter_name` 的 Rust 对应声明，宏据此
   生成 bootstrap 创建步骤，并在创建时校验槽位索引等于声明的固定索引。
   segment 只保留索引与数量常量，不再手写这组名字。该步骤在 StatsMain::init
   之后、任何 owner 注册与 listener 交付之前执行；不依赖跨 image 遍历顺序
   碰巧把 heartbeat 放在 slot zero。
3. runtime 先建立 node calls/vectors/clocks/suspends 与 clear baseline 的真实
   owner 更新点，再导出 `/sys/node/*`、名字与 `/nodes/<name>/*` symlink。
   当前 error descriptor/index 的存在不等于已有运行计数与导出能力。
4. heap usage 从 MemHeap 所属 authority 获取，stats 不扫描 allocator 私有结构。
   现有 API 只提供部分容量事实，完整 usage 字段须按 A1 扩展；不能用零值冒充统计。
5. 周期 collector 只调用 owner 的具体操作；插件状态留在插件中，不把业务指针、
   erased object 或 `dyn` collector 放进 runtime/stats。Collector 保留 VPP 的数值
   private_data cookie（例如 heap index），但不允许用它保存擦除的插件指针。
6. Main Thread 如需读取普通 worker 状态，优先复用合法 owner snapshot；不存在
   此入口时，明确在短 barrier 内采样。后者是相对 VPP 无 barrier node sampling
   的 Rust 安全适配，会带来周期暂停，必须测量，不能隐瞒。
7. clear 更新 owner baseline 与 `/sys/last_stats_clear`，不让 CLI 维护第二套
   subtraction state。普通原始累计值与对外 post-clear delta 分清，不重复扣减。

VPP 的 `/sys/vector_rate` 是 collector 派生值，而当前 CONTEXT 要求 rate 在
诊断格式化时派生。本文保留此项目策略；是否新增 VPP 同名 rate 投影是独立待决项，
不能以“全面对齐”自动覆盖现有契约。业务接口/session/TCP 指标也不在此次自动扩张。

### D5. name vector、symlink 与 ring 分别声明支持程度

**拟议决定：**name vector 的 set/grow/free 和 symlink 的 add/rename/remove
与 runtime node 导出一起完成；client 读取 symlink 解析目标及指定列，处理目标
删除、索引越界、重用和循环，保留 alias 名字，不能简单返回 Protocol。
生命周期操作直接属于 StatsSegment，NameVector 只保存有效身份，不再造通用 metric manager。

Ring 属于覆盖式历史记录，不能改成 SPSC 可靠队列，也不引入共享消费进度。
当前无生产使用点，本轮核心迁移不新增 ring producer API；只保留明确标注的布局
能力，并停止将无 sequence 的全槽复制描述成有效记录快照。后续若启用，必须另行
批准 producer、schema、head/sequence、未写槽、覆盖与 reader 重试契约。
vendored C 普通 dump 对 ring 也没有提供完整 payload 读取，不能把 Hammer 当前
`MetricValue::Ring(Vec<Vec<u8>>)` 说成既有 VPP dump 兼容接口。

### D6. 初始化、错误与退出必须一起闭环

**拟议决定：**沿用现有 StatsMain::init 与 runtime init hook。StatsMain::init
在内部完成配置验证、mapping、heap 与核心目录准备，全部成功后才设置已有
OnceLock；其内部失败清理本次资源。listener bind 与 FileMain 安装由 runtime
statseg 在此之后完成，对应 VPP 在 vlib 侧的 `stats_segment_socket_init`。
调用者不取得一个等待单独发布的 StatsMain，也不增加二阶段初始化 API。

runtime 随后通过既有入口完成 FileMain 安装与 owner registration；全部成功后
才启动 worker、collector 和 File 事件分发。如果这一阶段失败，终止 daemon
启动并执行启动清理，已经安装的 process-lifetime stats heap 按 D0.3 随进程退出。
这里不承诺跨 stats/runtime 的可重试事务，也不把 OnceLock 的存在当作启动完成。
删除 `StatsMain::global().is_ok()` 就成功跳过的路径，不得失败后在同进程重试并
沿用部分安装结果。此启动失败策略取代原草案跨 crate create/publish 的设计。

退出复用 runtime 的最终 barrier/main-loop-exit：先停止新增访问与 listener，
unlink 自己的 socket 路径；已发布 heap 与映射按 D0.3 保留到进程退出。
已有外部映射可继续存活但 heartbeat 停止，不承诺从同一 OnceLock 重启新一代 worker。
peer 断开只结束该次 fd 交付；文件系统清理失败可观察，不能覆盖原始启动错误。
`handoff_segment` 只从 segment 取出稳定 backing fd，`sendmsg` 与缓冲分配都不
持有 stats spin lock。

| 失败类别 | owner / caller 的恢复动作 | 原子性与证据 |
| --- | --- | --- |
| 配置、容量、mapping/fd/I/O | stats 使用既有 StatsError，listener bind 使用 `RuntimeError::StatsListenerBind`；infra 原始失败保留 source；startup caller 终止该次安装并清理 | StatsMain::init 内部失败不安装 Main；后续 bind/File/owner 安装失败终止启动，不进入事件循环或启动 worker；每个目录操作自身仍须失败原子。 |
| 外部畸形共享数据、版本不支持 | stats protocol/StatsClient；调用者断开或报告具体问题 | 在 span 验证前不解引用；稳定 epoch 下保留具体类别，不塌缩为无字段 Protocol。 |
| 结构变化、重试预算耗尽 | StatsClient；继续尝试或返回既有 ClientRetryExhausted | 不把竞争中的暂时缺失报告成确定 MetricNotFound；使用有界预算，不忙循环十六次后假定生产者故障。 |
| 内部重复所有权、错误线程、绑定失效 | 实际 owner 的程序不变量 | 预检入口约束；包路径不为程序 bug 构造控制面错误，更不能丢弃错误继续写。 |
| peer 消失、退出清理 | runtime stats listener / stats lifecycle owner | 文档限定可忽略的交付中断（bind 时回收失效路径、退出 unlink 失败只告警）；其余 cleanup error 可观察且不替换 primary error。 |

复用 `StatsError`，不新增通用 result alias/error 框架。`Error::source()` 必须
实际返回 IO/allocation/protocol 原因。细分类别及替换 catch-all variants 属于 A7，
不能把内部 bug 全部再包装成 `PublicationFailed`。

### D7. 内存布局：packed、cache-line mark 与 allocation alignment

本节以 **Linux 小端、64 位指针、VPP cache line = 64 字节** 为明确布局基线。VPP 的
`vppinfra/cache.h:12–29` 默认 CLIB_LOG2_CACHE_LINE_BYTES=6，并允许构建覆盖；
当前 Hammer 的 `hammer_infra::align::CACHE_LINE` 与 CacheLineAlignMark 固定为 64。
只对上述相同构建条件声明目标布局，不能因 version=2 就接受其他 metadata 步长。
以下数值来自源码布局推导，尚未执行 C/Rust 布局 fixture。

#### D7.1 类型表示与逐字段偏移

| 对象 / VPP 证据 | Rust 表示 | size / align | 字段偏移或约束 |
| --- | --- | --- | --- |
| SharedHeader；stats/shared.h:44 | 保留 repr(C)，不 packed、不加 mark | 40 / 8 | version=0、base=8、epoch=16、in_progress=24、directory_vector=32；结构在映射首系统页，不把结构 size 扩为一页或 64。 |
| DirectoryEntry；stats/shared.h:23 | 保留 repr(C)，不 packed、不加 mark | 144 / 8 | type=0（4 字节），4..8 为自然 padding，union=8，name=16（128 字节）；不能 packed 成 140，也不能每项补齐到 192。 |
| DirectoryData union / SymlinkIndex；shared.h 的匿名 union/struct | repr(C) union / repr(C) struct | 8 / 8；8 / 4 | union 各 arm offset=0；symlink entry_index=0、vector_index=4。协议 type 原始值仍先验证，不能把非法整数直接读成 Rust enum。 |
| Counter；vlib/counter_types.h:16 | 保留 repr(C)，不 packed、不加 mark | 16 / 8 | packets=0、bytes=8；相邻 counter 步长 16，不是 64。simple/histogram 元素为 u64，步长 8。 |
| RingConfig；stats/stats.h:170 | 保留 repr(C, packed) | 20 / 1 | entry_size=0、ring_size=4、n_threads=8、schema_size=12、schema_version=16。 |
| RingBufferHeader；stats/stats.h:192 | 保留 repr(C, packed) | 28 / 1 | config=0、metadata_offset=20、data_offset=24。整个 allocation 按 cache line 对齐，但此 header 类型仍是 align=1。 |
| RingMetadata；stats/stats.h:180 | repr(C)，首字段用已有 CacheLineAlignMark；不 packed | 64 / 64 | cacheline0=0（size=0）、head=0、schema_version=4、sequence=8、schema_offset=16、schema_size=20、padding=24（40 字节）。每线程一个完整 cache line。 |
| vec_header_t；vppinfra/vec_bootstrap.h:23 | 继续使用明确的 8 字节 codec，不新增 packed Rust 结构 | C header 为 8 / 4；实际 vector data 至少按 8 对齐 | len=0..4、hdr_size=4、第 5 字节为位字段、grow_elts=6、vpad=7；header 位于 data−8。Rust [u8;8] 的 align=1 不代表可放宽共享指针与数据的对齐校验。 |
| name vector 的 vlib_stats_header_t；stats/stats.h:101 | 共享 allocation 前缀中的 u32，不另加 owner 类型 | 4 / 4 | entry_index 在 allocation 起点，后面 padding/heap 指针/vec header，详见 D7.3。 |

**只有 RingConfig、RingBufferHeader 是 packed。** 不能给 SharedHeader、
DirectoryEntry、Counter、vector 数据或原子字段所在的 RingMetadata 加 packed。

RingMetadata 的字段变更具体为：复用已有 `hammer_infra::align::CacheLineAlignMark`，
新增零尺寸 cacheline0 标记，保留所有实际字段及 padding，把类型级
`repr(C, align(64))` 改为 `repr(C)`，由标记提供同样的对齐。不造第二个对齐类型：

```rust
#[repr(C)]
pub(crate) struct RingMetadata {
    cacheline0: hammer_infra::align::CacheLineAlignMark,
    head: u32,
    schema_version: u32,
    sequence: u64,
    schema_offset: u32,
    schema_size: u32,
    padding: [u8; hammer_infra::align::CACHE_LINE - 24],
}
```

marker 是零尺寸字段，不是插入一块 64 字节数组；不能让 head 被推到 offset 64。
packed 字段只按值复制或经 addr_of!/read_unaligned/write_unaligned 操作，禁止构造
未对齐引用。RingMetadata.sequence 与共享头的原子访问位置必须分别验证绝对地址
的 8 字节对齐；repr(C) 或 packed 属性本身都不能替代 D2 的并发访问证明。

#### D7.2 哪些位置放 cache-line mark

| 位置 | 决定与依据 |
| --- | --- |
| 每线程 RingMetadata 起点 | 有 mark，复用上述 CacheLineAlignMark；数组步长为 64。 |
| stats 结构锁的 lock word 所在 cache line | 有 mark，但已经由通用 SpinLock 的私有 SpinState 提供；VPP lock.h:41 的 clib_spinlock_s 同样标记 cacheline0。不在 stats 复制锁或再加一套 mark。 |
| StatsMain / StatsSegment / CollectorRegistration / Collector | VPP stats.h 对这些 owner 记录没有 packed 或 cache-line mark；Rust 不新增标记，不承诺 std 集合/Socket/OwnedFd 的布局与 C 私有 owner 一致。StatsMain 因包含现有 SpinLock 自然继承其对齐。 |
| SharedHeader / DirectoryEntry / Counter / name 字符串 | 没有 mark。SharedHeader 的 page-aligned 地址来自 VM；counter 的行对齐来自 allocation，均不等于字段或元素需要 mark。 |
| 二维 counter 的外层指针数组与每个 worker 行 | VPP stats.c:498–513 的两层 vec_validate_aligned 都请求 cache-line alignment；两层数据起点均对齐 64，元素保持 8/16 字节 stride。没有逐元素 mark。 |
| MemVmMapHeader / MemHeap 控制块 | VPP mem.h:45、mem_dlmalloc.c:18 均无 packed/mark；复用 infra 既有 repr(C)，stats 不重新定义它们，也不追加 cache padding。 |

MemVmMapHeader 是 B−P 的进程私有页；MemHeap 控制块是 mspace 内按自身 alignment
分配的块，不一定在 B+P，也不要求 cache-line 对齐。当前 MemHeap 的三个 flags
是独立 u8，而 VPP 使用位字段；这是 allocator 私有表示差异，不是 stats 共享 ABI。
客户端不解码或调用服务端 allocator。本 ADR 对齐共享 stats 字节布局与 VM/heap
分配边界，不据此声称 Rust MemHeap 控制块可交给 C allocator 直接解释。

#### D7.3 vector 前缀必须包含真实的 heap 与 user header 语义

依据 `vppinfra/vec.c:20–55`，令 A 为 vector data 的对齐，H 为用户头长度，E 为
是否显式指定 heap，allocation 起点为 Q，则：

```text
prefix = align_up(H + 8 + (E ? 8 : 0), A)
D = Q + prefix                         vector data
D - 8 .. D                             vec_header_t
D - 16 .. D - 8                         E=true 时的 heap 指针
Q .. Q + H                             user header
hdr_size = prefix / 8
```

vector 第 5 字节的低 7 位为 log2_align，高位为 default_heap；这是当前目标 C
ABI 的编码，使用现有 byte codec。default_heap=0 必须有 D−16 的 heap 指针；
default_heap=1 表示使用当前 active heap，**不意味着必然分配于进程 Main Heap**。
VPP counter validate 在 stats heap 激活期间创建向量，正是后一种情况。

| stats allocation | VPP 创建方式 / 目标前缀 | 数据对齐与 stride |
| --- | --- | --- |
| directory | init.c 的 vec_new_heap，H=0、E=true；prefix=16，hdr_size=2，default_heap=0 | A=8；DirectoryEntry stride=144。heap 指针在 Q，vec header 在 Q+8，目录从 Q+16 开始。 |
| simple/combined/histogram 外层与内层 | stats.c 的 vec_validate_aligned，在 stats active heap 中，H=0、E=false；prefix=64，hdr_size=8，default_heap=1 | A=64；外层指针 stride=8，内层 u64/Counter stride=8/16。 |
| name vector 外层 | stats.c:294 的 vec_new_generic，H=4、E=true；prefix=24，hdr_size=3，default_heap=0 | A=8；Q 处为 entry_index，Q+4..8 padding，Q+8 heap pointer，Q+16 vec header，指针元素从 Q+24 开始。 |
| name 字符串 | stats.c:353 显式 stats heap，H=0、E=true；prefix=16，hdr_size=2，default_heap=0 | A=8；字符 stride=1，字符串终止 NUL 属于向量内容。 |

已有 allocate_directory 写 hdr_size=1 且 default_heap=false，却没有 heap pointer；
allocate_vector 对所有种类使用 64 字节前缀、default_heap=false，同样没写 heap
pointer。它们不能随 MemHeap 迁移原样保留。按上表分配并编码，释放始终使用真实
owner 的 MemHeap，客户端不得解引用前缀中的服务端 heap 地址。

hdr_size 来自完整前缀，不是恒定 8 字节；user header 只属于 name vector 外层。
长度、前缀、元素数乘法与 span 全部检查溢出，log2_align 必须与实际分配一致。
grow_elts 只描述 owner 确实可用的预留元素，不能把未知 allocator 余量当作容量；
未保留额外元素时写 0，不因此省略其字段。vpad 和未使用前缀字节初始化为零。

这要求调整既有 stats 私有布局计算，不新增公开 vector/layout API。标准 Vec、
AlignedVec、heap_boxed::Slice 都不能直接充当这个带前缀的共享 vector。

#### D7.4 ring 必须保持 data → metadata → schema 的实际排列

设 C=64、线程数 N、每线程槽数 R、每槽字节数 E；VPP stats.c:607 的布局为：

```text
ring allocation 起点                  按 C 对齐
header                                [0, 28)
data_offset = 28
所有线程的 slot bytes                  N * R * E 字节
metadata_offset = align_up(28 + N * R * E, C)
metadata[i]                           metadata_offset + i * C
schema_offset（有 schema 时）          metadata_offset + N * C
总长度                                metadata_offset + N * C + schema_size
```

schema 只有一份，每线程 metadata 指向同一 schema；schema_size=0 时各 schema_offset
为 0。不能把 metadata 放到 data 前面，也不能把 data_offset 擅自从 28 改成 64。
因此 slot bytes 不保证 u64 或业务 record 的自然对齐，RingSchema 只在字节 slice
上编码/解码，不能直接构造 &T。只有 metadata 区按每线程 cache line 对齐。

现有 ring_layout 接受可变 cache_line_bytes，但 RingMetadata 的 size/stride 固定
64；例如传 128 只对齐区段开头，第二个 metadata 仍只前进 64 字节。应删除该参数，
既有实现与调用者统一使用 infra CACHE_LINE 和 size_of::<RingMetadata>()，并断言
两者相等。不新增“可选 cache-line 大小”配置；支持其他 ABI 须统一变更 mark、
stride 和客户端解码。共享头没有表达该构建差异的字段，不能靠它自动协商。

#### D7.5 布局验收与变更清单

- 保留并补齐 protocol.rs 的 size_of/align_of/offset_of 常量断言：包含上述所有
  字段、SharedHeader 总大小/对齐、DirectoryEntry/Counter 对齐，以及 marker 的
  size=0/align=64。这些编译期断言不新增运行期 getter。
- RingMetadata 增 cacheline0；保留 head=0 和 sequence=8。RingConfig/Header 保留
  packed；其余共享类型不新增 packed 或 mark。复用 infra 的 CACHE_LINE/VEC_MIN_ALIGN，
  删除 stats 的通用 VECTOR_DATA_ALIGNMENT=64 假设，按 allocation family 选择对齐。
- 用 MemMain/MemHeap 创建真实映射，验证 directory、两层 counter、name vector、
  字符串的 allocation 起点、heap 指针、完整前缀、元素 stride 与实际数据地址；
  ring 使用至少两个线程和不能整除 64 的 payload 长度，验证 metadata 数组及 schema。
- vendored C/Rust fixture 比较同一目标下的 sizeof/alignof/offsetof、vec 位字段字节、
  完整非空共享数据；原子地址对齐与 packed 未对齐访问分别验证。仅核对总 size
  或只读 Rust 源码字符串不足以证明兼容。这些测试只在后续最终 pre-commit gate 执行。
- 热路径短叶子函数按实际汇编决定是否 inline；不批量 inline(always)，不以 cache
  padding 的数量冒充性能结果。本文没有执行布局 fixture、测试或性能测量。

## 5. 分层隔离契约

| 层 | 可以调用/持有 | 不可以调用/持有 | 验证边界 |
| --- | --- | --- | --- |
| hammer-infra | MemMain VM/backing/map inventory、MemHeap/heaps inventory、allocation facts | stats type/path、StatsMain、runtime barrier、collector 业务 | VM 登记/摘除、heap 登记/回滚、只读权限、耗尽测试。 |
| hammer-stats | 通过 MemMain/MemHeap 持有 stats 存储生命周期；目录/布局、共享 payload、backing fd 借用 | 直接 mmap/munmap、旧 Segment/Talc、另一个 heap owner、DataPlaneMain、runtime 反向依赖、socket/listener 代码 | stats 编译、VM/heap inventory 与真实布局/生命周期测试。 |
| hammer-runtime | Main Thread 初始化、FileMain、Process、WorkerBarrier、runtime node 事实 | 替插件拥有指标状态；跨线程裸借用；通用共享 metrics store | runtime worker/collector/clear 行为测试。 |
| service/plugin owner | 自己的 counter 语义、当前 worker 更新与 owner snapshot | 修改其他 worker 行；把私有协议状态传给 stats 保存 | 接入时做具体 owner 测试；本 ADR 不扩业务指标。 |
| hammer-ipc::StatsClient | MemMain 只读 map/unmap、校验、重定位、返回拥有的数据 | memmap2 平行映射路径、客户端重建服务端 heap、共享消费 tail、写 stats、无界解引用 | 独立进程只读映射、客户端 VM inventory、并发结构变化测试。 |
| hammerctl/owner ctl | 已发布统计的选择与格式化 | 第二套计数存储、查询时推进业务指标 | 后续接入现有 owner ctl；本次不创建命令。 |

边界检查以 compilation、真实 lifecycle 和行为为主。`cargo metadata --no-deps`
可辅助审查依赖；`rg` 仅用于迁移删除清单，不能替代行为证明。

## 6. 新增/修改 API 审批清单

MemMain VM 与 MemHeap 的架构方向已有用户明确要求。以下**具体 API 变更仍为
拟议、未批准**；本次授权是完成 ADR，实施这些 API 没有获批。
候选类型、字段和签名见下文；未决所有权证明不能由编译错误临时补出新 API。

| 项 | 最终结果、owner 与消费者 | 现有 surface 为什么不足 | 拟议变更与审批边界 |
| --- | --- | --- | --- |
| A1 | MemMain 的 VM/backing/map inventory + MemHeap/heaps inventory；StatsMain 创建，StatsClient 只读 attach | `vm_create_backing/vm_map/vm_unmap`、`MemHeap::create_at/destroy` 已存在但 crate-private；vm_map 目前硬编码 RW，destroy 仅限发布前回滚；usage 未完整公开 | 开放既有通用入口的最小可见性，扩 vm_map 的只读权限选择，明确 backing 长度/页大小/fd 生命周期，复用 MemError；不新造 mapping/heap 类型；销毁仍遵守 D0.3，不默认授权运行期 destroy。 |
| A2 | 当前 worker 直接更新已经验证的 simple/combined/histogram 存储 | 现有 descriptor 只有 name/index，通过 &StatsMain + 任意 row 写入且加锁 | 删除原 add 转发链与草案三套 row getter；第 6.5 节限定真实 worker owner 的集成要求，不预造接口。 |
| A3 | 结构发布与 client 稳定读取；stats/IPC 共同消费 | 整块 volatile header、早退和共享内存引用未构成 Rust 证明 | 优先修复目录更新与既有 list/read；跨进程原子/回收完整证明是前置条件；若须改 ABI 或 client 公共 API，重新列明审批。 |
| A4 | Gauge 与 ScalarIndex 按 VPP 数值解释 | Gauge::store 的 f64 位模式与 GaugeValue(u64) 冲突，ScalarBits 可误导 bitcast | 删除 Gauge::store 转发，统一由 StatsSegment::set_gauge 写 u64；MetricValue 的 scalar 暴露改为明确整数语义，清理误导的 f64 bitcast；Timestamp 浮点输入不自动新增。 |
| A5 | runtime node/heap collector 周期投影真实 owner 事实 | StatsRegistration 只有启动安装 callback；NodeMain 缺少完整统计更新/导出闭环 | 拟增加 stats-owned `CollectorRegistration` 声明及具体注册/执行入口，运行由现有 Process 驱动；不得存插件引用或 erased private_data；节点事实与采样 API 需分别列出。 |
| A6 | name vector/symlink/remove 全生命周期 | NameVector 只有 new，remove 不对 owner 开放，client 拒绝 symlink | 操作直接归 StatsSegment；DirectoryIndex 保持单一 u32；删除/重用由实际 owner 生命周期管理，client 以 epoch 校验目录版本；不增加代际版本。 |
| A7 | StatsError 保留 category/source/cleanup precedence | 当前 From 丢协议事实，source() 默认 None，部分清理覆盖主错误 | 在现有 error owner 内细化类别；写明各 variant 消费者及恢复动作后批准，不新增万能 error carrier。 |
| A8 | 既有 StatsConfig 完整表达 statseg 输入，写入 StatsSegment 的实际配置事实 | 当前仅 [stats] 的 socket_path/interval，容量固定，page/node 不可选 | 按 6.6.1 改段名和字段、复用 Byte/PageSize/Duration、迁移全部配置调用点；size/page 随 M0，node/interval 随 M4；保留显式 socket 路径要求，不新增配置 owner 或发布 API。 |

Ring producer 不在上述实现批准范围内；启用时另立完整用途与 API 审批项。
没有拟议 `StatsWorkerView`、通用 per-thread container、collector trait object、
第二套 metrics registry 或 stats 专属 heap 类型。

### 6.1 类型总表与明确删除项

以下 Rust 定义是待实现的具体设计，不表示已经编译通过。按用户明确要求，
`SpinLock<T>` 的 `T` 就是受保护的真实状态；本设计使用
`StatsMain.segment: SpinLock<StatsSegment>`，没有另一层 State。

| 类型 | 动作 | 最终含义 |
| --- | --- | --- |
| `StatsMain` | 修改 | 仅持有被结构锁保护的 StatsSegment，对应 vlib_stats_main_t。 |
| `StatsSegment` | 修改，类型窄公开、字段 private | 直接保存下节的 segment 字段；不是 Arc/锁的代理；不 Clone。 |
| `StatsSegmentState` | **整个删除** | 删除定义、impl、import；不得改名、type alias 或包在另一个锁内保留。 |
| `RecordKind` | 删除 trait、Storage/Handle 及全部 impl | 删除依赖 StatsSegmentState/SegmentAllocation 的泛化注册层。 |
| `metric::layout::{Scalar,Simple,Combined,NameVector,Histogram,Ring}` | 删除六种中间类型和 TryFrom impl | concrete add 方法直接验证输入并创建目录项，不再 declaration → layout → storage。 |
| `DirectoryIndex` | 保留一个 u32 的表示 | 不增加代际版本；目录身份是当前 segment 内的 index，生命周期由 owner 管理。 |
| `Gauge/Timestamp/SimpleCounter/CombinedCounter/Histogram/NameVector` | 修改 | 保存有效 DirectoryIndex；不同时表示未注册声明与已绑定指标。 |
| `ScalarBits`、`protocol::Gauge` / `GaugeValue` | 删除及清理转换 | MetricValue 的 scalar/gauge 直接是 u64，消除 bitcast 歧义。 |
| `DirectoryDataPointer/StringVectorPointer` | 删除 wrapper/re-export/转换 | 协议内部直接使用已有 pointer arm；client 解码为 checked offset。 |
| `CollectorRegistration`、`Collector` | 新增 | 分别为待注册输入与已安装的 collector；字段对应 VPP collector reg/collector。 |
| `HeapUsage` | 移到 M4 | 拥有一次 usage 采样的数值，不保存 heap pointer 或借用；本次没有 owner 消费采样时先不新增空 API，随 M4 的 heap usage collector 一起加入。 |
| `StatsClient` | 修改 | 直接持有 MemMain 的只读映射、长度和收到的 fd。 |
| `SharedHeader/DirectoryEntry/DirectoryData/DirectoryType/Counter` | 保留 ABI 定义、改访问方法 | 不新增重复 header/entry 状态。 |
| `Ring<T>/RingSchema/RingConfig/RingBufferHeader/RingMetadata` | 保留既有能力、按 D7 明确布局 | Config/Header 保留 packed；Metadata 复用 CacheLineAlignMark；统一实际 stride，删除 ring_layout 的可变 cache-line 参数；本次不扩 producer 或有效记录 reader。 |

StatsSegment 不保存线程身份字段、目录代际数组或额外发布状态；DirectoryIndex
只有一个 u32。没有 RefCell/RefMut、listener_index setter 或另一层 State。

### 6.2 逐字段对照 vlib_stats_segment_t

```rust
pub struct StatsMain {
    pub segment: SpinLock<StatsSegment>,
}

pub struct StatsSegment {
    collectors: Pool<Collector>,
    directory_vector_by_name: HashMap<NameBytes, DirectoryIndex>,
    dir_vector_first_free_elt: Option<u32>,
    update_interval: Duration,
    memory_size: usize,
    log2_page_sz: u8,
    node_counters_enabled: bool,
    heap: NonNull<MemHeap>,
    mapping: NonNull<u8>,
    memfd: OwnedFd,
}

static STATS_MAIN: OnceLock<StatsMain> = OnceLock::new();
```

`Pool` 复用 hammer-infra 已有实现，名字 HashMap/collector Pool 属于进程 Main
Heap；目录、counter/name/ring payload 属于 stats MemHeap。StatsSegment 的字段
保持 private；StatsMain 直接暴露已有 SpinLock 字段，调用者取得标准 guard 访问
segment 的领域操作，不再增加一个仅转发 lock() 的 segment() 方法。
StatsSegment 是 backing、mapping、heap 和共享 allocations 的生命周期 owner；
不是因为字段写成 NonNull 就自动具有所有权。这里仅在既有底层 API 边界保留
`mapping` 与 `heap` 两个内部地址，具体责任见下面的“指针与借用边界”。
不保存独立 shared_header 或 directory_vector 指针字段。

| VPP 字段 | Rust 对应 | 保留的语义 / 明确的 Rust 表达差异 |
| --- | --- | --- |
| `collectors` | `Pool<Collector>` | 进程私有 installed collector pool，不是 Vec of static declarations。 |
| `directory_vector_by_name` | `HashMap<NameBytes, DirectoryIndex>` | 进程私有名字到目录索引的映射。 |
| `directory_vector` | 不再单独保存字段 | 根地址取自共享头已有 directory_vector 字段；目录 allocation 属于 StatsSegment，访问时读取真实 vector 长度并短期借用；没有私有 Vec 副本。 |
| `dir_vector_first_free_elt` | `Option<u32>` | 空槽链的首索引；None 对应 VPP 内部 invalid index，未增加版本维度。 |
| `update_interval` | `Duration` | 已验证的采样周期；f64 秒改成 Rust 时间值，不重复保存在另一个 Main。 |
| `stat_segment_lockp` | `StatsMain.segment: SpinLock<StatsSegment>` | 锁直接保护实际 segment 状态；不能在 StatsSegment 内再放一把 SpinLock<()>，也不再包 State。 |
| `locking_thread_index` | 不新增字段 | VPP 用 runtime thread index 判断递归持锁；Rust 嵌套调用收到同一 guard 借出的 &mut StatsSegment，不再获取锁，因此不保存锁所属线程。更不能替换为 OS ThreadId。 |
| `n_locks` | 不新增字段 | 同一外层 guard 覆盖完整操作，嵌套 owner 方法不重新锁；没有第二份手工递归计数。 |
| `vlib_stats_segment_lock/unlock` | 私有 `DirectoryWrite`（RAII，Drop 时发布 epoch） | in_progress/epoch 只有这一处写入入口；一次目录事务开闭一次，不是每个嵌套步骤各发布一次，见 6.3。 |
| `socket` | 不进入 StatsSegment；listener File 归 runtime statseg 模块 | VPP 的 `vlib_stats_segment_t.socket` 只被 `stats_segment_socket_init` / `stats_socket_accept_ready` 使用；Hammer 的对应层是拥有 FileMain 的 runtime `config::stats`，segment 不再持有 socket 或 listener 索引。 |
| `socket_name` | runtime `StatsConfig.socket_name` | bind、File 描述与退出 unlink 共用同一配置值；VPP 的 `unlink(sm->socket_name)` 对应 `exit_stats_main`。 |
| `memory_size` | `usize` | 校验且按 backing 页大小对齐后的映射长度；不保存负的 ssize_t。 |
| `log2_page_sz` | `u8` | 实际 backing 页大小指数；PageSize Config 在创建阶段解析，不把输入 enum 当实际页大小。 |
| `node_counters_enabled` | `bool` | 发布的 node 采样开关，不只留在 StatsConfig。 |
| `heap` | 私有 `NonNull<MemHeap>`，内部通过 `heap(&self) -> &MemHeap` 借用；activate 窗口与 `&mut self` 目录事务并存时先复制该 NonNull 再 `as_ref()` | create_at 返回的控制块地址；此地址不是 heap 范围起点，不能按固定偏移重建。生命周期由 StatsSegment 负责。也不能直接把 `&MemHeap` 存成字段（自引用）。 |
| `shared_header` | 删除该 Rust 字段，保留 `mapping: NonNull<u8>` | 映射根是 MemMain::vm_map 的返回值；共享头在 offset 0，字段级访问从 mapping 派生，不缓存第二个地址。 |
| `memfd` | `OwnedFd` | 共享 backing 的唯一 Rust fd owner；VM header 只记录该 fd。 |

payload 一律从 stats heap 取用，对应 VPP 的 `vec_new_heap` /
`clib_mem_heap_free (vec_get_heap (v), ...)`，并按 VPP 的规则决定是否需要
`MemHeap::activate()` 窗口（即 `clib_mem_set_heap (sm->heap)` 后 restore）：

| VPP 分配 | heap 决定方式 | Hammer |
| --- | --- | --- |
| 目录 vector、名字 vector 外层与字符串（`vec_new_heap` / `vec_new_generic (..., sm->heap)`，`vlib/stats/init.c:107`、`stats.c:311,353`） | vector 前缀自带 `_vec_heap`，`_vec_free` 从前缀取 heap（`vppinfra/vec.h:115,349`） | `allocate_vector(..., explicit_heap = true)` / `allocate_string`；无 activate 窗口，释放仍显式传 heap。 |
| 计数行与它的外层（`vec_validate_aligned`，`vlib/stats/stats.c:420,498,501,510,513`） | default-heap vector，`_vec_free` 在“当前 heap”上释放 | `allocate_vector(..., explicit_heap = false)`，`validate` 整个替换窗口（分配与释放替换下来的行）都在 `activate()` 内，行先写进未发布的 replacement 外层，窗口里不做 std 分配。 |
| ring payload（`clib_mem_alloc_aligned` / `clib_mem_free`，`stats.c:178,648`） | 隐式 heap，必须先把 segment heap 设为 active | `add_ring` 的分配与失败回滚、`release_payload` 的 ring 分支各开一个 `activate()` 窗口。 |

`activate()` 窗口也是隐式分配的归属边界：窗口内任何经全局分配器的分配都落在
stats heap，因此窗口里不能构造会在 Main Heap 下 drop 的 `String`/`Vec`——目录
事务、名字查找表和错误值都留在窗口外。`MemHeap::allocate/deallocate` 仍由调用者
携带 heap，`active_heap` 只决定窗口内隐式分配的归属。

#### 指针与借用边界

原草案把 C 的 heap/shared_header/directory_vector 全部照抄成 NonNull，缺少了
Rust owner 与访问边界的说明，撤销该字段方案。不是每个 C 指针都需要一个 Rust
指针字段，也不能把消除 NonNull 的数量当作内存安全证明。

| 对象 | 实际拥有者与 Rust 表达 | 为什么不能随意替换 |
| --- | --- | --- |
| backing fd | StatsSegment 的 OwnedFd | 已有的 fd owner；不是裸 fd，也不需要新 wrapper。 |
| 映射范围 | StatsSegment 保存 MemMain 返回的 mapping 和 memory_size，负责未发布失败回滚 | 现有 MemMain 只有底层 map/unmap，没有可直接复用的带生命周期映射 owner。用 Box::from_raw 会套用错误的分配/释放契约；改成 usize 也不会产生所有权。 |
| heap 控制块 | StatsSegment 保存 create_at 返回的 heap 地址；私有 heap() 将有效借用限制在 &self 内 | MemHeap::create_at 在 mspace 内调用 mspace_memalign 分配控制块，再登记 MemMain.heaps；控制块不能搬出映射，也不能假定处于 B+P。 |
| 共享头 | 不存地址字段；从 mapping 的 offset 0 执行字段级读写 | 不缓存 SharedHeader 值；发布后不向调用者提供整个 &mut SharedHeader，与原子字段访问并存时尤其不能制造覆盖整个头的独占引用。 |
| 共享目录 | 不存地址字段；已发布根只在 SharedHeader.directory_vector 中保存 | StatsSegment 负责前缀、容量、扩容和释放；进程内受锁保护的短期访问使用 &[DirectoryEntry] / &mut [DirectoryEntry]，不让引用跨 guard 或目录替换。 |

`MemHeap::create_at` 的控制块分配见 `crates/hammer-infra/src/mem/mod.rs:703`；
现有 `SsvmPrivate::heap`（`svm/ssvm.rs:390`）也从 owner 返回 `&MemHeap`，但
它使用不同的共享头布局，不能把整个 SsvmPrivate 搬来替代 stats segment。
`heap_boxed::Slice` 是已有 owning allocation，但目前 crate-private、不可增长、
没有 stats vector 前缀；`AlignedVec` 与标准 Vec 也不具备该前缀。不能把普通 Vec
的 as_ptr 发布为 VPP vector，不能以 Box/Vec 名义掩盖一个不匹配的释放布局。
目录在未发布创建/扩容准备阶段的 allocation 是临时拥有资源；提交时更换共享根，
失败时释放准备 allocation，不增设第二份常驻目录根。

借用与释放约束必须落在真实 owner 上：

- 不把 mapping/heap pointer 暴露为公共 getter；heap() 仅在 stats 模块内部使用，
  外部 collector 通过具体采样操作获取拥有的数值。
- 不在 StatsSegment 内同时存 owner 和指向自己的 `&MemHeap` / `&SharedHeader`；
  那是自引用。也不伪造 `'static` 借用来跨越未发布失败回滚。
- 私有目录借用由 &self / &mut self 约束，只用于已排除同进程并发写的目录操作；
  不能通过借用整个目录逃避 scalar 原子访问要求。客户端读取外部可变内存仍按 D2
  的字段级访问规则，不复用服务端 slice 借用。
- 初始化未完成时，局部拥有已创建资源，按 D0.3 反向清理；只有全部资源完整时才
  构造 StatsSegment。StatsMain::init 内部后续失败结束借用后回滚；成功安装后保持进程
  生命周期。heap destroy 前释放 allocations，随后 vm_unmap(mapping)，最后关 fd。
- 不直接为这两个 NonNull 补一份无条件 unsafe Send/Sync；是否允许跨线程移动与
  访问由实际锁 payload、allocator 和借用约束证明，外层 SpinLock 本身不是证明。

这保留了必要的底层地址操作，没有新增 StatsHeap/StatsMapping 或借用 wrapper。
若以后需要通用拥有型 VM/heap API，应在 hammer-infra 另列真实资源所有权契约；
本 ADR 不把不存在的类型写成已经可复用的解决方案。

因此 lockp/locking_thread_index/n_locks 这一组不逐字段机械复制：其“外层互斥、
同线程嵌套、最外层发布”语义由一次 SpinLockGuard 和嵌套 &mut 调用表达。这是
明确的 Rust 适配；不能保留不驱动任何行为的线程编号或计数器冒充对齐。

当前实现在 hammer-infra::sync 提供 SpinLock，而仓库规则要求通用同步原语归
hammer-runtime::sync；当前 runtime 又依赖 stats。这个已有归属/依赖冲突必须在
实施前解决，本文不通过 stats 反向依赖 runtime、复制锁或新增 stats 专用锁来
掩盖。上面的 SpinLock/SpinLockGuard 指已有通用原语，锁内值的设计不依赖给它
另起名字。

| 当前字段 | 最终动作 |
| --- | --- |
| `StatsSegment.state: Arc<SpinLock<StatsSegmentState>>` | 删除；真实状态由上面 StatsSegment 直接保存，唯一结构锁在 StatsMain.segment。 |
| `StatsSegmentState.mapping: Segment` | 删除旧类型，改为 MemMain 返回的 mapping 根；由 StatsSegment 管理 heap/memfd/size/page facts。 |
| `StatsSegmentState.header: SharedHeader` | 删除本地副本，只操作 mapping offset 0 的共享头；也不新增 shared_header 指针缓存。 |
| `StatsSegmentState.directory_vector: Vec<DirectoryEntry>` | 删除私有副本；根取自共享头，不替换为常驻 NonNull<DirectoryEntry> 字段。 |
| `directory_block: SegmentAllocation` | 删除；目录的 allocation 直接由 stats heap 管理。 |
| `payloads: Vec<Vec<SegmentAllocation>>` | 删除；按照 entry family 的真实 allocation 根和协议前缀释放。 |
| `names / first_free` | 分别归 directory_vector_by_name / dir_vector_first_free_elt。 |
| `tearing_down` | 删除；候选回滚与进程生命周期明确，不再模拟可重启 teardown。 |
| `StatsMain.socket_path` | 删除此字段，路径事实归 runtime `StatsConfig.socket_name`；stats crate 不再有 socket 字段。 |

### 6.3 持锁、发布与方法接收者

结构入口取得一次既有 SpinLockGuard；内部方法接收真实 `&mut StatsSegment`。
嵌套 add/validate/remove 调用不调用 StatsMain::global，也不重复 lock。计数热路径
不使用这些结构入口。一次目录事务的 in_progress/epoch 更新围绕完整提交阶段，
不是每个嵌套小函数各发布一次。

`in_progress`/`epoch` 的写入入口只有私有 `DirectoryWrite` 值：结构入口在已持有的
`&mut StatsSegment` 上取它打开事务，它在 Drop 时增加 epoch 并 release 清除
in_progress，对应 `vlib_stats_segment_lock/unlock`
（`third_party/vpp/src/vlib/stats/stats.c:11,32`）。事务随该值的作用域开闭一次，
不是每个嵌套步骤各发布一次，也不能忘记关闭；准备阶段产生的、尚未被目录引用的
allocation 不属于该事务的提交阶段。

锁内不做 socket I/O、未知 collector callback、不可预测分配或进入 WorkerBarrier。
需要分配的操作先准备容量/storage，再在短锁区复核目录 epoch 和参与者并提交；
若基础发生变化，释放准备资源并重试。不能把删掉 per-packet lock 写成“stats
从此不需要结构锁”，也不能因为 VPP 的 C 实现锁内分配就违反当前 Rust 锁约束。
受影响 worker 存储的操作先建立 runtime 的 barrier，再进入短 stats 提交区；
barrier 证明 worker 已停止，stats 锁不承担 worker acknowledgement。

```rust
impl StatsMain {
    pub fn init(
        name: &str, size: usize, page_size: PageSize,
        update_interval: Duration,
        node_counters_enabled: bool,
    ) -> StatsResult<()>;
    pub fn global() -> StatsResult<&'static Self>;
    pub fn collect(&self) -> StatsResult<()>;
}

impl StatsSegment {
    pub(crate) fn create(
        name: &str, size: usize, page_size: PageSize,
        update_interval: Duration,
        node_counters_enabled: bool,
    ) -> StatsResult<Self>;
    pub fn segment_fd(&self) -> RawFd;
    fn heap(&self) -> &MemHeap;
    fn directory(&self) -> &[DirectoryEntry];
    fn directory_mut(&mut self) -> &mut [DirectoryEntry];
    pub fn find(&self, name: &str, expected: DirectoryType)
        -> StatsResult<DirectoryIndex>;
    pub fn add_gauge(&mut self, name: &str) -> StatsResult<Gauge>;
    pub fn add_timestamp(&mut self, name: &str) -> StatsResult<Timestamp>;
    pub fn set_gauge(&mut self, index: DirectoryIndex, value: u64) -> StatsResult<()>;
    pub fn set_timestamp(&mut self, index: DirectoryIndex, value: u64) -> StatsResult<()>;
    pub fn add_simple_counter(&mut self, name: &str) -> StatsResult<SimpleCounter>;
    pub fn add_combined_counter(&mut self, name: &str) -> StatsResult<CombinedCounter>;
    pub fn add_histogram(&mut self, name: &str) -> StatsResult<Histogram>;
    pub fn add_name_vector(&mut self, name: &str, length: u32) -> StatsResult<NameVector>;
    pub fn add_ring<T: RingSchema>(&mut self, descriptor: Ring<T>)
        -> StatsResult<DirectoryIndex>;
    pub fn validate(&mut self, index: DirectoryIndex, row: u32, column: u32)
        -> StatsResult<()>;
    pub fn remove_entry(&mut self, index: DirectoryIndex) -> StatsResult<()>;
    pub fn add_symlink(&mut self, target: DirectoryIndex, column: u32, name: &str)
        -> StatsResult<DirectoryIndex>;
    pub fn rename_symlink(&mut self, index: DirectoryIndex, name: &str)
        -> StatsResult<()>;
    pub fn set_name(&mut self, index: DirectoryIndex, element: u32, value: &str)
        -> StatsResult<()>;
    pub fn update_interval(&self) -> Duration;
    pub fn node_counters_enabled(&self) -> bool;
}

/// VPP `STAT_COUNTER_*`: the fixed directory indexes the segment owns.
pub const STAT_COUNTER_HEARTBEAT: u32;
pub const STAT_COUNTER_LAST_STATS_CLEAR: u32;
pub const STAT_COUNTER_BOOTTIME: u32;
```

保留的公共操作必须有实际职责；不把 VPP 内部 C helper、Rust 私有解引用或协议
布局计算各包装成公共方法。上面的 create/heap/directory/directory_mut 是内部实现
边界，尤其借用方法用于集中 unsafe 检查，不是给外部调用者增加第二套 stats API。

| 保留操作 | VPP 对照与实际消费者 |
| --- | --- |
| add_gauge / set_gauge、add_timestamp / set_timestamp | stats.h 的对应 add/set；runtime Sys/collector 与指标 owner 使用，descriptor 不再重复提供 store。 |
| add_simple_counter / add_combined_counter / validate | add_counter_vector / add_counter_pair_vector / validate；负责目录及容量，不负责每包更新。 |
| add_name_vector / set_name、add_symlink / rename_symlink | add/set_string_vector 与 add/rename_symlink；名字、alias 的真实目录生命周期。删除统一走 remove_entry。 |
| find / remove_entry / register_collector | find_entry_index / remove_entry / register_collector_fn；只在 StatsSegment 实现一次。 |
| add_histogram / add_ring | 对应现有 histogram/ring 注册能力；本次不顺手补齐 VPP 的全部 ring reserve/commit/consume 方法。 |
| update_interval / node_counters_enabled | 前者对应 get_segment_update_rate；后者是 runtime 跨 crate 选择 node 采样所需的只读入口。保留这两个实际调用点，不为其他 private 字段自动生成 getter/setter。 |
| StatsMain::init/global、collect | 初始化/取得唯一 owner、执行已注册采样。fd 交付与退出 unlink 归 runtime statseg 的 listener callback 与 exit hook；collect 对应 collector.c 的周期工作，由 runtime Process 调用，不能持锁执行未知 callback。 |

依据为 `third_party/vpp/src/vlib/stats/stats.h:123` 起的领域操作，以及 init.c /
collector.c 的生命周期函数。接口收缩发生在操作的归属与重复层，不将同一批
方法改名后搬到另一个公开 facade。StatsMain 不提供 add/set/validate 转发；六种
指标 descriptor 不提供方法；客户端只保留既有 connect/list/read。

创建、注册的分配准备在未发布的 owner 上完成时，可以直接通过 &mut StatsSegment
进行；对已发布 segment 的在线扩容，上面签名本身不足以证明分配不发生在外层锁
内。实现前必须列出“准备、复核、提交”的真实调用点；不能把锁内扩容先做出来再
留下 TODO。本文没有批准另一个准备状态类型或 closure transaction 框架。

StatsMain::init 只创建 segment 自身资源（mapping、heap、共享头、固定槽位前
状态）并设置已有 STATS_MAIN，没有公共 create/publish，也不以
install/commit/finish 等名称另造同一阶段。StatsSegment::create 仍是 stats
内部的资源构造函数。

listener 的 bind、FileMain 注册、accept、fd 交付和退出 unlink 都在 runtime
`config::stats`：对应 VPP 把 socket 原语放在 vppinfra、把
`stats_segment_socket_init` / `stats_socket_accept_ready` /
`stats_segment_socket_exit` 放在 vlib 的分层。Hammer 的 `hammer-stats` 位于
runtime 之下且不能依赖 FileMain，因此这三个函数归 runtime statseg，而不是在
stats crate 里复制一套 socket 层。listener 在 Linux 上是 `SOCK_SEQPACKET`
（VPP `CLIB_SOCKET_F_SEQPACKET`；无 seqpacket 的平台上退化为 `SOCK_STREAM`），
因此以 `OwnedFd` 直接注册 File，不经过只支持 stream 的 std listener 类型。
`stats_segment_listener` File callback 只做
accept→sendmsg(memfd)→close；`handoff_segment` 先取得 segment 的稳定
`segment_fd()` 再释放结构锁做 I/O，fd 生命周期由 process-lifetime owner 保证，
不缓存跨生命周期的裸 fd。不存在 listener_index 字段/注册表，也不存在 stats
crate 内的 socket helper。

| 原方法/函数 | 删除、修改或新增 |
| --- | --- |
| `StatsMain::init` | 保留初始化入口，去掉 socket 参数与 OwnedFd 返回值；补齐配置参数与函数内部失败清理；删除草案新增的公共 create/publish 接口。 |
| `StatsMain::{bind_index,store_timestamp,increment_timestamp,store_gauge,validate_counter,write_simple_counter,write_combined_counter,write_histogram,add_gauge,add_timestamp,add_simple_counter,add_combined_counter,add_name_vector,add_histogram,add_ring}` | 删除逐层转发；具体目录操作直接在 StatsSegment。 |
| 草案 `StatsMain::segment` | 撤销；直接 StatsMain.segment.lock()，锁本身约束借用，不增加转发 getter。 |
| `StatsSegment::{heap,directory,directory_mut}` | 新增私有借用方法，生命周期来自 self；替代各处直接解引用缓存指针，不提供整个共享头的可变引用。 |
| `StatsSegment::{create,find,validate}` | 按以上接收者修改；不再访问 self.state；create 仅为 stats 内部资源构造。 |
| `StatsSegment::directory_vector_len` | 删除独立接口；内部需要长度时从真实目录切片取得。 |
| 旧 `StatsSegment::{add_simple_counter,add_combined_counter,add_histogram}` 增量签名 | 删除；新同名方法只负责注册，不负责每包 increment。 |
| `StatsSegment::register<K>` | 删除，连同 RecordKind/layout 泛化层。 |
| `StatsSegment::remove` | 替换 remove_entry；维护 name/free-list、payload 和依赖 alias，不增加代际版本。 |
| `StatsSegment::{send_to,teardown}` | 删除；fd 发送归 runtime `stats_segment_listener` File callback，取消 Arc quiescence 假证明。 |
| 草案 `bind_listener` / `accept_connection` / `send_segment_fd` / `unlink_socket_path`（在 hammer-stats） | 删除；这些是 vppinfra 层职责，Hammer 对应实现只保留 runtime `config::stats` 的 `bind_listener` / `handoff_segment` / `send_segment_fd`，退出 unlink 直接内联在 `exit_stats_main`。 |
| `Drop for StatsSegment` 的 let _ = teardown | 删除旧路径；未发布候选按 heap→VM→fd 回滚，错误由生命周期边界处理。 |
| `StatsSegmentState::{allocate_vector,vector_len,vector_element}` | 在 StatsSegment 内重写真实协议操作，原 impl 整体删除。 |
| `StatsSegmentState::{mapping_size,allocation_address,allocate_block,allocate_directory,publish,write_shared_header}` | 删除旧 SegmentAllocation/private Vec 整块复制路径；使用 MemHeap、真实共享目录和字段级发布。 |
| `directory_entries_for_write/counter_cell` | 删除私有/共享 entry 双写和每包 lookup 路径。 |
| `directory_layout/vector_data_offset/vector_log2_alignment` | 按 D7.3 修正不同 family 的前缀与 alignment；仍是私有布局计算，不移入 runtime。 |
| `ring_layout` 的 cache_line_bytes 参数 | 删除；复用 infra CACHE_LINE 与 RingMetadata 实际 stride，调用者同步修改，不新增方法。 |

### 6.4 指标字段、目录身份和 scalar

```rust
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DirectoryIndex(u32);

pub struct Gauge { pub index: DirectoryIndex }
pub struct Timestamp { pub index: DirectoryIndex }
pub struct SimpleCounter { pub index: DirectoryIndex }
pub struct CombinedCounter { pub index: DirectoryIndex }
pub struct Histogram { pub index: DirectoryIndex }
pub struct NameVector { pub index: DirectoryIndex }
```

这六种已有 descriptor 仅保留宏识别的指标类别与索引记录，不新增 impl。
index 不是权限凭据，也不靠 descriptor 外壳证明槽位类型或存活；segment 的控制面
操作在实际访问处检查 index/family。宏先按 bootstrap 声明创建固定槽位并校验其
固定索引，再对固定 Sys 项使用一次 find 构造记录；普通注册直接接收 add_* 的
返回值，避免 new → register → bind → store 转发链。

DirectoryIndex 仍是 VPP-style 目录 index，不是带代际版本的 pool handle。
删除/重用时由控制 owner 结束旧 descriptor、alias、collector 的使用；外部 client
用共享 epoch 检查一次查询的目录版本，不把旧 index 跨目录版本缓存成永久身份。
若要检测任意调用者长期保存的失效 index，必须另提需求，不能在本 ADR 暗加版本表。

| 字段/接口 | 最终动作 |
| --- | --- |
| metric.name: String | 删除；名字属于注册输入、共享 entry 与 name lookup。 |
| metric.index: Option<DirectoryIndex> | 改为必有 index；不再构造未绑定 descriptor。 |
| NameVector.length | 删除持久长度，长度来自共享 vector。 |
| NameVector.index | 新增，以便 set/free 对应真实目录项。 |
| `new`（上述六种 descriptor） | 删除，StatsSegment::add_* 直接返回已注册指标。 |
| 各 descriptor 的 `bind` | 删除六份同构方法；固定系统项由已有宏调用一次 segment.find(name, expected) 后直接构造对应索引记录，普通注册直接使用 add_* 的返回值。 |
| 草案六份 `index()` | 全部撤销；descriptor 的 index 是普通身份字段，可直接读取，不另加 accessor。 |
| Gauge::store | 删除；调用 segment.set_gauge(gauge.index, value)，设置操作只保留一层。 |
| Timestamp::store/increment | 删除；设置时间用 segment.set_timestamp；heartbeat 在既有 stats collector 执行中直接更新固定槽，不为它保留通用 increment 转发。 |
| 草案 NameVector::set | 撤销；只保留 segment.set_name(name_vector.index, element, value)。 |
| SimpleCounter/CombinedCounter/Histogram 的 validate/add | 删除原 StatsMain 参数转发；validate 属于结构 owner，增量属于 worker 的真实计数存储。 |
| 每种 descriptor 单独 free | 不新增；结束使用后由真实 owner 调 remove_entry。 |
| ScalarBits/GaugeValue/指针 wrapper 的转换 | 随类型删除；数值与 pointer arm 由协议类型检查后的直接操作取代。 |
| SharedHeader 字段和 symlink 两个 u32 | 保留共享 ABI；不插入代际版本、Rust handle 或 allocator metadata。 |

### 6.5 worker 行不增加第二套访问 API

撤销草案的 simple_row_mut、combined_row_mut、histogram_row_mut 三个公共方法。
它们没有对应的实际 worker 安装调用链，只增加了一套必须持 stats 锁的行访问，
不能完成 VPP counter main 的当前线程直接更新。

现有仓库只有 Sys 在使用 descriptor，没有可据以复用的 SimpleCounter::add
业务调用链。M2 必须先确定真实 worker owner 的存储字段、每次借用结束点和
扩容/释放安装点，再决定缺少什么入口；不先增加任意 row/thread_index getter。
正常增量只借用 worker 已拥有的当前行，不查全局目录、不取 stats segment lock。
不能把控制面 guard 下的切片延长为 static 后安装到 worker。

### 6.6 collector、配置、宏与 runtime 接入

```rust
pub struct CollectorRegistration {
    pub collect: fn(u32, u32, u64) -> StatsResult<()>,
    pub entry_index: u32,
    pub vector_index: u32,
    pub private_data: u64,
}

pub struct Collector {
    collect: fn(u32, u32, u64) -> StatsResult<()>,
    entry_index: u32,
    vector_index: u32,
    private_data: u64,
}

impl StatsSegment {
    pub fn register_collector(&mut self, registration: CollectorRegistration)
        -> StatsResult<()>;
}

// hammer-runtime 中的既有声明，字段与签名保留。
pub struct StatsRegistration {
    pub name: &'static str,
    pub register: fn(&StatsMain) -> RuntimeResult<()>,
}
```

collect 函数三个参数按顺序是 entry_index/vector_index/private_data。对应 VPP
注册和 installed collector 的事实；private_data 保留数值 cookie，例如 memory
provider 的 heap index，不允许把插件对象指针/trait object/引用擦成 u64。
VPP collector_data.entry 是调用期便利指针；Rust 不另造指针 View，owner callback
按身份执行明确采样操作。CollectorRegistration 不等于启动宏 StatsRegistration。

StatsMain::collect 从 pool 取得本轮 collector 声明值后释放结构锁，再调用函数；
未知 callback 不能在 SpinLockGuard 内执行。采样值由具体 owner 写回，目录变化
需重新验证目标。正常注册阶段在 Process 启动前；在线删除必须先停止对应 collector
再释放目标 entry，不能让下一轮 callback 指向复用槽。没有第二套 collector registry。

| runtime/macros 项 | 具体增删 |
| --- | --- |
| StatsConfig | 字段、默认值、校验与配置入口的具体定义见 6.6.1；沿用现有类型，不新增 StatsegConfig。 |
| `STATS_SEGMENT_SIZE` | 删除固定使用点，改配置默认值。 |
| Sys | 三个指标字段保留，并作为固定槽位的唯一声明：字段的 `bootstrap = STAT_COUNTER_*` 给出 VPP 固定索引，宏生成创建与校验步骤；`install` 随后 find 绑定，不让 generic DuplicateName 吞冲突。 |
| StatsRegistration.register | 保留既有 fn(&StatsMain) -> RuntimeResult<()>；由 owner 安装入口执行具体目录操作；不增加 bind 字段。 |
| 宏 `expand_stats_descriptor` | 删除 descriptor::new 的未绑定聚合；注册直接使用已有静态 path。 |
| 宏 register/bind/install | 删除宏生成的独立 register/bind 方法；既有 install 一次完成整组安装，直接调用 add_*；带 `bootstrap` 的聚合另生成 bootstrap 创建步骤，Sys 固定项随后通过 find 构造索引记录。成功后才设置指标 owner OnceLock，失败回滚本组；遵守 6.3 的准备/短锁提交约束。 |
| `run_stats_registrations` | 保留现有无参数 RuntimeResult 入口及已有 image inventory；调用既有 registration.register，失败交 runtime 启动边界。 |
| `init_stats_main` | StatsMain::init → Sys::bootstrap 建立固定槽位 → `bind_listener` + FileMain 安装 `stats_segment_listener` → run_stats_registrations；删除 global 已存在就成功跳过的逻辑；任一步失败终止启动，不新增 bind_stats_registrations。 |
| collector Process | 周期从 StatsSegment 读取 interval/node flag，释放锁后采样，完成后更新 heartbeat；锁不跨 await。 |
| `exit_stats_main` | 新增 main-loop-exit hook，unlink StatsConfig.socket_name；失败只告警，退出清理不替换启动/运行错误，路径由下次启动的 bind 回收，无 listener_index 字段。 |
| NodeRuntimeSlot/NodeMain | 后续增加真实 calls/vectors/clocks/suspends 累计事实与 clear baseline；不把这组计数存入 StatsSegment 私有副本。 |

当前 Process future 不持有可恢复的 &mut DataPlaneMain，周期进入 WorkerBarrier 的
runtime 采样入口仍是 M4 必须证明的集成点；不通过 OS ThreadId、current_main TLS
或通用 worker closure 队列绕过。宏与 collector 签名具体化不表示这项已经实现。

#### 6.6.1 statseg 配置入口、字段和生效链

**已核实：**VPP 是 `VLIB_EARLY_CONFIG_FUNCTION(statseg_config, "statseg")`，
不是名为 stats-segment 的配置段；解析直接设置 segment 的配置字段。当前 Hammer
在 `crates/hammer-runtime/src/config/stats.rs` 注册 `[stats]`，只有 socket_path
与 update_interval。原草案只增加 Rust 字段，遗漏了配置解析、默认值和消费端迁移。

**拟议输入：**沿用 TOML 与项目的 snake_case 键名，配置段改为 `[statseg]`。
以下为实施后的配置示例，当前实现尚不支持这些新键：

```toml
[statseg]
socket_name = "/tmp/hammer.stats.sock"
size = "32 MiB"
page_size = "default"
per_node_counters = false
update_interval = "10s"
```

**明确删除旧配置：**最终只有 `[statseg]` 这一套输入。删除 `[stats]` 的配置
注册、`StatsConfig.socket_path` 字段、`deserialize_socket_path` 和旧配置示例；
全部生成启动 TOML 的调用点一次性替换。没有兼容入口、serde alias、双读或旧键
回退，也不设置弃用过渡期。复用 StatsConfig 这个 Rust 类型名不代表保留旧配置
结构；它的字段与解析契约完全以上面的新配置为准。

| VPP 输入 | Hammer 输入 / 默认 | 写入或调用位置 |
| --- | --- | --- |
| socket-name | socket_name，非空路径，继续必填 | runtime StatsConfig.socket_name；socket bind、listener File 描述与退出 unlink 共用此值。 |
| size | size = "32 MiB" | 从 Byte 检查转换为 usize，按实际 backing 页大小检查向上对齐；传给 backing resize / MemMain::vm_map，结果写 memory_size；heap 范围扣除首系统页。 |
| page-size | page_size = "default" | 复用 hammer_infra::mem::PageSize，传 MemMain::vm_create_backing / vm_map；实际 backing 页指数写 log2_page_sz，不直接写入配置枚举。 |
| per-node-counters on/off | per_node_counters = false | 写 StatsSegment.node_counters_enabled；控制 node collector 的目录注册和周期采样，关闭时仍保留基础系统与 heap collector。 |
| update-interval | update_interval = "10s" | 复用 Duration + humantime_serde，写 StatsSegment.update_interval；collector 每轮从 segment 取得周期后释放 guard，再 await。 |
| 已弃用 default 空操作 | 不提供对应配置键 | VPP 源码明确说明其无效果，Hammer 不增加无效果开关。 |

PageSize 已支持 `"default"`、`"default-hugepage"` 和明确字节尺寸字符串，如
`"2 MiB"`。默认大页选择由 MemMain 的既有配置决定；请求的大页不可用则报错，
stats 不自行回退普通页或 Main Heap。保留共享头的首**系统页**，不能将 page_size
直接当作 header 长度。VPP 的 size=0 在初始化中选默认容量；Hammer 以省略 size
选默认值，显式 0 拒绝，属于输入校验差异。

**socket 默认值的实际差异：**VPP 在 statseg_init 使用 runtime directory。
当前 Hammer 没有可复用的 runtime-directory owner，本方案继续要求显式路径，
不硬编码 /run、不借用 Binary API prefix、不声称省略整个配置段与 VPP 等价。
示例的 /tmp 路径是用户输入示例，不是新增默认值。

既有 `hammer_runtime::config::stats::StatsConfig` 修改为：

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatsConfig {
    pub socket_name: PathBuf,
    #[serde(default = "default_segment_size")]
    pub size: byte_unit::Byte,
    #[serde(default = "default_page_size")]
    pub page_size: hammer_infra::mem::PageSize,
    #[serde(default)]
    pub per_node_counters: bool,
    #[serde(default = "default_update_interval", with = "humantime_serde")]
    pub update_interval: Duration,
}

#[hammer_component_macros::config_function(
    name = "runtime_stats_config",
    section = "statseg",
    early = true,
    required = true,
)]
fn configure_stats(config: StatsConfig) -> RuntimeResult<()>;
```

这里复用原 configure_stats 函数、注册名和 StatsConfig 类型，仅更改配置段与输入
内容。私有 default_segment_size 返回 Byte::from_u64(32 << 20)，default_page_size
返回 PageSize::Default；default_update_interval 与 DEFAULT_UPDATE_INTERVAL 原样
复用。runtime Cargo.toml 增加已有 workspace 的 byte-unit 依赖，不造容量解析器。

| 当前字段 / 方法 / 配置使用点 | 最终动作 |
| --- | --- |
| StatsConfig.socket_path | 改为 socket_name；同步 init 输入与路径错误事实；StatsClient::connect 的路径参数无需更名。 |
| StatsConfig.size/page_size/per_node_counters | 新增；不把 Config 的 size: Byte 与运行期 memory_size: usize 混成同一个含义。 |
| 旧草案 node_counters | 撤销该拼写，输入统一为 per_node_counters，运行期字段仍为 node_counters_enabled。 |
| deserialize_socket_path | 删除旧字段专属反序列化函数；configure_stats 检查非空 socket_name，与其他配置校验归到同一入口。 |
| configure_stats / STATS_CONFIG / stats_config() | 保留既有启动输入暂存；回调校验输入，init 读取一次；collector 删除 stats_config() 调用，运行决策只读 StatsSegment。 |
| STATS_SEGMENT_SIZE | 删除固定容量调用点和常量；默认容量由上述配置默认值提供。 |
| init_stats_main | 从 StatsConfig 取得五项输入，经既有 StatsMain::init 参数写入 segment；不得继续传固定 32 MiB 或丢弃 page/node 输入。 |
| [stats] / socket_path | **删除旧段注册与旧键支持**；全部配置使用点替换为 [statseg] / socket_name，无兼容过渡。 |

configure_stats 检查空路径、零容量与零采样周期；字节数转 usize、向上对齐溢出、
首系统页与 heap 所需空间、页支持性在 StatsMain::init / MemMain 的对应边界验证。
输入错误保留现有 runtime 配置错误边界；backing/映射/heap 失败按 A7 保留具体
StatsError/MemError 类别及 source。错误不返回默认容量或默认周期掩盖输入问题。
这些校验不新增公共 validate/resolve/build 方法，也不增加配置发布阶段。

删除旧配置必须同步替换已有示例和生成 TOML 的调用点：
`crates/hammer/examples/plugin_additive_load.rs`、
`crates/hammer-ipc/src/binary_api/memory_client/tests.rs`、
`crates/hammer-ipc/tests/control_ping.rs`、
`crates/hammer-service/tests/show_version.rs`。这是后续实施清单，本次不修改这些文件。

配置验收包括：仅 socket_name 时获得其余默认值；五项非默认值传到真实 owner；
缺少必填段/路径、未知字段、旧字段、零 size/interval、字节数/对齐溢出被拒绝；
实际小容量映射与 heap inventory 相符；大页不可用不回退；node off/on 的注册与
采样行为不同；collector 使用 segment 中的周期。这些均为拟议行为测试，不以
源文件字符串匹配替代配置解析或真实初始化。

### 6.7 MemMain/MemHeap 与 StatsClient 的具体 surface

`MetricValue` 是已经存在于 `crates/hammer-stats/src/metric.rs:240` 的客户端
读取结果类型，`crates/hammer-ipc/src/stats_client.rs:250` 的 read 已经返回它。
它不是新增类型，更不是 StatsMain、StatsSegment 或共享目录的字段；其中 Vec
是客户端复制读取结果所拥有的普通集合，不是服务端 stats heap 的存储设计。
本 ADR 从服务端类型定义中移除该枚举的重复展示，保留现有 read 返回契约，仅将
Scalar(ScalarBits) / Gauge(ProtocolGauge) 的内容改为 u64，修正数值含义。
Simple/Combined/Names/Histogram/Ring 不新增变体或字段，Ring 限制仍按 H11。

```rust
impl MemMain {
    pub fn vm_create_backing(page_size: PageSize, name: &str)
        -> Result<OwnedFd, MemError>;
    pub fn vm_map(
        base: Option<NonZeroUsize>, size: usize, page_size: PageSize,
        backing: Option<BorrowedFd<'_>>, backing_offset: u64,
        alignment: usize, read_only: bool, name: &str,
    ) -> Result<NonNull<u8>, MemError>;
    pub unsafe fn vm_unmap(base: NonNull<u8>) -> Result<(), MemError>;
}

impl MemHeap {
    pub unsafe fn create_at(base: NonNull<u8>, size: usize, locked: bool, name: &str)
        -> Result<NonNull<Self>, MemError>;
    pub unsafe fn destroy(&self);
}

pub struct StatsClient {
    mapping: NonNull<u8>,
    mapping_size: usize,
    segment_fd: OwnedFd,
}

impl StatsClient {
    pub fn connect(socket_path: impl AsRef<Path>) -> StatsResult<Self>;
    pub fn list(&self) -> StatsResult<Vec<String>>;
    pub fn read(&self, name: &str) -> StatsResult<MetricValue>;
}
```

| 项 | 动作 / safety 契约 |
| --- | --- |
| MemMain | 不加 stats 字段；现有 VM list 是唯一登记；只改变已有方法的必要可见性，vm_map 增加 read_only 参数。 |
| MemMain::vm_map 当前 Main Heap caller | 显式传 `read_only = false`；stats server 同样 false，StatsClient 为 true；不增加第二个 read-only mapping wrapper。 |
| MemHeap | 不加 stats 字段；create_at/destroy 只扩大窄 API 边界，保持 unsafe 前置条件；destroy 仍仅允许未发布且无 live allocation 的回滚。 |
| MemHeap::{activate,allocate,allocate_zeroed,reallocate,deallocate,is_heap_object,base,size,name} | 直接复用，不增加同义 StatsHeap 方法。 |
| MemHeap::usage / HeapUsage | 随 M4 新增一次完整采样操作与拥有的事实值；现有 size/free_space 不能给出七项同次采样，不向 stats 暴露 DlMallInfo 或 mspace。本次 collector 仍是 heartbeat-only，先不落地没有消费者的采样 API。 |
| StatsClient.mapping | 删 Mmap，改为 MemMain 返回的实际映射根；StatsClient 存活即拥有映射，不加 open/closed 状态。 |
| StatsClient.mapping_size / segment_fd | 新增长度与 fd owner；这里不保存 heap、shared header 副本、目录 pointer cache 或 per-metric pointer。 |
| StatsClient::connect | 删除 MmapOptions::map 和成功 attach 后立即 drop(segment_fd)；调用 MemMain 的 read-only VM map。 |
| 草案 StatsClient::close | 撤销；没有调用者需要保留一个关闭后的 client，不增加相应状态或错误。 |
| Drop for StatsClient | 接替原 Mmap 的释放职责，借用结束后经 MemMain::vm_unmap 释放，再关闭 OwnedFd；unmap 无法完成时保留诊断并按致命内存边界终止，不能 let _ = 丢弃。 |
| StatsClient::{list,read} | 保留签名，按 D2 重写 epoch 和 bounds 流程；Ring 仍不声明有效记录快照。 |

VM 操作使用既有类型和一个表示保护属性的 bool，不添加 Vmap/StatsMapping/StatsHeap
owner wrapper。MemMain.vm_map 产生的 pointer 归 caller 持有，caller 必须在没有
live borrow 时交回同一个 vm_unmap；不能用于解除其他来源的映射。

HeapUsage 对应 V17，随 M4 的 heap usage collector 一起新增；字段按事实命名，
free_chunks 不加 bytes 前缀；它是拥有数值的采样结果，没有 heap pointer 或借用
lifetime。固定 mspace 可能使部分字段为零，由真实 mallinfo 返回决定，不由 stats
伪造。此新增类型列入 A1 审批。

`vm_unmap` 的返回契约也必须补齐：可恢复 Err 意味原 mapping 仍可被 owner 管理，
成功意味着 payload 和 VM inventory 已撤销。当前实现有 payload 已撤销后解除
header 页失败的路径，不能把这种状态作为普通 Err 交给 StatsClient 再次 unmap。
infra 必须在该不可回滚阶段完成释放或按已有致命内存边界终止；不能让 client
增加一个猜测“半 unmap”的状态字段。client 的释放依赖这个 infra 契约，不额外
提供关闭后重试的生命周期 API。

### 6.8 StatsError 的删除、修改、新增

错误仍由现有 StatsError/协议 Error/MemError 承担，不加 result alias 或新统一
error 框架。`protocol::Error` 改为公开 `ProtocolError`（更名并 re-export 原类型，
不复制枚举），让 source 链有可命名的具体原因。

| 现有 variant / conversion | 动作与拟议字段 | owner / 恢复者 |
| --- | --- | --- |
| `Protocol` | 改 `Protocol { source: ProtocolError }`；From 保留 source，不丢弃 | stats codec；client 拒绝稳定 epoch 下的畸形输入。 |
| `Allocation(SegmentAllocationError)` | 删除；新增 `HeapExhausted { requested: usize, alignment: usize, capacity: usize }` | stats 具体分配；控制 caller 回滚/降低容量需求。 |
| `Io(io::Error)` | 删除万能 I/O variant；StatsError 只保留 stats 自己拥有的失败：`BackingResize { size, source }` 与 `Memory { source }`。listener bind/accept/handoff 失败由 runtime owner 承担：`RuntimeError::StatsListenerBind { path, source }` 与既有 `FileAccept { source }`；peer 断开按 io kind 处理 | stats 初始化 owner 与 runtime statseg owner；终止启动或本次交付。 |
| VM/heap 初始化错误 | 合并为 `Memory { source: MemError }`；`MemError` 已区分 backing/map/heap 阶段，调用者恢复动作相同（终止启动） | stats lifecycle；保留 infra 原因与 source 链。 |
| `ClientMapping { source: io::Error }` | 改 source 为 MemError；撤销草案 ClientUnmap/ClientClosed | client connect caller 处理映射失败；Drop 释放错误直接进入已有内存生命周期诊断。 |
| `DuplicateName` | 改 `DuplicateName { name: String }` | registry；注册 caller 解决两个 owner 的命名冲突。 |
| `MetricTypeMismatch { expected: &'static str, actual: &'static str }` | 字段改为 DirectoryType，显示文本最后生成 | stats；bind caller 拒绝错误 family。 |
| `MetricUnbound` | 删除；最终 metric 类型不能表示未绑定值。 |
| `Teardown/WorkerNotQuiescent` | 删除；不再用 Arc/teardown API 模拟 worker 生命周期。 |
| `PublicationFailed` | 删除 catch-all；可恢复输入问题交具体 ProtocolError/容量错误，owner 内不可能状态在原地断言 | 原 owner；不能把损坏的 storage 作为可恢复状态继续运行。 |
| client 缺失/类型/重试错误 | 现有 MetricNotFound/ClientRetryExhausted 等保留；只有稳定 epoch 才返回确定错误 | client caller 重试或修正查询。 |
| scalar/vector/ring layout 容量错误 | CapacityTooSmall/InvalidLayout/CollectionCapacity/InvalidShape/InvalidRingSchema 保留并检查是否能携带所需事实 | stats；初始化/扩容 caller 处理，不在每包增量产生。 |
| `InvalidSocketPath` | 删除；非空 socket_name 由配置 owner 校验并返回配置错误，stats 不保留重复的路径校验 variant | stats 配置入口；缺失/空路径在启动配置阶段拒绝。 |
| `InvalidLayout` / `CollectionCapacity` | 删除；旧 layout/collection 容量路径已不存在，实际边界只剩 `CapacityTooSmall` 与 `HeapExhausted` | stats；不在每包增量产生。 |

ProtocolError 新增 symlink 目标越界/循环的具体类别，保留原始 index/深度事实；
`WireValueOverflow` 改为 `EncodedValueOverflow`，没有兼容 alias。两种 Error 都
实现真实 source 链。清理错误不嵌成无限递归万能 variant；启动回滚保留 primary
error，并通过启动生命周期诊断呈现 cleanup error，释放无法完成时按上述内存边界处理。

### 6.9 删除验收与本次文档边界

必须一起删除的生产定义/依赖：`StatsSegmentState`、其整个 impl/import、
`StatsSegment.state`、`Arc<SpinLock<...>>`、Clone、StatsSegment 内的
Segment/SegmentAllocation、`RecordKind`、全部 layout 中间类型、私有目录 Vec、
payload owner 列表，以及第 6.3/6.4 节列出的旧转发方法和协议 wrapper。
宏、runtime stats listener callback、runtime init/collector、IPC client 不能残留旧参数类型。
旧 `[stats]` 注册、socket_path 配置字段及其反序列化函数、旧启动配置示例也必须
一起删除；有效配置示例只使用 `[statseg]`。历史源码差异说明和拒绝旧输入的测试
可以引用旧拼写，但不能保留可运行的旧入口或兼容分支。

不允许 `type StatsSegmentState = StatsSegment`，不允许 `StatsStorageState` 等
改名替代，不允许只把原 Arc 挪到 StatsMain。stats/core/macros 依赖是否还能删要
根据实际剩余调用者审查；不为这份 ADR 自动删除全仓仍被使用的 infra Segment。

文档验收是每个现有字段和方法有“保留/修改/删除/移入真实 owner”去向。
实现验收还包括编译、真实行为及删除搜索；对未决 A2/M4，本文明确列出缺少的
worker/Process 所有权证明；撤销三套 row getter 不代表已经解决 worker 存储安装，
不能把上面的 candidate Rust signature 当成已完成
热路径或完整 collector。此轮只补 ADR，不执行上述代码删除或测试。

## 7. 迁移闭包与执行顺序

| 阶段 | 修改/复用范围 | 必须一起删除或修正的旧 surface | 完成证据 |
| --- | --- | --- | --- |
| M0：MemMain VM + MemHeap，首个实现闭包 | infra mem 现有 VM/heap 入口及其当前调用者；stats segment/metric/allocation/error；IPC StatsClient mapping/关闭；statseg size/page_size 与配置调用点迁移、D7 的共享前缀/packed/mark/stride、相关 manifest；对应 A1/A3/A7/A8 | stats 的 Segment/SegmentAllocation/Talc、绕过 MemMain 的 backing/map/unmap、client memmap2 路径；若 memmap2 还有其他用户则仅删本调用点 | daemon/client 各自 VM inventory 正确，服务端 heap inventory 正确，header 页分离，固定容量、只读 attach、失败回滚、fd 生命周期。 |
| M1：读取与数值契约 | stats protocol/segment/metric、IPC stats_client、对应 source-chain 错误；对应 A3/A4/A7 | Gauge bitcast、client 错误前缀起点、整块 volatile 被视作同步的说明、最终校验前的不当返回 | Gauge 1/0/大整数；非空二维向量；畸形映射；变化 epoch 的成功/错误重试。 |
| M2：worker 计数与发布所有权 | stats segment/StatsMain、runtime 控制入口与 worker 行借用；对应 A2/A3 | 热路径 Arc/SpinLock、任意 worker 行写入、Arc quiescence 断言 | 无锁更新证据、扩容/删除/barrier、耗尽失败原子性、客户端同时读取。 |
| M3：目录和初始化闭环 | NameVector/Symlink、宏 Stats 安装、runtime registration/config/File/exit、IPC 解析；对应 A6/A7 | 仅类型匹配就吞 DuplicateName、部分初始化后成功跳过、未接入 unlink、固定索引仅有常量 | 重名 owner、固定系统槽位、失败回滚、删除重用、listener 退出。 |
| M4：真实 collector | runtime node/error 事实与 clear baseline、stats collector declaration、infra usage、statseg per_node_counters/update_interval 的消费；对应 A5/A8 | heartbeat-only 被称为完整 collector、CLI 第二套基线等替代设计 | 两 worker 不同计数、按 node/name 查列、clear 一次、heap 数值来自 owner。 |
| M5：能力收口 | 核对 M0/M4 已接入的 statseg 配置、示例与 CONTEXT 术语；A8 的最终验收，不延后配置实现 | 未验证的“VPP 完全兼容”表述；Ring 完整快照暗示 | 各能力支持矩阵与配置验证，Ring 未完成项保持明确。 |

所有阶段都覆盖定义、exports、构造/转换、宏展开、registration image、调用者、
tests、manifest 与文档。不增加对旧 API 的转发 wrapper，不以改名保留旧 ownership。
每阶段须形成一个完整候选改动后再验证，按 infra → stats → runtime → IPC/owner
的依赖关系组织提交；不把“只修一个调用点能编译”视作闭包完成。

## 8. 验证矩阵

本节是后续实施的验收计划，**本次未执行**；测试名称描述场景，不承诺已存在文件。

| 场景 | 验证方法 | 验收条件 |
| --- | --- | --- |
| 共享布局 | 按 D7 执行 C/Rust 布局 fixture 与真实映射检查 | packed/mark、所有偏移、完整 vector 前缀、两线程 metadata stride 与 schema 位置相符。 |
| MemMain VM/heap 归属 | 创建实际 stats mapping，观察现有 `MemMain::mapping_count/heap_count`，并核对目标地址/页大小的 VM facts | map 与 heap 各登记一次；VM 私有页不在 fd 内；stats header 在 fd offset zero；不能仅断言 mmap 返回非空。 |
| 内存回滚 | backing 建立、VM map、heap 创建、目录分配、发布前安装各阶段故障注入 | 未发布资源释放后 map/heap inventory 回到基线，fd 无泄漏；清理错误可观察。 |
| stats heap 固定容量 | 检查 directory/payload/heap 控制块的地址范围与 `MemHeap::is_heap_object`，耗尽再失败 | 全部属于 stats heap，无 Main Heap fallback、无额外扩容映射。 |
| 客户端 MemMain 只读 attach | 独立进程收到 fd，经 MemMain 只读 map 到不同 VA，然后解除映射 | client map inventory 增/减，无 client heap 新增；服务端 heap 不被重建；payload 写入由 OS 拒绝。 |
| C/Rust 布局与基础互操作 | 使用 vendored shared.h/vec 语义的独立 C client/producer fixture，与实际 Hammer mapping 相互读取 | layout、Gauge 数值、ScalarIndex、simple/combined/name/symlink 均符合；不仅比 size。 |
| 向量 header | 真实创建 64 字节对齐的两行多列数据，StatsClient 读取；另覆盖 8 字节目录前缀 | 所有元素正确，空向量与 null row 按类型契约处理，无假越界。 |
| 发布与回收 | 独立进程 reader 与 main 注册/扩容/删除/同槽重用交错，故意暂停 reader | 获得有效采样或具体有界重试；不越界、不把旧名称解析为新对象。 |
| 畸形输入 | 构造受控 mmap：截短 header、异常长度、溢出、非法 pointer/type、ring 越界、symlink 环 | 在解引用前拒绝，match 具体错误字段/source；不以 panic 或 OOM 代替错误。 |
| worker 所有权 | 两个真实 runtime worker 更新不同 counter 行，运行中扩容/clear | 行隔离；精确断言在 barrier 后执行；错误线程无法安全取得别人的 mutable 行。 |
| 热路径成本 | release/LTO 汇编与 1/2/N worker 基准 | 无锁/分配/目录查找；统计 cache-line 争用与 collector 暂停时间，结果才用于 inline 决定。 |
| statseg 配置 | 按 6.6.1 解析默认/非默认/非法输入，并调用真实初始化及 collector | 输入进入实际 StatsSegment、VM/heap；无静默回退或第二份运行期配置。 |
| 安装失败原子性 | 小容量耗尽、重复名字、File 注册失败、宏组中途失败、清理失败注入 | 主错误与 source 保留；init 内部失败不留下 Main；后续启动失败不运行 worker/collector 或交付 fd，按 D6 清理并退出；组内注册失败不残留部分目录项。 |
| collector 与 baseline | 推进可控 node 事件和 collector tick，再 clear 后继续更新 | calls/vectors 等来自实际 owner；别名投影同值；delta 只扣一次；heartbeat 在完成采样后推进。 |
| fd 与退出 | 独立进程 socket/fd attach、异常 ancillary data、peer 断开、main exit | 只读映射；多余 fd 不泄漏；listener 移除与 socket unlink；旧映射 heartbeat 停止。 |
| 宏/image | 编译真实 Stats 聚合并调用注册 hook，必要时 real dlopen/dlsym | 声明身份、固定槽位、重复安装及 plugin image 生命周期正确，不用源码字符串断言。 |

实施候选完成 review、格式化和提交内容整理后，按实际影响选择：

```bash
cargo fmt --all -- --check
cargo check -p hammer-stats -p hammer-runtime -p hammer-ipc
cargo clippy -p hammer-stats -p hammer-runtime -p hammer-ipc --all-targets
cargo test -p hammer-infra -p hammer-stats -p hammer-component-macros -p hammer-runtime -p hammer-ipc
```

测试仅在最终 pre-commit gate 执行，成功后立即提交；不在实现过程中或提交后
再跑测试。本 ADR 不启动 daemon、不创建 TUN，也不运行本地 lab。C 互操作 fixture
纳入最终测试 gate；ARM64/不同 cache-line 平台由相应 CI 验证后再声明支持。

## 9. 审查结果与未决项

**已核实事实：**VPP stats 通过 mem VM map 登记共享映射，再在映射内建 heap；
当前 Hammer 的 Segment/Talc 路径同时绕过这两个权威，必须首先迁移。VPP 的
worker counter 更新不取全局 stats 锁；当前 Hammer
会取锁。Gauge 的整数/bitcast 差异、client 的 vector 前缀错误、heartbeat-only
collector、缺少 name/symlink 操作、错误 source 丢失、未接退出 hook 都有本地
源码依据。固定布局和 Linux fd/只读 mmap 方向已有对齐，值得保留。

**本次要求与授权：**做本地源码对照并写 ADR，不调用技能；用户明确要求 heap
对齐和共享内存经过 MemMain VM map，已作为 D0/M0 的必需架构方向。未获得实现、
新类型/API、修改既有 rate 策略或启用完整 Ring 的授权。本文保持 proposed。

**尚需验证/决定：**Rust 跨进程 mmap 发布/回收的完整证明；worker 借用与扩容的
具体落点；通用 SpinLock 所在层与 stats/runtime 依赖方向；MemMain/MemHeap 的只读权限实现；node 采样使用短 barrier 的成本；
当前布局与 vendored C 客户端的双向实测；是否需要 VPP rate 路径及默认 socket
策略。未做性能测量，未证明全部平台兼容，也未把源码推导写成已通过的测试结果。

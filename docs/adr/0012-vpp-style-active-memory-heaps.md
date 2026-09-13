# ADR-0012：VPP 风格 active heap、内存 Main 与 SVM pvt/data heap

- 日期：2026-09-12
- 修正日期：2026-09-13
- 状态：Accepted；设计修正已于 2026-09-13 批准，尚未实施生产代码
- 范围：`hammer-infra` allocator、`svm_region` 的 pvt/data heap、运行时线程入口
- VPP 基线：`third_party/vpp`，提交 `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`
- 被替换 allocator 调研：当前 `libmimalloc-sys 0.1.49` 内置 mimalloc `3.3.2`；
  最终设计不再依赖 mimalloc
- 替代：ADR-0011 2026-09-11 版本中已经撤销的 offset region、
  `SvmRegionHeap`、`SvmHashMap`、different-VA region attach 和独立 subregion
  identity；queue、FIFO、multiarch 与 `ssvm` 由修正版 ADR-0011 独立决定

## 1. 决策边界

本文只决定以下问题：

1. Hammer 如何用 `MemMain`、`MemThreadMain`、`MemHeap` 表达 VPP
   `clib_mem_main_t`、`clib_mem_thread_main_t`、`clib_mem_heap_t` 的所有权；
2. Rust `GlobalAlloc` 和已存在的 C malloc-family interposition 如何把普通分配
   路由到当前线程的 active heap；
3. `svm_region` 如何把 VPP 的 pvt heap 和可选 data heap 都实现为 `MemHeap`，
   并通过 active heap 执行 `Vec`、字符串、名字表和用户数据分配；
4. Main Heap、process-private heap 与 shared heap 如何统一使用 dlmalloc mspace；
5. 当前已经落地的 offset region heap 需要删除什么。

本文不把 `SvmFifoSegment`、`SvmMsgQ` 或普通 `ssvm` payload 自动改成
`svm_region`，也不改变 AppClient 通过 Unix socket 和 `SCM_RIGHTS` 接收 fd 的产品
边界。需要 region 的映射必须遵守本文的同 VA 契约；FIFO segment 之类只交换
offset 的 SSVM payload 必须按修正版 ADR-0011 单独证明，不能反推 region 支持
different-VA attach。

命名直接删除 VPP 的 `clib_` 和 C typedef `_t`：

| VPP | Hammer |
| --- | --- |
| `clib_mem_main_t` | `MemMain` |
| `clib_mem_thread_main_t` | `MemThreadMain` |
| `clib_mem_heap_t` | `MemHeap` |

不引入 `AllocatorMain`、`AllocatorThreadMain`、`SvmRegionHeap`、`OffsetHeap` 或
其它同义类型。

## 2. 证据账本

### 2.1 VPP

| ID | 路径与符号 | 已核实行为 | 设计约束 |
| --- | --- | --- | --- |
| V1 | `src/vppinfra/mem.h:70-83`，`clib_mem_thread_main_t` | TLS 状态直接保存 `active_heap`、thread index 和 thread-main 链 | `MemThreadMain` 是 allocator 的线程状态，不是 `DataPlaneMain` 字段 |
| V2 | `src/vppinfra/mem.h:85-119`、`:420-424`，`clib_mem_main_t` / `clib_mem_get_last_error` | process Main 保存 main heap、heap 列表、thread-main 列表、system/default/system-default-huge page size、malloc-family intercept flag、NUMA bitmap、mapping 双向链、map lock和last-error pointer | allocator、page、NUMA、mapping和inventory状态必须移植；C sentinel + mutable error pointer不进入Rust owner，见D1 |
| V3 | `src/vppinfra/mem.c:16-28`，`clib_mem_thread_init` | 新线程默认把 main heap 设为 active heap，再登记 thread index | 未显式切换的线程必须始终从 main heap 分配 |
| V4 | `src/vppinfra/mem.h:184-200`，`clib_mem_get_heap` / `clib_mem_set_heap` | get 在未初始化时初始化线程状态；set 替换 active heap 并返回旧 heap | nested active-heap scope 必须恢复旧 heap |
| V5 | `src/vppinfra/mem_dlmalloc.c:41-100`，`clib_mem_create_heap_internal` | `create_mspace_with_base` 在给定 range 内建 heap；`MemHeap` 等价控制块再从该 mspace 自身分配；heap 列表扩容时临时切回 main heap | pvt/data heap 的控制块在自身映射内，attach 不重建 |
| V6 | `src/vppinfra/mem_dlmalloc.c:363-383`、`:446-473`、`:519-537` | alloc/realloc/free 没有显式 heap 时取 active heap；free 断言对象属于该 heap | Rust/C 普通 allocation family 必须共用 active selector；错误 active heap 是违规，不自动猜 owner |
| V7 | `src/vppinfra/dlmalloc.c:4012-4032`、`:4054-4065` | mspace allocator state 位于 supplied base；locked mspace 的锁也在该 state；range 标为 external | fixed-VA attach 后可直接继续使用 shared mspace state |
| V8 | `src/svm/svm_common.h:19-50` | region header 保存version、mutex/condvar、mutex owner pid/tag、flags、virtual base/size、region/data heap、data base、user context、bitmap size/pointer、region/backing names、filenames和client pids；pvt heap默认128 KiB | Hammer header逐字段保留VPP region fields并使用固定VA raw pointers，不增加lifecycle enum或offset descriptor |
| V9 | `src/svm/svm.c:434-531`，`svm_region_init_mapped_region` | creator 在 `baseva + page_size` 创建 locked `region_heap`，切到它分配名字、pid vec、bitmap；之后按 flags 创建可选 data heap | region 初始化必须先有 pvt heap，所有 region metadata 由它分配 |
| V10 | `src/svm/svm.c:537-610`、`:654-704`，`svm_map_region` | creator 用 `MAP_SHARED|MAP_FIXED`；attach 先探测一页取得 creator 发布的 VA/size，再在相同 VA 重映射完整 region | raw pointers 和 shared heap 依赖所有参与进程映射到同一 VA |
| V11 | `src/svm/svm.c:733-751`，attach branch | attach 持 region lock，切到 creator 的 pvt heap，追加 client pid，并只映射 data backing；不创建 pvt/data heap | attach 复用 creator 已发布的 `MemHeap` 控制块 |
| V12 | `src/svm/svm.c:770-818`，`svm_region_init_internal` | root 映射完成后，在 `NEED_DATA_INIT` 分支切到 pvt heap，分配 `svm_main_region_t`、name hash、root path，再令 `data_base = mp` | root 的 `data_base` 是 pvt heap 中的 main-region 对象，不是 data heap 起点 |
| V13 | `src/svm/svm.c:821-875`，root init callers | global root 使用 `SVM_FLAGS_NODATA`，不是 `SVM_FLAGS_MHEAP` | root 只有 pvt heap；不得为它虚构 data heap |
| V14 | `src/svm/svm.c:879-975`，`svm_region_find_or_create` | root pvt heap 中的 bitmap 选择连续 VA，subregion 映射到 `root_base + page_index * page_size`；名字表值是 subregion pool index | root 继续拥有 VA 切分与名字注册，不使用任意 VA 的独立 region id |
| V15 | `src/svm/svm.c:252-331`、`:335-406` | 只有 `SVM_FLAGS_MHEAP` 创建 data heap；attach 的 map path不重建它 | data heap 是可选 `MemHeap`，与永远存在的 pvt heap不同 |
| V16 | `src/svm/svm.h:20-69`，`svm_mem_alloc` / `svm_push_pvt_heap` / `svm_push_data_heap` | pvt/data helper 都通过 `clib_mem_set_heap` 切换 active heap；data alloc/free 还持有 region mutex | `SvmRegion` 只授予受 region lock 约束的 pvt/data active scope |
| V17 | `src/svm/svmdb.c:70-120`、`:185-214` | DB 初始化把共享 header、Hash 和 Vec 放进 active data heap；后续 mutation 同样先切 data heap | active heap 必须覆盖普通 collection 的隐式分配，不只覆盖手写 raw allocation API |
| V18 | `src/vppinfra/dlmalloc.h:527-535`、`:1330-1469` | VPP 固定启用 `USE_LOCKS=1`、`ONLY_MSPACES=1`，并声明 supplied-base mspace 与 heap 检查、禁止扩展等接口 | Hammer 必须构建同一类 mspace backend，不能改用只提供 process-global malloc 的 Rust allocator |
| V19 | `src/vppinfra/CMakeLists.txt:61-92` | `dlmalloc.c` 和 `mem_dlmalloc.c` 是两个独立编译单元，vppinfra 默认隐藏内部 C 符号 | Hammer 只引入 dlmalloc engine；`MemMain`/`MemHeap` 行为由 Rust 移植，不能再编译一套 `clib_mem_*` authority |
| V20 | `src/vppinfra/mem_intercept.c:28-38`、`:79-99`，`src/vpp/vnet/main.c:132-175`、`:362-375` | intercept开启后free/realloc无条件走active heap；VPP先销毁bootstrap heap，只让独立mmap中的config bytes跨heap切换 | Hammer cutover前必须结束全部System/libc allocation owner；不实现切换后的provenance routing |
| V21 | `src/vppinfra/mem.c:16-28`、`src/vppinfra/mem_dlmalloc.c:145-167` | VPP把TLS对象地址链接到process inventory，destroy时遍历并写零每个记录；源码没有短命thread的unlink | 只有保证存活到process exit的runtime thread可进入Hammer inventory；短命外部thread只初始化active main heap |
| V22 | `src/svm/svm.c:709-751`、`src/vppinfra/dlmalloc.c:407-480`、`:1183`、`:1336-1337` | VPP发现region mutex owner死亡后重建mutex并继续；shared mspace使用不具备owner-death恢复能力的spinlock，进程可能死在allocator mutation中 | Hammer不得验证或继续使用未知mspace/collection；owner death必须终止region生命周期 |
| V23 | `src/svm/svm.c:1034-1119`、`:1215-1228` | member、pool、hash和name删除都在region/root lock及对应active pvt heap内完成；heap-owned值不按值返回给外层销毁 | Rust删除API只返回无heap ownership的事实；所有drop在owner的active PVT scope内完成 |

`src/svm/svm_test.c` 是旧调用签名的 smoke program，没有覆盖当前
`svm_region_init_internal` 的 creator/attach、pvt/data heap 和失败原子性。本文第 10 节的
subprocess 测试主要从上述实现路径推导，不声称 VPP 已提供对应测试。

### 2.2 当前 Hammer

| ID | 路径与符号 | 已核实行为 | 差距 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-infra/src/main_heap.rs` | 一个固定容量 mimalloc arena；`GlobalAlloc` ready 前走 System，ready 后直接 `mi_malloc_aligned`；无 active heap | 普通分配只能进入 process Main Heap |
| H2 | `crates/hammer-infra/src/heap.rs` | crate-private `Heap` 用私有 vtable 在 main heap 与 creator-only `SegmentMapping` 间选择；不参与全局 allocator | 不是 `MemHeap`，attached region 被拒绝，不能提供 VPP active semantics |
| H3 | `crates/hammer-infra/src/svm/region_heap.rs` | 自研 `SvmRegionHeap` 是 offset block allocator | VPP 没有该类型或该 offset heap |
| H4 | `crates/hammer-infra/src/svm/region.rs` | `SvmRegionHeader` 内嵌 `metadata_heap`/`data_heap` 两个 offset descriptor，所有位置为 payload offset，支持不同 VA attach；另有VPP不存在的failure latch | layout与V8-V16相反；重写时删除offset heaps和该latch，不增加替代状态字段 |
| H5 | `crates/hammer-infra/src/svm/hash_map.rs` | `SvmHashMap` 专为 offset heap 实现 | VPP 通过 active pvt/data heap让普通 Hash/Vec 分配，没有对应 SVM hash 类型 |
| H6 | `crates/hammer-infra/src/svm/ssvm.rs` | `SvmRegion` 被放进 `SsvmPrivate` payload；requested VA 为 0 时允许任意地址 attach | VPP `svm_region` 自己映射 region，不是 `ssvm_private_t` payload |
| H7 | `crates/hammer-runtime/src/start_workers.rs:25-55`、`worker_thread.rs:107-185` | thread zero 构造 worker `DataPlaneMain` 后 move 到 worker OS thread | 这不妨碍 allocator-owned TLS；只有把 `MemThreadMain` 错放进 `DataPlaneMain` 才会制造冲突 |
| H8 | 修正前的 `CONTEXT.md` 与 ADR-0011 | 把 offset heap、不同 VA attach、独立 subregion 和 `SvmRegionHeap` 记录成目标语言 | 已从规范删除；当前代码仍须按本文迁移 |

### 2.3 allocator backend 结论

当前 mimalloc arena 能提供固定容量 process heap，但没有让另一个进程在 fixed VA 上
attach 并继续使用同一个 live heap state 的 supported mspace contract。为避免 process
heap 和 SVM heap 各自拥有一套 allocator/TLS/provenance 语义，最终设计删除 mimalloc，
所有 `MemHeap` 统一使用 vendored dlmalloc mspace。

实现从 vendored VPP commit `629fe2764bd997189fedd2d98cbe8dc9189c1ec3` 使用的
public-domain dlmalloc 2.8.6 提取 `third_party/dlmalloc/`。该目录保存来源、许可证、
独立的 `dlmalloc.c` 与 `dlmalloc.h`，只保留 VPP memory path 实际依赖的 mspace engine
和扩展；移除对 `vppinfra/clib.h`、`vppinfra/cache.h`、`os_panic` 及 VPP trace symbol
的编译依赖时，不改变 chunk、mspace、锁或 supplied-base 行为。

`hammer-infra/build.rs` 用 `cc` 直接构建这份 C source，并固定
`ONLY_MSPACES=1`、`MSPACES=1`、`USE_LOCKS=1` 和 hidden visibility。不增加
`dlmalloc-sys` workspace crate或运行时 Rust allocator dependency，也不编译
`third_party/vpp/src/vppinfra/mem_dlmalloc.c`。后者只作为 Rust `MemMain`/`MemHeap`
实现的行为证据。

Main Heap、process-private heap、PVT Heap 与 Data Heap都通过
`create_mspace_with_base(base, size, locked)` 在 owner 已映射的 fixed range 内创建，随后
调用 `mspace_disable_expand`。`MemHeap` 控制块从该 mspace 自身分配，attach 复用原控制块
和 mspace state，不重建。

Main Heap 和所有跨线程/跨进程 heap 使用 locked mspace。仅有完整单线程 owner 生命周期的
process-private heap 才允许 `locked = false`；该选择在创建后不可改变。Cargo workspace、
`hammer-infra`、C interposition 和 plugin image checks 删除全部 `libmimalloc-sys`/`mi_*`
依赖与符号。

## 3. 三方语义比较

| 维度 | 当前 Hammer | 目标 Hammer | VPP |
| --- | --- | --- | --- |
| process authority | `main_heap.rs` 的分散 atomics | `MemMain` 集中拥有 main heap、heap/thread inventory 和现有固定 arena | `clib_mem_main_t` |
| thread state | 无 Hammer active selector | allocator-owned `MemThreadMain` TLS，默认main heap；只有process-lifetime runtime threads进入inventory | `__thread clib_mem_thread_main_t`；所有初始化thread都入链且无unlink |
| ordinary allocation | ready 后固定 mimalloc main | Rust GlobalAlloc 与 C malloc family读取同一 active dlmalloc mspace；cutover前bootstrap allocation归零 | `clib_mem_alloc/free/realloc` 取active heap；intercept后不识别libc provenance |
| memory errors | owner-local `Result` | 每次失败直接返回owned typed `MemError`，`MemMain`不保存第二份error pointer | C函数返回sentinel并在`clib_mem_main.error`保存mutable error |
| process heap backend | mimalloc fixed arena | fixed-range locked dlmalloc mspace | locked dlmalloc mspace |
| SVM heap backend | offset `SvmRegionHeap` | fixed-base locked mspace，公开身份仍是 `MemHeap` | locked mspace |
| region mapping | `SsvmPrivate` payload，可不同 VA | `SvmRegion` 自己拥有 fixed-VA map；attach 同 VA | `svm_map_region` |
| root layout | header 后一个 metadata offset heap | page 0 header，page 1 起 pvt heap，`data_base` 指向 pvt 中 `SvmMainRegion` | `svm_region_init_internal` |
| ordinary shared collections | `SvmHashMap` + offset arrays | 同构建版本的 `Vec`/`HashMap`/`Pool` 在 active pvt/data heap 分配 | clib vec/hash/pool 在 active heap 分配 |
| subregion location | 独立任意 VA + monotonic id | root bitmap 分配 VA；backing fd 仍可经 Hammer rendezvous 交付 | root bitmap + named shm |
| shared pointer contract | mapping-relative offset | region 内 raw pointer；同 arch、同 build、同 VA | raw pointer + same VA |
| region owner death | old offset region latches`Failed` | 不新增shared state；保留dead owner pid/tag并返回typed error，不再触碰pvt/data mspace或collections | 重建region mutex后继续使用所有shared state |
| shared cleanup | offset owner返回ids | owner在root/region lock + active PVT内drop；只返回bool/index等无heap ownership事实 | pool/hash/vec/name都在active region heap内删除 |

## 4. 最终决策

### D1：三个内存 owner 的名字与归属

`hammer-infra::mem` 新增且只新增三个核心 owner 名字：`MemMain`、
`MemThreadMain`、`MemHeap`。`MemMain` 是 process-global authority；
`MemThreadMain` 是 allocator-owned OS-thread state；`MemHeap` 是可被设为 active 的
具体 heap。`MemMain` 本身就是 `#[global_allocator]` static使用的`GlobalAlloc`实现，不再
保留当前零大小`HammerMainHeap`代理。`MemHeap` 不公开trait、手写vtable、`dyn`或backend
enum。

`MainHeapConfig::initialize` 是唯一 public startup initialization seam，并直接消费
`hammer-infra` 拥有的配置。`MemMain` 内部执行一次性初始化；不把旧
`main_heap::{init, init_with, init_default}` 提升为新的 `MemMain` public API。

`MemMain` 的规范字段不是只含 allocator selector。完整移植VPP memory-main中的main heap、
heap inventory、thread-main inventory、system page size、
selected default hugepage size、system default hugepage size、malloc-family interception flag、
available NUMA-node bitmap、mapping 双向链首尾和map lock。
`MemVmMapHeader` 直接对应
`clib_mem_vm_map_hdr_t`，同样移植 base、page count、page size、backing fd、固定容量 name 与
prev/next link。字段的 Rust 拼写见 8.2，不得把这些状态拆回 `physmem` 或 runtime 的第二个
process authority。

VPP的`clib_mem_main_t.error`和`clib_mem_get_last_error`不移植。它们为返回sentinel的C
接口保存第二条mutable error side channel；Hammer对应操作已经直接返回拥有所有权的typed
`MemError`。同时保留pointer和`Result`会造成重复error authority、悬空借用以及并发替换
语义，因此这是明确的Rust typed-Result差异，不是缺失字段。

`MemThreadMain` 保存 `active_heap`、`thread_index`、process-lifetime thread inventory link和
VPP已有的`trace_thread_disable`。`MemHeap` 完整保存 base、mspace、size、page size、
locked/traced/unmap-on-destroy flags和末尾name bytes；没有backend selector。

这些字段按VPP调用路径工作，不只是diagnostics：

1. `MainHeapConfig::initialize` 先以`log2_page_size == 0`作为VPP相同的一次性platform-init
   guard。Linux通过`sysconf`取得
   system page size，通过hugetlb memfd或`/proc/meminfo`取得system default hugepage size，
   把它复制到selected default hugepage size，并通过`get_mempolicy(MPOL_F_MEMS_ALLOWED)`
   填充NUMA bitmap；其它平台实现相同字段语义或返回typed unsupported error。
2. 创建Main Heap时，VM mapper先为目标range和前置header page预留地址，再映射payload；
   映射全部成功后才在`map_lock`下初始化`MemVmMapHeader`并接到`first_map/last_map`双向链。
   unmap在同一把锁下恢复header可写、摘除prev/next、unmap payload，解锁后unmap header page。
3. `PageSize::Default`解析到system page size；`PageSize::DefaultHuge`解析到selected default
   hugepage size。`MainHeapConfig.default_hugepage_size`若存在，在Main Heap发布后覆盖selected
   值，不改写system default值。
4. Main Heap、heap inventory、main-thread `MemThreadMain.active_heap`全部发布后，才把
   `allocation_intercept`设为true。C malloc-family在false时走libc，在true时与Rust
   `GlobalAlloc`共同走current active mspace。
5. NUMA枚举从bitmap中清除不大于previous node的bits，再返回最低set bit；VM mapping和
   affinity选择都只读该字段，不重复探测另一份NUMA state。
6. backing-fd、NUMA、mapping和heap创建失败只通过当前调用的`Result<T, MemError>`返回；
   caller拥有错误及source chain。`MemMain`不缓存、替换或借出上一次错误。

### D2：只有一个 active-heap authority

`MemThreadMain.active_heap` 是 Rust GlobalAlloc、C malloc-family interposition 和
`MemHeap` 普通入口共同读取的唯一 authority。首次读取初始化为
`MemMain.main_heap`。`MemHeap::activate` 返回 `ActiveHeap<'_>`；该值拥有恢复义务，
`Drop` 恢复旧 heap，因此 early return 和 unwind 不泄漏 heap selection。nested scope 按栈
恢复。

底层 replace 操作保持 crate-private；不公开 raw pointer setter，不提供 closure-mediated
`with_heap`。`ActiveHeap` 是 restoration token，不是借用现有状态的 `View` 或 wrapper。
它不可 `Send`/`Sync`，也不能跨 `.await`。

VPP 用 `__thread`，而 `GlobalAlloc` 没有 `DataPlaneMain` 参数。要让标准库和第三方
allocation 自动读取 active heap，allocator 必须有一个 TLS slot。实现使用 allocator C
shim 的 `_Thread_local MemThreadMain`，不新增 Rust `thread_local!`、generic per-thread
container 或跨线程 install/clear API。这是相对仓库“线程状态由 runtime value 持有”规则的
一项窄例外，需要随 A2 明确批准；不批准该例外就无法同时满足 GlobalAlloc 与 active heap，
本文不得以隐藏的 pthread key 或全局锁绕过。

runtime thread 在自己的 OS-thread entry 登记 `thread_index`。只有main、Data Worker以及
其它明确保证存活到process exit的runtime OS thread允许登记并链接到`MemMain.threads`；
登记入口是`unsafe`，调用方必须证明该TLS记录在process lifetime内不会失效。短命auxiliary
thread和任意外部thread只懒初始化本TLS的`active_heap = main_heap`与保留thread index，绝不
链接inventory，也不需要TLS destructor修改共享链。登记只允许一次，inventory只用于这些
process-lifetime记录的诊断，不提供restart或跨线程mutable access。

current-thread selector完全属于allocator内部。它只返回本次调用使用的raw
`*mut MemThreadMain`、`*mut MemHeap`或Copy thread index，不公开`&'static MemThreadMain`，
也不从TLS制造任何安全`&MemHeap`。process Main Heap的`'static`借用由process lifetime
成立；region内pvt/data heap的安全借用只能由`RegionLock`产生并受其lifetime约束。

### D3：普通 Rust/C 分配都走 active heap

`MemMain` 的 `GlobalAlloc::{alloc,alloc_zeroed,dealloc,realloc}` 与当前 interposed
`malloc/calloc/free/realloc/aligned_alloc/posix_memalign/...` 先取得 active `MemHeap`，
再调用同一个 mspace dispatch。Rust 和 C 不再各自维护默认 heap。

ready前的Rust/C bootstrap allocation走System/libc，不增加VPP没有的per-allocation header。
与VPP在切换Main Heap前销毁bootstrap heap、把config bytes暂存在独立`mmap`中的流程一致，Hammer
process entry必须把early parse限定在独立scope：只留下Copy的`MainHeapConfig`和mmap中的原始
startup bytes，所有System/libc bootstrap owner在Main Heap发布前drop。cutover的前置条件是
经`MemMain` bootstrap path产生的live allocation计数为零；实现用test/debug-only observer
验证该条件，不把计数、header或provenance table带入production layout。存在任一live
bootstrap allocation时不得发布Main Heap或开启intercept。

发布后alloc/realloc/free直接走current active mspace；free先以
`mspace_is_heap_object(active.mspace, pointer)`断言pointer属于当前heap，再调用`mspace_free`。
不得扫描heap inventory猜owner，不得回落System/libc。dlmalloc chunk metadata是唯一allocation
metadata，不增加`AllocationHeader`。这与`clib_mem_heap_free`的wrong-active-heap assert和
`mem_intercept.c`按当前intercept flag选择allocator的逻辑一致。

因此不存在“初始化后free/realloc bootstrap block”的合法路径，也没有切换后的provenance
routing。startup owners归零是cutover proof；一旦intercept开启，任何System/libc pointer
进入free/realloc都违反该前置条件并按wrong-heap abort。

OOM 对 Rust GlobalAlloc 返回 null，由 Rust allocation failure boundary 终止；C malloc-family
保持 C ABI 的 null/`ENOMEM`。显式 `MemHeap` nullable API 可返回 null。wrong-active-heap
free、double free和mspace corruption是programmer/shared-state violation，立即abort，不进入
`SvmRegionError`。

### D4：所有 `MemHeap` 统一使用 dlmalloc mspace

Main Heap、process-private heap 和 SVM PVT/Data Heap 只有一个 backend：从 vendored VPP
使用的 public-domain dlmalloc 提取的 mspace。owner 先映射完整 range，再调用
`create_mspace_with_base(base, size, locked)`，随后 `mspace_disable_expand`；`MemHeap`
控制块也从该 mspace 自身分配。Main Heap 与 shared heap 固定 `locked = true`，只有由一个
OS thread完整拥有的 process-private heap才允许创建为 unlocked。

#### D4.1 source 与 build ownership

目录与职责固定为：

```text
third_party/dlmalloc/
  LICENSE
  README.md      # upstream/VPP commit、提取范围和本地差异
  dlmalloc.c
  dlmalloc.h
```

`third_party/vpp` 仍只作证据树，不成为 build dependency。`hammer-infra` 已有的
`build.rs` 是唯一编译 owner；不增加单独的 sys crate。Cargo 只增加 build-time `cc`：

```toml
# workspace Cargo.toml
[workspace.dependencies]
cc = "1"

# crates/hammer-infra/Cargo.toml
[build-dependencies]
cc = { workspace = true }
```

`build.rs` 保留已有 Linux interpose linker 参数，并增加以下 mspace 编译；所有 source
路径都从 `CARGO_MANIFEST_DIR` 解析：

```rust
use std::env;
use std::ffi::OsStr;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"),
    );
    let source_dir = manifest_dir.join("../../third_party/dlmalloc");

    let mut build = cc::Build::new();
    build
        .file(source_dir.join("dlmalloc.c"))
        .include(&source_dir)
        .define("ONLY_MSPACES", "1")
        .define("MSPACES", "1")
        .define("USE_LOCKS", "1")
        .flag_if_supported("-fvisibility=hidden");

    if env::var_os("CARGO_CFG_TARGET_OS").as_deref() == Some(OsStr::new("macos")) {
        build.define("DARWIN", "1");
    }

    build.compile("dlmalloc");

    println!("cargo:rerun-if-changed={}", source_dir.join("dlmalloc.c").display());
    println!("cargo:rerun-if-changed={}", source_dir.join("dlmalloc.h").display());

    if env::var_os("CARGO_CFG_TARGET_OS").as_deref() == Some(OsStr::new("linux")) {
        println!("cargo:rustc-link-arg=-Wl,-z,interpose");
    }
}
```

`ONLY_MSPACES=1` 保证 dlmalloc 不定义或接管 process `malloc/free`。Rust
`GlobalAlloc` 与已有 C malloc-family interposition 仍由 `MemMain` 统一接收，再把操作
派发到当前 active `MemHeap.mspace`；dlmalloc 不能成为第二个全局 allocator。

#### D4.2 Rust FFI

FFI 是 `hammer-infra::mem` 的私有 implementation surface。`Mspace` 只是 C opaque
pointer alias，不是 allocator owner、backend type或公开 API：

```rust
type Mspace = *mut c_void;

unsafe extern "C" {
    fn create_mspace_with_base(
        base: *mut c_void,
        capacity: usize,
        locked: i32,
    ) -> Mspace;
    fn destroy_mspace(mspace: Mspace) -> usize;
    fn mspace_disable_expand(mspace: Mspace);
    fn mspace_memalign(
        mspace: Mspace,
        alignment: usize,
        size: usize,
    ) -> *mut c_void;
    fn mspace_realloc_in_place(
        mspace: Mspace,
        pointer: *mut c_void,
        size: usize,
    ) -> *mut c_void;
    fn mspace_free(mspace: Mspace, pointer: *mut c_void);
    fn mspace_usable_size(pointer: *const c_void) -> usize;
    fn mspace_is_heap_object(mspace: Mspace, pointer: *mut c_void) -> i32;
    fn mspace_least_addr(mspace: Mspace) -> *mut c_void;
    fn mspace_footprint(mspace: Mspace) -> usize;
}
```

不绑定或调用 process-global `dlmalloc`/`dlfree`，也不暴露 C mspace symbol 给 plugin 或
业务 crate。`MemHeap` 不保存 backend enum或函数表。workspace 删除
`libmimalloc-sys` dependency、mimalloc arena registration、`mi_*` interposition调用和
plugin image中的mimalloc authority检查。任一 mspace 耗尽都不得外扩或回落 Main Heap。

### D5：SVM region 不再是 SSVM payload

`SvmRegion` 对应 VPP `svm_region_t`/`svm_map_region`，直接拥有 region mapping 的本地
fd/base/size 生命周期；shared header 位于 mapping page 0。它不嵌套在
`SsvmPrivate` payload 中，也不复用 SSVM ready/header。普通 `SsvmPrivate` 继续服务其已有
协议，两者不能互相冒充。

creator 必须取得非零 fixed VA；attach 先临时映射 page 0，读取并验证 version、base 和 size，
解除 probe 后在 creator 发布的同一 VA 映射整个 region。目标 range 已占用时返回 typed
mapping error，不覆盖现有 mapping。Hammer 用 `MAP_FIXED_NOREPLACE` 或等价的 reserve +
validated fixed map，不能用裸 `MAP_FIXED` 擦除未知 mapping。

同 VA 是 region ABI，不再承诺不同地址 attach。共享 Rust collection 只支持同 architecture、
同 Hammer build/layout version 的进程；attach 在接触任何 shared collection 前验证版本和
layout fingerprint。

### D6：每个 region 永远有 pvt heap

region page 0 是 `SvmRegionHeader`；pvt heap 从 `base + page_size` 开始，默认
`SVM_PVT_HEAP_SIZE = 128 KiB`。creator 调用 `MemHeap::create_at` 在这段 range 建 locked
shared heap，并把返回的 `*mut MemHeap` 写入 `pvt_heap`。该控制块和 mspace state 都位于
映射中。attach 只读取并验证该 pointer/range，绝不 recreate、reload 或转成 offset heap。

creator 持 region mutex并激活 pvt heap后，分配 region name、client pid Vec、root bitmap
等 metadata。每个 metadata mutation 都遵守：

```text
lock region -> activate pvt_heap -> mutate/drop ordinary collections
            -> restore previous heap -> unlock region
```

`SvmRegionHeader` 使用 `pvt_heap` 这个 Hammer 字段名，明确表达实际角色；证据中保留 VPP
字段名 `region_heap`。不存在 `metadata_heap` 字段或 `SvmRegionHeap` 类型。

VPP header中的`bitmap_size`、`backing_file`、`filenames`和`client_pids`同样保留。
`bitmap_size`参与root VA诊断和边界检查；`backing_file`由file-backed data mapping路径写入。
vendored tree没有`filenames`调用点，但它仍作为VPP header字段移植并由PVT Heap拥有；不得
用该字段虚构新的文件注册API。

#### D6.1：PVT Heap 使用 `MemHeap` 的实际调用链

PVT Heap没有单独allocator类型或分配API。creator先完成header mutex/condvar初始化并取得
region lock，然后在第二个mapped page开始的range直接创建locked `MemHeap`，把控制块raw
pointer发布到header：

```rust
let pvt_heap_size = if config.pvt_heap_size == 0 {
    SVM_PVT_HEAP_SIZE
} else {
    config.pvt_heap_size
};
let pvt_base = NonNull::new(base.as_ptr().wrapping_add(page_size))
    .expect("mapped region base plus one page is non-null");
let pvt_heap = unsafe {
    MemHeap::create_at(pvt_base, pvt_heap_size, true, "svm region")?
};
unsafe {
    header.as_mut().pvt_heap = pvt_heap.as_ptr();
}
```

`MemHeap::create_at`在该range内调用`create_mspace_with_base`、disable expand，并从同一
mspace分配`MemHeap`控制块及尾随name；因此`pvt_heap`指向region mapping内的真实
`MemHeap`，不是descriptor、offset或process-local proxy。attach在same VA完整映射后验证
该pointer落在`[base + page_size, base + page_size + pvt_heap_size)`且`base/size/mspace`
自洽，只复用它，绝不再次调用`create_at`。

所有PVT-owned普通对象必须按以下顺序创建、修改和销毁：

```rust
let region_lock = region.lock()?;
let active_heap = region_lock.pvt_heap().activate();

let region_name = Box::into_raw(Box::new(config.name.clone()));
let client_pids = Box::into_raw(Box::new(vec![current_pid]));
let bitmap = Box::into_raw(Box::new(Bitmap::with_capacity(region_page_count)));

unsafe {
    header.as_mut().region_name = region_name;
    header.as_mut().client_pids = client_pids;
    header.as_mut().bitmap = bitmap;
}

drop(active_heap); // 恢复进入region前的active heap
drop(region_lock); // 随后释放region mutex
```

这里的`Box`、`String::clone`、`Vec`增长和`Bitmap` backing都调用process-global
`MemMain::GlobalAlloc`；它读取当前`MemThreadMain.active_heap`，最终调用PVT
`MemHeap.mspace`。实现不得把这些对象先在Main Heap构造后move进region，也不得给PVT
metadata增加显式allocator参数或另一套collection。

attach追加PID使用同一路径：

```rust
let region_lock = region.lock()?;
let active_heap = region_lock.pvt_heap().activate();
unsafe {
    (&mut *(*header.as_ptr()).client_pids).push(current_pid);
}
drop(active_heap);
drop(region_lock);
```

实际实现可用经过验证的private pointer helper消除示例中的重复unsafe，但helper不得改变
owner、返回越过`RegionLock`的引用或接收caller closure。声明顺序保证unwind时先drop
`ActiveHeap`恢复previous heap，再drop `RegionLock`解锁。

Data Heap完全复用同一机制：`MemHeap::create_at(data_base, data_size, true, "svm data")`
创建后写入`data_heap`；持锁的caller通过`region_lock.data_heap()`取得`&MemHeap`并调用
`activate()`。无Data Heap时返回`None`，不得选择PVT Heap或Main Heap。最后client的unmap
先在region PVT `MemHeap` active时删除PID；若计数归零，再切到root PVT `MemHeap`完成
name hash、subregion pool和bitmap清理，恢复原heap后才解锁和unmap。

### D7：root init 精确对应 `svm_region_init_internal`

root region 使用 `NODATA`/subdivided flag，不创建 data heap。creator map path先创建 pvt
heap，并把 `NEED_DATA_INIT` 作为内部未完成状态。`svm_region_init_internal` 对应的 Hammer
owner随后持锁、清 pending flag、激活 pvt heap，并在其中构造：

- `SvmMainRegion`；
- `HashMap<String, u32>` 名字表；
- `Pool<SvmSubregion>`。

shared `SvmMainRegion` 不保存 process-private fd。构造完成后，header 的 `data_base` 指向
pvt heap 中的 `SvmMainRegion`。这条指针赋值不是 data heap，也不是 data-section offset。

Hammer 不移植 VPP 的 `root_path`/uid/gid 文件路径策略，因为当前产品通过 Unix socket
鉴权并用 `SCM_RIGHTS` 交付 fd；daemon rendezvous owner的process-private fd table在Main
Heap中构造和销毁，不在PVT active scope内，也不进入shared pointer layout。这是transport
divergence，不改变pvt heap、bitmap或fixed-VA语义。

### D8：subregion 与可选 data heap

root pvt heap中的 bitmap按 page 标记 root VA range。`find_or_create` 在 root lock + active
pvt heap 下查名字；已存在时 attach 对应 backing，新建时先找连续 clear pages，把
`base = root_base + page_index * page_size` 写进 args，再创建 fixed-VA region。名字表 value
继续是 `Pool<SvmSubregion>` index，不改成 monotonic region id。

每个普通 subregion 同样先创建自己的 pvt heap。只有请求 `DATA_HEAP` 时，creator 才在
`data_base` 创建另一个 locked `MemHeap` 并写 `data_heap`；attach 只复用。调用方持 region
lock 后可从 `RegionLock` 直接借 pvt/data `&MemHeap` 并取得 `ActiveHeap`。无 data heap 时
`data_base` 是调用方数据起点，不能回落 pvt 或 main heap。

名字插入、bitmap 占用和 backing 创建是一个 owner transaction：全部验证和 reserve 在首个
shared mutation 前完成；之后失败按记录逆序撤销 bitmap、名字/pool entry、mapping/fd，且
restoration token 必须先恢复 active heap。与 VPP `svm_region_init_mapped_region` 先发布
version、只 warning data setup error 的行为相比，Hammer 在完整初始化后先写layout version，
再以version作为最后一次Release publish；失败绝不发布半初始化 region。

### D9：共享对象使用普通 collection，但不获得普通 Rust owner lifecycle

`SvmMainRegion`、region name、client pid list、bitmap、HashMap/Pool backing allocations 都由
active pvt heap提供；data-heap user objects同理。这些对象位于 fixed-VA mapping，内部 raw
pointer 在 peer 中保持同值。

进程本地 `SvmRegion` 只借用 shared object，不按值拥有它们。unmap 不运行整个 shared object
graph 的 Rust `Drop`；删除单个 name/client/value 时，必须在对应 heap active 的情况下执行
该对象的正常 drop。和`svm_region_unmap_internal`一致，`unmap(self)`删除当前pid后若发现
`client_pids`为空，就在root lock + active root PVT Heap内复制region name、释放bitmap区间、
删除subregion pool entry和name hash entry，再释放复制的name并unmap。不存在独立的
`remove_subregion`路径，也不得把`SvmSubregion`或其它PVT-owned值返回到Main Heap scope。
`create/attach`在region lock + active PVT Heap内直接把当前pid加入`client_pids`；显式
`unmap(self)`在相同scope内直接删除pid并完成本地unmap，不增加独立注册对象。
最后region teardown先使名字不可发现并等待使用者退出，再unmap；mapping消失即同时结束
pvt/data heap storage生命周期。

### D10：`DataPlaneMain` 构造不因 allocator TLS 改动

`MemThreadMain` 不放进 `DataPlaneMain`、`WorkerThread` 或共享 `Vec`，所以 ADR-0009 的
worker `DataPlaneMain` 预构造并 move 到 OS thread 与本文没有 ownership 冲突。worker closure
在执行可能切 heap 的 worker-init 前登记 allocator TLS；main thread在 `MemMain` 初始化时
登记 index 0；只有明确保证存活到process exit的auxiliary runtime thread才在自己的entry
登记，短命auxiliary thread只使用未登记的Main Heap default。

如果后续设计要求 `DataPlaneMain` 按值拥有 `MemThreadMain`，那是不同设计，届时必须先修改
ADR-0009 和 worker construction。本文不做这个修改，也不为一个不存在的字段重排 worker
启动链。

### D11：region owner death不增加shared状态字段

`SvmRegionHeader`删除当前failure latch，不以其它enum、failed bit或retired bit替代；和VPP
一样，version是唯一ready authority，`mutex_owner_pid`/`mutex_owner_tag`记录当前lock owner。
正常unlock先把两者清零。attach和lock在进入heap前检查owner信息，robust mutex acquisition
也必须区分owner-dead/not-recoverable与普通lock failure。

检测到dead owner时，当前进程返回owner-local
`SvmRegionError::OwnerDied { owner_pid, owner_tag }`，不返回guard，不激活、验证、修复或销毁
pvt/data heap，也不读取或drop任何shared collection。mutex和dead owner字段保持不可继续
使用的事实，因此后续attach/lock同样失败。lifecycle owner只关闭自己的fd并unmap本地
mapping；健康root可在自己的lock + active PVT Heap中删除该subregion的name、bitmap和pool
记录，但不得进入dead-owner subregion heap。root本身发生owner death时停止该root的全部
操作。

shared layout仍与VPP一致，不为错误处理创造第二个状态机。Hammer唯一差异是拒绝
`svm.c:709-751`的force mutex reinit/continue路径：VPP dlmalloc mspace内部lock不是robust，
region mutex恢复不能证明allocator或collection一致。

## 5. 所有权、同步、发布与回收

| 状态 | Owner | Writers/readers | 同步与发布 | 回收 |
| --- | --- | --- | --- | --- |
| `MemMain` | process `hammer-infra` | startup写page/NUMA/intercept/main-heap状态；mapping owner在map lock下写双向链；allocator/diagnostics读 | one-time init Release/Acquire；heap inventory使用control-plane mutex；mapping双向链只在map lock下读写 | process exit；main heap不提前销毁；map owner逐项unmap并摘链 |
| `MemThreadMain` | 当前 OS thread 的 allocator TLS | 仅当前thread写/读；`MemMain` inventory只含process-lifetime runtime threads并只诊断 | TLS program order；selector返回瞬时raw pointer/Copy值，不产生安全`'static`borrow | 已登记thread存活到process exit；短命thread从不入inventory，TLS退出无需摘链 |
| process `MemHeap` | `MemMain` | 所有 thread经各自 active selection使用 | locked dlmalloc mspace | process exit；live allocation存在时不得 destroy |
| pvt/data `MemHeap` | owning `SvmRegion` mapping | attached processes只能在`RegionLock`borrow期间使用 | locked mspace；复合metadata另持process-shared robust region mutex | 正常时region不可发现且无clients后随mapping消失；owner death后不再进入heap |
| active selection | 当前 `MemThreadMain` | 当前 thread唯一 writer/reader | `ActiveHeap` RAII nested restore | scope结束恢复 previous heap |
| root registry/bitmap | root region pvt heap | region owner和attached clients | root region mutex；version最终Release publish | 名字先删除，bitmap后释放，backing最后关闭 |

不得在 active selector周围添加 `Mutex`、`RwLock`、atomic pointer publication 或
WorkerBarrier。WorkerBarrier不覆盖外部进程，不能证明 SVM heap安全。

## 6. 错误与失败原子性

`MemError` 归 `hammer-infra::mem`，只覆盖调用方能处理的control-plane失败：无效Main Heap
配置、VM address reservation、payload/header map、hugepage `mlock`、header protection、
unmap、backing fd create/seal/page-size query、NUMA policy和mspace heap creation。每个variant
直接命名失败操作并携带base、size、page size、fd、NUMA node等恢复所需事实；底层OS error
保留`#[source]`。不再用`Mapping { source: PhysmemError }`把一组操作压成另一层错误，也没有
任何mimalloc variant。

以下不是 `MemError`：

- active heap OOM：Rust allocation boundary终止，C ABI返回 null/`ENOMEM`；
- wrong active heap、double free或mspace corruption：abort；
- system page size无法取得：与`clib_mem_main_init`一致，是进程环境不成立，startup abort；
- 重复Main Heap初始化、重复thread registration或损坏的heap/map链：assert/abort；
- SVM version/layout/base不匹配：`SvmRegionError`；
- region backing fd/map失败：`SvmRegionError` 保留 OS source；
- fixed VA冲突：attach/create失败且不覆盖现有 mapping。
- region mutex owner death：`SvmRegionError::OwnerDied`；不创建shared failure state且不访问
  pvt/data heap或collection，caller只能停止使用并释放process-local mapping资源。

初始化顺序为 reserve/map -> mspace create/disable-expand -> private owner fields -> shared
metadata -> version Release。version发布前的失败逆序destroy未发布mspace、关闭fd、unmap并
释放reserve。cleanup error可见，但不替换primary error。

VPP与Hammer错误边界逐项对应如下：

| VPP路径 | VPP行为 | Hammer行为 |
| --- | --- | --- |
| `linux/mem.c:103-129`，`clib_mem_main_init` | system page query失败panic；hugepage/NUMA探测失败分别保留unknown/zero | system page失败startup abort；hugepage用`Option::None`、NUMA用zero bitmap，不制造error variant |
| `mem.c:31-65`，`clib_mem_vm_reserve` | 失败返回`~0` | `AddressReservation`保留requested base/size/alignment和OS source |
| `linux/mem.c:308-410`，`clib_mem_vm_map_internal` | reserve/map/mlock/header-map任一步失败返回sentinel | 分别返回`AddressReservation`、`AddressRangeOccupied`、`VirtualMemoryMap`、`VirtualMemoryLock`或`MappingHeaderMap`，失败前不接mapping链 |
| `linux/mem.c:413-452`，`clib_mem_vm_unmap` | mprotect/munmap失败返回`CLIB_MEM_ERROR` | `MappingHeaderProtect`或`VirtualMemoryUnmap`；摘链与unmap保持failure-atomic |
| `linux/mem.c:214-284`，`clib_mem_vm_create_fd` | 失败写`clib_mem_main.error`并返回错误码 | `BackingFileCreate`、`BackingFileSeal`或`BackingFilePageSize`直接返回给Rust caller并保留OS source |
| `linux/mem.c:558-607`，NUMA affinity | unsupported node或syscall失败写last error并返回错误码 | `NumaUnavailable`、`NumaNodeUnavailable`或`NumaPolicy` |
| `mem_dlmalloc.c:41-100`，heap create | map/mspace失败返回null | mapping错误沿上述variant返回；mspace null返回`HeapCreation` |
| `mem_dlmalloc.c:361-443`，ordinary allocation | no-fail入口调用`os_out_of_memory`，nullable入口返回null | Rust `GlobalAlloc`返回null交给allocation failure boundary；显式nullable API返回`None`；C返回null/`ENOMEM` |
| `mem_dlmalloc.c:518-537`，free | `mspace_is_heap_object`失败ASSERT | wrong active heap、double free和损坏mspace直接abort |
| `mem_dlmalloc.c:116-123`、`mem.c:20-28`，init | Main Heap重复初始化与错误thread-init顺序由ASSERT/owner lifecycle排除 | 重复Main Heap初始化、重复thread registration均assert，不进入`MemError` |

## 7. 层隔离契约

| 层 | 允许调用 | 禁止调用/保存 | 验证 |
| --- | --- | --- | --- |
| `mem` | dlmalloc mspace、System bootstrap、allocator TLS | SVM name/member policy、runtime graph/plugin状态、caller closure、第二allocator backend | infra allocation tests、C ABI tests、wrong-heap subprocess |
| `SvmRegion` | mapping/fd、region mutex、owner pid/tag、`MemHeap`、standard collection/Pool、root bitmap | `SsvmPrivate` header、offset heap、runtime WorkerBarrier、fallback Main Heap、owner-death后访问shared heap、额外shared状态字段 | same-VA subprocess creator/attach与owner-death failure |
| `RegionLock` | 直接借 pvt/data `MemHeap` 并创建 `ActiveHeap` | 返回越过lock lifetime的heap借用、从allocator TLS取得safe heap reference、跨`.await` scope | compile-time lifetime用例、nested restore行为 |
| runtime thread entry | 只为process-lifetime OS thread登记index；短命thread仅使用unregistered TLS default | 拥有或跨线程move `MemThreadMain`、登记可能在线程退出时失效的TLS address、为allocator添加第二个TLS | startup与short-lived thread integration |
| App/session rendezvous | 交付 fd、base、size和协议版本 | 把 fd/进程本地表写入 shared `SvmMainRegion` | multi-process attach |

## 8. 变更清单

### 8.1 新增或修改，已批准

| ID | 类型/API | Owner / consumer | 最终结果与现有接口不足 |
| --- | --- | --- | --- |
| A1 | `#[global_allocator] static MEM_MAIN: MemMain`；`GlobalAlloc`实现；page/NUMA/intercept/map/heap/thread字段；`MemVmMapHeader`；direct getter；public startup只经`MainHeapConfig::initialize` | `hammer-infra::mem`；所有Rust/C allocation、VM mapping与daemon/ctl startup | 移植有运行语义的`clib_mem_main_t` authority并让它本身成为Rust global allocator；C last-error pointer由owned typed `Result`替代；删除`HammerMainHeap`代理和`MemDiagnostics` wrapper |
| A2 | `MemThreadMain`；unsafe process-lifetime registration、private raw active selector、Copy thread index | allocator TLS；runtime thread entries | `GlobalAlloc`需要VPP等价current-thread selector，但不得制造`'static`safe borrow；短命thread不进入TLS-address inventory |
| A3 | `MemHeap`；`create_at`、`allocate`、`allocate_zeroed`、`reallocate`、`deallocate`、`contains`、`base`、`size` | `MemMain`、SVM region、现有 explicit allocator consumers | 统一 process heap和pvt/data heap身份；替代 crate-private `Heap` 和 `SvmRegionHeap` |
| A4 | `ActiveHeap<'a>`；`MemHeap::activate` | 需要临时切换的 region/allocator caller | 保存并 RAII恢复 previous heap；不能用 closure或公开 raw setter表达 unwind-safe nested scope |
| A5 | `MemError` concrete operation variants | `hammer-infra::mem`；startup、VM mapping、backing fd、NUMA和heap create seam | 替换当前`MainHeapError`及`PhysmemError`透传；VPP assert/OOM边界不进入该enum，OS source只在真实control-plane失败处保留 |
| A6 | `SvmRegionHeader` 的 VPP fields：`virtual_base`、`pvt_heap: *mut MemHeap`、`data_base`、`data_heap: *mut MemHeap`、bitmap/name/backing/client pointers | `hammer-infra::svm::region` | 恢复 VPP pointer/fixed-VA layout；删除两个内嵌offset heap descriptor和当前failure latch，不增加替代state |
| A7 | `SvmRegion` creator/attach/find-or-create/unmap、`RegionLock::{pvt_heap,data_heap}` direct borrows和typed owner-death error | infra SVM consumers | region自己拥有fixed-VA mapping并直接增删`client_pids`；heap borrow受lock lifetime约束；owner death后拒绝attach/lock |
| A8 | `SvmMainRegion`、`SvmSubregion` 使用 `HashMap`/`Pool`/bitmap 的 active-pvt allocation；最后client的`unmap`完成root cleanup | root region | 恢复`svm_region_init_internal`、root VA切分和`svm_region_unmap_internal`单一路径；heap-owned值的drop不逃出active PVT scope |
| A9 | `third_party/dlmalloc/{LICENSE,README.md,dlmalloc.c,dlmalloc.h}`；workspace build-time `cc`；`hammer-infra/build.rs` mspace build；`hammer-infra::mem` private FFI | `hammer-infra::mem`唯一 backend | Main/PVT/Data Heap统一使用VPP同类mspace；不编译`mem_dlmalloc.c`，不增加sys crate，不公开mspace symbol；当前offset allocator与mimalloc都不能满足完整目标 |
| A10 | 删除 `libmimalloc-sys`、mimalloc arena/interposition 和 image-authority checks | workspace、`hammer-infra`、daemon example | 不保留第二allocator authority或无调用的mimalloc binding |
| A11 | 零live-allocation bootstrap cutover、`mspace_is_heap_object`与统一Rust/C routing | global allocator/interposition与process entry | 不增加allocation header、pointer-owner scan或provenance routing；bootstrap owner在publication前全部结束，之后只由active mspace处理，wrong heap abort |
| A12 | `SvmRegionError::OwnerDied`和无额外shared state的owner-death stop boundary | attach/lock与region lifecycle owner | shared layout保持VPP字段；robust region mutex不能证明非robust shared mspace一致，因此不force-unlock后继续 |

以上批准不自动授权公开 backend enum、allocator trait、generic TLS container、closure access、
`dyn` dispatch、不同 VA兼容模式或第二allocator backend。

### 8.2 规范 Rust API

以下类型、全部字段与方法签名是实施契约。字段即使不公开，也不得在实现时省略、替换 owner
或藏进未审查的状态容器；不允许增加第二allocator backend、public raw heap setter或
closure-mediated access。

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MainHeapConfig {
    #[serde(rename = "main_heap_size")]
    pub size: Byte,
    #[serde(rename = "main_heap_page_size")]
    pub page_size: PageSize,
    #[serde(rename = "default_hugepage_size")]
    pub default_hugepage_size: Option<PageSize>,
}

impl MainHeapConfig {
    pub fn validate(&self) -> Result<(), MemError>;
    pub fn size_bytes(&self) -> Result<usize, MemError>;
    pub fn initialize(&self) -> Result<usize, MemError>;
}

pub struct MemMain {
    main_heap: *mut MemHeap,
    heaps: Vec<*mut MemHeap>,
    threads: *mut MemThreadMain,
    log2_page_size: u8,
    log2_default_hugepage_size: u8,
    log2_system_default_hugepage_size: u8,
    alloc_free_intercept: u8,
    numa_node_bitmap: u64,
    first_map: *mut MemVmMapHeader,
    last_map: *mut MemVmMapHeader,
    map_lock: u8,
}

impl MemMain {
    const fn new() -> Self;
    pub fn main_heap() -> &'static MemHeap;
    pub fn heap_count() -> usize;
    pub fn registered_thread_count() -> usize;
    pub fn mapping_count() -> usize;
    pub fn system_page_size() -> usize;
    pub fn default_hugepage_size() -> Option<usize>;
    pub fn system_default_hugepage_size() -> Option<usize>;
    pub fn set_default_hugepage_size(page_size: PageSize) -> Result<(), MemError>;
    pub fn next_numa_node(previous: Option<u32>) -> Option<u32>;
    pub fn set_numa_affinity(numa_node: u32, force: bool) -> Result<(), MemError>;
    pub fn set_default_numa_affinity() -> Result<(), MemError>;
    pub(crate) fn vm_create_backing(
        page_size: PageSize,
        name: &str,
    ) -> Result<OwnedFd, MemError>;
    pub(crate) fn vm_map(
        base: Option<NonZeroUsize>,
        size: usize,
        page_size: PageSize,
        backing: Option<BorrowedFd<'_>>,
        backing_offset: u64,
        alignment: usize,
        name: &str,
    ) -> Result<NonNull<u8>, MemError>;
    pub(crate) unsafe fn vm_unmap(base: NonNull<u8>) -> Result<(), MemError>;
}

#[global_allocator]
static mut MEM_MAIN: MemMain = MemMain::new();

unsafe impl GlobalAlloc for MemMain {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8;
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8;
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout);
    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: Layout,
        new_size: usize,
    ) -> *mut u8;
}

#[repr(C)]
struct MemVmMapHeader {
    base_address: usize,
    page_count: usize,
    page_size_log2: u8,
    backing_fd: RawFd,
    name: [u8; 64],
    previous: *mut MemVmMapHeader,
    next: *mut MemVmMapHeader,
}

#[repr(C)]
pub struct MemThreadMain {
    active_heap: *mut MemHeap,
    thread_index: u32,
    next: *mut MemThreadMain,
    trace_thread_disable: i32,
}

impl MemThreadMain {
    unsafe fn current() -> *mut Self;
    fn active_heap() -> *mut MemHeap;
    pub unsafe fn register_current(thread_index: u32);
    pub fn thread_index() -> Option<u32>;
}

#[repr(C)]
pub struct MemHeap {
    base: *mut c_void,
    mspace: *mut c_void,
    size: usize,
    page_size_log2: u8,
    locked: u8,
    traced: u8,
    unmap_on_destroy: u8,
    name: [u8; 0],
}

impl MemHeap {
    pub(crate) unsafe fn create_at(
        base: NonNull<u8>,
        size: usize,
        locked: bool,
        name: &str,
    ) -> Result<NonNull<Self>, MemError>;

    pub fn activate(&self) -> ActiveHeap<'_>;
    pub fn allocate(&self, layout: Layout) -> Option<NonNull<u8>>;
    pub fn allocate_zeroed(&self, layout: Layout) -> Option<NonNull<u8>>;
    pub unsafe fn reallocate(
        &self,
        pointer: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Option<NonNull<u8>>;
    pub unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout);
    pub fn is_heap_object(&self, pointer: NonNull<u8>) -> bool;
    pub fn base(&self) -> NonNull<u8>;
    pub fn size(&self) -> usize;
    pub fn name(&self) -> &str;
}

#[must_use = "dropping ActiveHeap restores the previous heap"]
pub struct ActiveHeap<'heap> {
    active_heap: &'heap MemHeap,
    previous_heap: *mut MemHeap,
}

impl Drop for ActiveHeap<'_> {
    fn drop(&mut self);
}

#[derive(Debug, thiserror::Error)]
pub enum MemError {
    MainHeapTooSmall {
        requested: usize,
        minimum: usize,
    },
    MainHeapSizeOverflow {
        requested: usize,
        page_size: usize,
    },
    PageSizeUnavailable {
        requested: PageSize,
    },
    AddressReservation {
        requested_base: Option<usize>,
        size: usize,
        alignment: usize,
        #[source]
        source: io::Error,
    },
    AddressRangeOccupied {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    VirtualMemoryMap {
        requested_base: Option<usize>,
        size: usize,
        page_size_log2: u8,
        backing_fd: Option<RawFd>,
        backing_offset: u64,
        #[source]
        source: io::Error,
    },
    VirtualMemoryLock {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    MappingHeaderMap {
        address: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    MappingHeaderProtect {
        address: usize,
        writable: bool,
        #[source]
        source: io::Error,
    },
    VirtualMemoryUnmap {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    BackingFileCreate {
        page_size_log2: u8,
        #[source]
        source: io::Error,
    },
    BackingFileSeal {
        fd: RawFd,
        #[source]
        source: io::Error,
    },
    BackingFilePageSize {
        fd: RawFd,
        #[source]
        source: io::Error,
    },
    NumaUnavailable {
        requested: u32,
    },
    NumaNodeUnavailable {
        requested: u32,
        available: u64,
    },
    NumaPolicy {
        requested: Option<u32>,
        force: bool,
        #[source]
        source: io::Error,
    },
    HeapCreation {
        base: usize,
        size: usize,
        locked: bool,
    },
}
```

`main_heap::{init, init_with, init_default}` 不属于最终API。所有startup、test和tool调用方
统一迁移到 `MainHeapConfig::initialize`。当前`HammerMainHeap`删除，不在`MemMain`外保留
第二个`GlobalAlloc`实现。

```rust
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmRegionFlags(u64);

impl SvmRegionFlags {
    pub const DATA_HEAP: Self;
    pub const NODATA: Self;
    pub fn contains(self, flag: Self) -> bool;
}

#[derive(Debug)]
pub struct SvmRegionConfig {
    pub name: String,
    pub size: usize,
    pub pvt_heap_size: usize,
    pub flags: SvmRegionFlags,
}

#[repr(C, align(64))]
pub(crate) struct SvmRegionHeader {
    version: AtomicU64,
    mutex: MaybeUninit<RawMutexAlloc>,
    condvar: MaybeUninit<RawCondvarAlloc>,
    mutex_owner_pid: AtomicI32,
    mutex_owner_tag: AtomicI32,
    flags: SvmRegionFlags,
    virtual_base: *mut u8,
    virtual_size: usize,
    pvt_heap: *mut MemHeap,
    data_base: *mut c_void,
    data_heap: *mut MemHeap, // root/NODATA为null；仅DATA_HEAP region非null
    user_ctx: AtomicPtr<c_void>,
    bitmap_size: usize,
    bitmap: *mut Bitmap,
    region_name: *mut String,
    backing_file: *mut String,
    filenames: *mut Vec<String>,
    client_pids: *mut Vec<i32>,
}

pub struct SvmSubregion {
    subregion_name: String,
}

pub struct SvmMainRegion {
    subregions: Pool<SvmSubregion>,
    name_hash: HashMap<String, u32>,
}

impl SvmMainRegion {
    pub fn subregion_index(&self, name: &str) -> Option<u32>;
    pub fn subregion(&self, index: u32) -> Option<&SvmSubregion>;
    pub fn subregion_count(&self) -> usize;
}

pub struct SvmRegion {
    backing: OwnedFd,
    base: NonNull<u8>,
    size: usize,
    header: NonNull<SvmRegionHeader>,
    root_header: Option<NonNull<SvmRegionHeader>>,
    is_client: bool,
}

impl SvmRegion {
    pub fn create(
        base: NonZeroUsize,
        config: &SvmRegionConfig,
        backing: OwnedFd,
    ) -> Result<Self, SvmRegionError>;
    pub fn attach(backing: OwnedFd) -> Result<Self, SvmRegionError>;
    pub fn base(&self) -> NonNull<u8>;
    pub fn size(&self) -> usize;
    pub fn flags(&self) -> SvmRegionFlags;
    pub fn lock(&self) -> Result<RegionLock<'_>, SvmRegionError>;
    pub fn client_count(&self) -> Result<usize, SvmRegionError>;
    pub fn remove_exited_clients(&self) -> Result<usize, SvmRegionError>;
    pub fn find_or_create_subregion(
        &mut self,
        config: &SvmRegionConfig,
        backing: OwnedFd,
    ) -> Result<Self, SvmRegionError>;
    pub fn unmap(self) -> Result<(), SvmRegionError>;
}

pub struct RegionLock<'region> {
    header: NonNull<SvmRegionHeader>,
    guard: StandardGuard<'region>,
}

impl RegionLock<'_> {
    pub fn pvt_heap(&self) -> &MemHeap;
    pub fn data_heap(&self) -> Option<&MemHeap>;
    pub fn main_region(&self) -> Option<&SvmMainRegion>;
    pub fn main_region_mut(&mut self) -> Option<&mut SvmMainRegion>;
}
```

`SvmRegionError`使用以下owner-local typed variant；首次检测owner death和之后观察同一
不可恢复mutex的attach/lock都返回该variant：

```rust
#[derive(Debug, thiserror::Error)]
pub enum SvmRegionError {
    // 第6节已有的mapping、layout、client-registration与OS-source variants
    #[error("region mutex owner pid {owner_pid} tag {owner_tag} died")]
    OwnerDied {
        owner_pid: i32,
        owner_tag: i32,
    },
}
```

这里的注释只表示其余既有错误类别由第6节约束，不授权catch-all variant。`create/attach`
登记pid，`unmap`删除pid并处理root注册；三者的shared mutation都在owner的active PVT
scope内完成。`SvmRegion::drop`不得执行fallible shared mutation或吞掉lock、allocator、
client-registration和cleanup错误。

以下类型/字段删除且不保留alias：

```rust
// 删除
RegionMembership<'region>
SvmRegionState
RegionLockTag // 不再公开caller-selected诊断tag
SvmRegion::join
SvmRegion::state
SvmRegion::remove_subregion
SvmRegionHeap
SvmRegionHeapViolation
SvmHashMap<K>
SvmRegionMain
SvmRegionHeader::state
SvmRegionHeader::data_base_offset
SvmRegionHeader::metadata_heap
SvmRegionHeader::root_offset
```

### 8.3 修改调用方

| Surface | 处理 |
| --- | --- |
| `main_heap.rs` 和 startup callers | 状态迁入 `mem::MemMain`；`PageSize` 保留领域含义但由 `mem` 导出；daemon/ctl/test startup统一调用`MainHeapConfig::initialize`，删除旧`init/init_with/init_default` public入口 |
| `HammerMainHeap` 与 `GLOBAL_ALLOCATOR` proxy | 删除；改为`#[global_allocator] static MEM_MAIN: MemMain`直接实现`GlobalAlloc` |
| `third_party/dlmalloc`、workspace Cargo与`hammer-infra/build.rs` | vendored source记录VPP commit与提取差异；`cc`按D4.1构建hidden mspace-only archive；`mem`私有FFI按D4.2链接 |
| `heap.rs`、`heap_boxed.rs`、Bihash explicit allocator | crate-private `Heap` 改为直接借 `MemHeap`；不保留 vtable或 `Heap::svm_data` |
| Rust `GlobalAlloc` 与 C interpose | 改读private current-thread raw active selector；cutover前证明bootstrap live allocation为零；cutover后只接受active mspace object，不做bootstrap provenance routing或heap inventory scan |
| workspace与plugin image检查 | 删除`libmimalloc-sys`依赖、所有`mi_*`调用与mimalloc authority检查；改为验证唯一dlmalloc/mem authority位于`hammer-infra` |
| runtime main/worker/aux thread entry | main、Data Worker和process-lifetime auxiliary thread在各自OS thread登记index；短命auxiliary/external thread不进inventory；不改变`DataPlaneMain` ownership |
| `svm/region.rs` | 按 D5-D11 重写 mapping/header/root/subregion/heap操作；`create/attach/unmap`直接增删`client_pids`；删除`RegionMembership`、`SvmRegionState`、`join/state`和public caller-selected lock tag；layout version bump，拒绝旧 layout |
| SVM region consumers | offset变成 same-VA pointer；mutation在 RegionLock + ActiveHeap scope内使用普通 collections |

### 8.4 删除

| 删除项 | 原因 | 迁移证明 |
| --- | --- | --- |
| `svm/region_heap.rs` 全文件、`SvmRegionHeap`、`SvmRegionHeapViolation` | VPP没有 offset heap；pvt/data 都是 `MemHeap` | type/API调用者归零，region subprocess走shared mspace |
| `svm/hash_map.rs` 全文件及全部 `Svm*Entry/Iter/Keys/Values` | active pvt heap已让普通 HashMap分配在region | 名字插入/查询/删除/rehash跨进程通过 |
| `SvmRegionHeader::{metadata_heap,data_heap: SvmRegionHeap,data_base_offset,...}` 的 offset layout | 与 VPP fixed pointer ABI相反 | layout probe和same-VA attach通过；旧版本明确拒绝 |
| `RegionMembership`、`SvmRegion::join` | VPP在create/attach/unmap函数内直接增删`client_pids`，没有独立注册owner | create/attach各追加一次，unmap直接删除一次；无fallible Drop路径 |
| `SvmRegionState`、`SvmRegion::state`及header failure latch | VPP shared header只有`version` ready字段和mutex owner诊断字段 | layout无额外state；owner death返回typed error且不访问heap |
| public `RegionLockTag`与caller-selected `Membership`/`Allocate` tag | VPP tag是`svm.c` file-local lock调用点的调试整数，不是public lifecycle API | public `lock()`不接收tag；owner内部按调用点记录诊断值 |
| `SvmRegion::remove_subregion` | VPP由最后client的`svm_region_unmap_internal`在root/region lock内完成pool/hash/bitmap和mapping cleanup | 只有`unmap(self)`删除client并在计数归零时清root注册；无第二条删除路径 |
| `SvmRegionMain` monotonic id API | VPP名字表值是subregion pool index，root bitmap拥有地址 | find/create/remove重复index行为与bitmap断言通过 |
| `SvmRegion` over `SsvmPrivate`、不同 VA attach contract | shared heap与普通 collection含raw pointer | occupied-VA attach明确失败；无different-VA测试 |
| `Heap`/`HeapError::AttachedSvmRegion`/private vtable | `MemHeap` 已表达实际分配 authority | Bihash/heap_boxed编译并走明确 heap |
| 旧 ADR-0011 的 offset-heap target与 CONTEXT 同名术语 | 文档不得继续把已否决实现写成目标 | 文档 review 与修正版 ADR-0011 cross-reference |

旧 shared layout没有兼容读取路径、alias或迁移器。实现合入时 bump region version，所有
daemon/app组件必须同批重编译并重建 region。

## 9. 被拒绝方案

1. **继续 `SvmRegionHeap`/offset heap。** 这不是 vendored VPP 的设计，且绕过用户要求的
   active `MemHeap`。
2. **把 pvt heap叫 metadata heap。** metadata 是主要用途，不是VPP heap角色；root
   `data_base` 也实际指向该 heap中的对象。字段统一叫 `pvt_heap`。
3. **保留 mimalloc 作为 process heap。** 这会让 process 与 shared heap拥有不同allocator
   状态、provenance和interposition路径；最终决定统一使用dlmalloc mspace并删除mimalloc。
4. **用 mimalloc exclusive arena直接 attach。** 现有 supported API没有 shared live heap
   reopen，`mi_heap_t`/TLS/TLD仍是process local。
5. **启用 mimalloc中注释掉的 reload代码。** 该代码不是exported contract，也没有并发
   multi-process correctness证据；不能冒充已支持能力。
6. **GlobalAlloc显式传 allocator或只给SVM raw API。** 标准 Vec/HashMap和第三方分配不会
   收到该参数，无法满足 active heap。
7. **把 `MemThreadMain` 放入 `DataPlaneMain`。** allocator在DataPlaneMain构造前、auxiliary
   thread和外部thread也会运行；这会制造不必要的Send/启动冲突。
8. **按 pointer range猜 owner并自动 free。** cutover前bootstrap allocation必须归零；
   cutover后active `mspace_is_heap_object`未命中就是wrong-scope violation。不得扫描其它heap、
   回落System/libc或选择另一个heap执行free，否则会破坏VPP active-free invariant并隐藏
   wrong-scope SVM drop。

## 10. 验证矩阵

测试遵守仓库 final pre-commit gate：实现、review和格式完成前不运行测试。

| Test | 层级与设置 | 必须断言 | 决策/证据 |
| --- | --- | --- | --- |
| `mem_main_initializes_once` | infra unit/integration | system/default/system-default-huge page与NUMA bitmap先初始化；fixed capacity、main heap和intercept按VPP顺序发布一次；重复初始化assert；可恢复失败不部分publish | D1/D4，V2/V5 |
| `mem_errors_are_owned_results` | infra unit/integration | backing-fd/NUMA/map/heap失败直接返回具体`MemError`与OS source；连续失败各自独立拥有；无last-error pointer或借用生命周期 | D1，V2及`linux/mem.c:184-203,267-279,558-607` |
| `vm_mapping_inventory_tracks_lifecycle` | infra unit/subprocess | header占前一页；map成功后在map lock下接first/last/prev/next；unmap摘链；每种OS失败匹配具体`MemError`及字段 | D1，V2/V7 |
| `thread_defaults_to_main_heap` | real OS threads | main/Data Worker登记并落main；短命aux/external thread不登记也落main；其退出后inventory count和每个已登记TLS address仍有效 | D2/D10，V1-V4/V21 |
| `active_heap_restores_nested_scopes` | infra unit | main -> heap A -> heap B -> A -> main；early return/unwind也恢复；token不可跨thread | D2，V4 |
| `active_selector_does_not_escape_region_lock` | compile-time lifetime/API checks | 无public `current() -> &'static MemThreadMain`或TLS-derived `&MemHeap`；region heap borrow与`ActiveHeap`均不能越过`RegionLock`或unmap | D2/D8，V4/V16 |
| `rust_and_c_allocations_use_active_heap` | Rust + linked C probe | Vec/String/HashMap与malloc/calloc/aligned_alloc都落当前heap，alignment/zeroing正确 | D3，V6/V17 |
| `plugin_allocations_use_host_active_heap` | real plugin DSO load/invoke | plugin在Main/PVT/Data active scopes中分别执行Rust collection与C malloc/realloc/free；pointer属于调用时active mspace；对象在同一scope内drop；DSO不产生第二allocator authority | D2-D4，V6/V19 |
| `wrong_active_heap_free_aborts` | subprocess | 在heap A分配、heap B active时drop，进程abort并输出owner facts；不调用System/其它heap | D3，V6 |
| `bootstrap_cutover_has_no_live_allocations` | dedicated startup subprocess + test/debug-only allocation observer | early parse owners先drop；只有Copy config与mmap bytes跨scope；observer在intercept publish前为0；注入retained bootstrap block时拒绝cutover且Main Heap/intercept未发布；切换后不执行System/libc free/realloc routing | D3，V20 |
| `shared_mspace_uses_supplied_range` | C ABI + Rust integration | `MemHeap`控制块与所有block都在base/size内；disable expand；耗尽不回落main | D4/D6，V5/V7 |
| `root_init_allocates_main_region_from_pvt_heap` | infra subprocess | root无data heap；name/pids/bitmap/`SvmMainRegion`都由pvt heap拥有；`data_base`等于该对象pointer | D6/D7，V9/V12/V13 |
| `pvt_heap_routes_global_allocations` | infra unit + independent process attach | creator只调用一次`MemHeap::create_at`并把mapping内pointer写入header；attach复用相同pointer；PVT active时Box/String/Vec/Bitmap/HashMap/Pool block均由PVT mspace识别；scope退出恢复previous heap | D2/D6.1，V4/V5/V9/V11 |
| `subregion_data_heap_follows_flag` | infra integration | 每个region有pvt heap；仅DATA_HEAP flag产生data heap；无flag时绝不fallback | D8，V15/V16 |
| `region_attach_reuses_heaps_at_same_va` | independent exec processes | peer先probe再映射同VA；pvt/data `MemHeap` pointer值相同；两进程交替allocate/free可见；attach没有create/reload | D5-D8，V10/V11 |
| `occupied_fixed_va_rejects_attach_atomically` | subprocess预占目标range | 返回fixed-address error，预占mapping未被覆盖，region/client list不变 | D5/D8，V10 + Hammer safety divergence |
| `root_bitmap_assigns_subregion_va` | multi-process integration | 连续page选择、名字幂等、pool index、删除后bitmap释放；subregion base由root计算 | D7/D8，V14 |
| `region_layout_rejects_old_offset_version` | attach integration | legacy offset region layout被version/fingerprint拒绝，未触碰旧heap字段 | D5，H3-H6 |
| `shared_collections_mutate_under_active_heap` | multi-process integration | Vec/HashMap/Pool grow、remove、drop均在pvt active；peer看到相同内容；unmap不运行whole-graph Drop | D6-D9，V17 |
| `last_client_unmap_cleans_root_inside_pvt_scope` | root/subregion integration + wrong-heap abort guard | `unmap`删除最后pid后在active root PVT Heap内释放复制name、HashMap key和`SvmSubregion`，并清bitmap；caller在Main Heap中没有待drop的PVT-owned值；无独立remove API | D9，V23 |
| `region_client_pids_follow_mapping` | multi-process integration | create/attach在region lock + active PVT Heap内各追加当前pid一次；显式unmap在同一scope内直接删除一次并完成本地mapping cleanup；没有`join`或独立注册对象；`Drop`不执行fallible shared mutation或吞错 | D9，V23 |
| `region_owner_death_stops_without_heap_access` | independent process dies while holdingregion mutex以及注入mspace-mutation窗口 | observer不写额外shared state、不返回guard并返回dead owner pid/tag；不进入pvt/data mspace或collection；后续attach/lock继续返回typed owner-death error；健康root可删除dead-owner subregion注册，dead-owner root停止全部服务 | D11，V22 + Hammer safety divergence |
| `allocator_ffi_links_privately` | infra integration + final-image symbol inspection | `MemHeap`通过private FFI真实调用mspace ABI；mspace symbol保持hidden；dlmalloc不定义`malloc/free`；workspace与Hammer image无`mi_*`、`dlmalloc`、`dlfree`符号 | D4，V5-V7/V18/V19 |
| `region_creation_rolls_back_before_version` | injected map/mspace/name/data-heap failures | version保持0；fd/map/bitmap/name/pool按逆序清理；primary error保留 | D8，failure atomicity |

实施完成后的 final gate 至少包括：

```text
cargo fmt --all -- --check
cargo clippy -p hammer-infra -p hammer-runtime --all-targets
cargo test -p hammer-infra
cargo test -p hammer-runtime
cargo test --workspace
git diff --check
```

## 11. 批准记录与结论

用户已于 2026-09-13 批准 A1-A12，包括：

- `MemMain` / `MemThreadMain` / `MemHeap` 命名与唯一 active authority；
- A2 的 allocator-owned C TLS 窄例外；
- 只有process-lifetime runtime threads进入inventory，短命thread只使用未登记TLS default；
- bootstrap allocation在intercept cutover前归零，不增加切换后provenance routing；
- Main/process-private/PVT/Data Heap统一使用dlmalloc mspace；
- SVM region 同 VA/raw-pointer ABI，以及 root bitmap VA切分；
- `create/attach/unmap`直接维护`client_pids`，删除`RegionMembership`、`join`、
  `SvmRegionState`和`state()`；
- region owner death后的stop boundary、lock-bounded heap borrow和owner-local cleanup；
- VPP last-error pointer由owned typed `MemError` `Result`替代；
- 删除已实现的 `SvmRegionHeap`、`SvmHashMap` 和不同 VA layout，不保留兼容层；
- `third_party/dlmalloc` 的引入与 `libmimalloc-sys`/mimalloc调用的完整删除。

设计 verdict：**Aligned design，已批准，待实施**。active heap、pvt/data heap、root init和
attach ownership与vendored VPP一致；typed `Result`替代last-error pointer、只登记
process-lifetime TLS、safe fixed-map、通过SCM_RIGHTS交付backing fd、version最后发布以及
region owner-death后停止访问是明确的Hammer差异，均有对应验证。当前代码仍是
H1-H6描述的旧实现，不能因本文完成而声称功能已对齐。

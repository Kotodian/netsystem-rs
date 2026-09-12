# ADR-0012：VPP 风格 active heap、内存 Main 与 SVM pvt/data heap

- 日期：2026-09-12
- 状态：Proposed；仅设计，尚未批准，未实施生产代码
- 范围：`hammer-infra` allocator、`svm_region` 的 pvt/data heap、运行时线程入口
- VPP 基线：`third_party/vpp`，提交 `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`
- mimalloc 调研：`third_party/mimalloc`，`v3.5.1-2-gc9cc3394`；
  `third_party/mimalloc_rust`，`a35cc75`
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
4. 当前 mimalloc 能做什么、不能做什么，以及 shared heap 由哪个 backend 承担；
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
| V2 | `src/vppinfra/mem.h:85-119`，`clib_mem_main_t` | process Main 保存 main heap、heap 列表、thread-main 列表、页与映射状态 | `MemMain` 是 `hammer-infra` 的进程级分配 authority |
| V3 | `src/vppinfra/mem.c:16-28`，`clib_mem_thread_init` | 新线程默认把 main heap 设为 active heap，再登记 thread index | 未显式切换的线程必须始终从 main heap 分配 |
| V4 | `src/vppinfra/mem.h:184-200`，`clib_mem_get_heap` / `clib_mem_set_heap` | get 在未初始化时初始化线程状态；set 替换 active heap 并返回旧 heap | nested active-heap scope 必须恢复旧 heap |
| V5 | `src/vppinfra/mem_dlmalloc.c:41-100`，`clib_mem_create_heap_internal` | `create_mspace_with_base` 在给定 range 内建 heap；`MemHeap` 等价控制块再从该 mspace 自身分配；heap 列表扩容时临时切回 main heap | pvt/data heap 的控制块在自身映射内，attach 不重建 |
| V6 | `src/vppinfra/mem_dlmalloc.c:363-383`、`:446-473`、`:519-537` | alloc/realloc/free 没有显式 heap 时取 active heap；free 断言对象属于该 heap | Rust/C 普通 allocation family 必须共用 active selector；错误 active heap 是违规，不自动猜 owner |
| V7 | `src/vppinfra/dlmalloc.c:4012-4032`、`:4054-4065` | mspace allocator state 位于 supplied base；locked mspace 的锁也在该 state；range 标为 external | fixed-VA attach 后可直接继续使用 shared mspace state |
| V8 | `src/svm/svm_common.h:19-40` | region header 保存 `virtual_base`、`region_heap`、`data_base`、`data_heap`；pvt heap 默认 128 KiB | Hammer header 保留 pvt/data heap 指针和固定 VA，不把它们改成 offset descriptor |
| V9 | `src/svm/svm.c:434-531`，`svm_region_init_mapped_region` | creator 在 `baseva + page_size` 创建 locked `region_heap`，切到它分配名字、pid vec、bitmap；之后按 flags 创建可选 data heap | region 初始化必须先有 pvt heap，所有 region metadata 由它分配 |
| V10 | `src/svm/svm.c:537-610`、`:654-704`，`svm_map_region` | creator 用 `MAP_SHARED|MAP_FIXED`；attach 先探测一页取得 creator 发布的 VA/size，再在相同 VA 重映射完整 region | raw pointers 和 shared heap 依赖所有参与进程映射到同一 VA |
| V11 | `src/svm/svm.c:733-751`，attach branch | attach 持 region lock，切到 creator 的 pvt heap，追加 client pid，并只映射 data backing；不创建 pvt/data heap | attach 复用 creator 已发布的 `MemHeap` 控制块 |
| V12 | `src/svm/svm.c:770-818`，`svm_region_init_internal` | root 映射完成后，在 `NEED_DATA_INIT` 分支切到 pvt heap，分配 `svm_main_region_t`、name hash、root path，再令 `data_base = mp` | root 的 `data_base` 是 pvt heap 中的 main-region 对象，不是 data heap 起点 |
| V13 | `src/svm/svm.c:821-875`，root init callers | global root 使用 `SVM_FLAGS_NODATA`，不是 `SVM_FLAGS_MHEAP` | root 只有 pvt heap；不得为它虚构 data heap |
| V14 | `src/svm/svm.c:879-975`，`svm_region_find_or_create` | root pvt heap 中的 bitmap 选择连续 VA，subregion 映射到 `root_base + page_index * page_size`；名字表值是 subregion pool index | root 继续拥有 VA 切分与名字注册，不使用任意 VA 的独立 region id |
| V15 | `src/svm/svm.c:252-331`、`:335-406` | 只有 `SVM_FLAGS_MHEAP` 创建 data heap；attach 的 map path不重建它 | data heap 是可选 `MemHeap`，与永远存在的 pvt heap不同 |
| V16 | `src/svm/svm.h:20-69`，`svm_mem_alloc` / `svm_push_pvt_heap` / `svm_push_data_heap` | pvt/data helper 都通过 `clib_mem_set_heap` 切换 active heap；data alloc/free 还持有 region mutex | `SvmRegion` 只授予受 region lock 约束的 pvt/data active scope |
| V17 | `src/svm/svmdb.c:70-120`、`:185-214` | DB 初始化把共享 header、Hash 和 Vec 放进 active data heap；后续 mutation 同样先切 data heap | active heap 必须覆盖普通 collection 的隐式分配，不只覆盖手写 raw allocation API |

`src/svm/svm_test.c` 是旧调用签名的 smoke program，没有覆盖当前
`svm_region_init_internal` 的 creator/attach、pvt/data heap 和失败原子性。本文第 10 节的
subprocess 测试主要从上述实现路径推导，不声称 VPP 已提供对应测试。

### 2.2 当前 Hammer

| ID | 路径与符号 | 已核实行为 | 差距 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-infra/src/main_heap.rs` | 一个固定容量 mimalloc arena；`GlobalAlloc` ready 前走 System，ready 后直接 `mi_malloc_aligned`；无 active heap | 普通分配只能进入 process Main Heap |
| H2 | `crates/hammer-infra/src/heap.rs` | crate-private `Heap` 用私有 vtable 在 main heap 与 creator-only `SegmentMapping` 间选择；不参与全局 allocator | 不是 `MemHeap`，attached region 被拒绝，不能提供 VPP active semantics |
| H3 | `crates/hammer-infra/src/svm/region_heap.rs` | 自研 `SvmRegionHeap` 是 offset block allocator | VPP 没有该类型或该 offset heap |
| H4 | `crates/hammer-infra/src/svm/region.rs` | `SvmRegionHeader` 内嵌 `metadata_heap`/`data_heap` 两个 offset descriptor，所有位置为 payload offset，支持不同 VA attach | 与 V8-V16 的 shared `MemHeap`、raw pointer、同 VA 契约相反 |
| H5 | `crates/hammer-infra/src/svm/hash_map.rs` | `SvmHashMap` 专为 offset heap 实现 | VPP 通过 active pvt/data heap让普通 Hash/Vec 分配，没有对应 SVM hash 类型 |
| H6 | `crates/hammer-infra/src/svm/ssvm.rs` | `SvmRegion` 被放进 `SsvmPrivate` payload；requested VA 为 0 时允许任意地址 attach | VPP `svm_region` 自己映射 region，不是 `ssvm_private_t` payload |
| H7 | `crates/hammer-runtime/src/start_workers.rs:25-55`、`worker_thread.rs:107-185` | thread zero 构造 worker `DataPlaneMain` 后 move 到 worker OS thread | 这不妨碍 allocator-owned TLS；只有把 `MemThreadMain` 错放进 `DataPlaneMain` 才会制造冲突 |
| H8 | 修正前的 `CONTEXT.md` 与 ADR-0011 | 把 offset heap、不同 VA attach、独立 subregion 和 `SvmRegionHeap` 记录成目标语言 | 已从规范删除；当前代码仍须按本文迁移 |

### 2.3 mimalloc 与 Rust FFI

当前 `libmimalloc-sys 0.1.49` 内置 mimalloc `3.3.2`
（`c_src/mimalloc/v3/include/mimalloc.h:11`）。C header 声明但 Rust binding 缺少：

| C API | `libmimalloc-sys` binding | 调研结果 | 本设计是否依赖 |
| --- | --- | --- | --- |
| `mi_heap_main` | 缺少 | 3.3.2 与 3.5.1 都实现 | 是，取得 process main heap |
| `mi_heap_theap` | 缺少 | 3.3.2 与 3.5.1 header 都声明 | 否，`MemThreadMain` 是唯一 active selector |
| `mi_theap_get_default` | 缺少 | 3.3.2、3.5.1 实现 | 否 |
| `mi_theap_set_default` | 缺少 | 3.3.2 header 声明但 `src/theap.c` 无实现；3.5.1 在 `src/theap.c:182-188` 实现 | 否，不能用双重 TLS authority |
| `mi_heap_contains` | 缺少 v3 binding | 3.3.2 与 3.5.1 都实现 | 是，诊断 process heap provenance |

mimalloc 不能作为本设计的 shared pvt/data heap backend：

- `mi_manage_os_memory_ex` 只把一段 memory 注册成当前进程的 arena；
- `mi_heap_new_in_arena` 在 `src/heap.c:128-154` 从当前进程 main heap 分配
  `mi_heap_t`，且为它创建 process-local dynamic TLS slot；
- exclusive arena 的 `mi_theap_t` 虽可在 arena 中分配，但
  `src/theap.c:308-340` 仍把它连接到当前进程的 heap、TLD 和链表；
- `mi_arena_unload/reload`、`mi_theap_unload/reload` 只存在于注释掉的
  experimental 代码（`include/mimalloc.h:434-442`、`src/arena.c:2655-2763`、
  `src/theap.c:475-533`），不是可链接或受支持的 API；
- 现有代码没有并发多进程 attach/reopen 一个 live heap 的契约。

因此“给 `libmimalloc-sys` 补几个 declaration 后，SVM 也直接使用 mimalloc”是被拒绝的
方案。mimalloc 继续服务 process heap；shared pvt/data heap 使用 VPP 同类 mspace。

## 3. 三方语义比较

| 维度 | 当前 Hammer | 目标 Hammer | VPP |
| --- | --- | --- | --- |
| process authority | `main_heap.rs` 的分散 atomics | `MemMain` 集中拥有 main heap、heap/thread inventory 和现有固定 arena | `clib_mem_main_t` |
| thread state | 无 Hammer active selector | allocator-owned `MemThreadMain` TLS，默认 main heap | `__thread clib_mem_thread_main_t` |
| ordinary allocation | ready 后固定 mimalloc main | Rust GlobalAlloc 与 C malloc family读取同一 active heap | `clib_mem_alloc/free/realloc` 取 active heap |
| process heap backend | mimalloc fixed arena | 保留 mimalloc | dlmalloc mspace；有意内部差异 |
| SVM heap backend | offset `SvmRegionHeap` | fixed-base locked mspace，公开身份仍是 `MemHeap` | locked mspace |
| region mapping | `SsvmPrivate` payload，可不同 VA | `SvmRegion` 自己拥有 fixed-VA map；attach 同 VA | `svm_map_region` |
| root layout | header 后一个 metadata offset heap | page 0 header，page 1 起 pvt heap，`data_base` 指向 pvt 中 `SvmMainRegion` | `svm_region_init_internal` |
| ordinary shared collections | `SvmHashMap` + offset arrays | 同构建版本的 `Vec`/`HashMap`/`Pool` 在 active pvt/data heap 分配 | clib vec/hash/pool 在 active heap 分配 |
| subregion location | 独立任意 VA + monotonic id | root bitmap 分配 VA；backing fd 仍可经 Hammer rendezvous 交付 | root bitmap + named shm |
| shared pointer contract | mapping-relative offset | region 内 raw pointer；同 arch、同 build、同 VA | raw pointer + same VA |

## 4. 最终决策

### D1：三个内存 owner 的名字与归属

`hammer-infra::mem` 新增且只新增三个核心 owner 名字：`MemMain`、
`MemThreadMain`、`MemHeap`。`MemMain` 是 process-global authority；
`MemThreadMain` 是 allocator-owned OS-thread state；`MemHeap` 是可被设为 active 的
具体 heap。`MemHeap` 不公开 trait、手写 vtable、`dyn` 或 backend enum。

`MemMain` 私有字段至少包含 main heap、已创建 heap inventory、已登记 runtime thread
inventory，以及现有 main-heap arena/base/capacity/init state。`MemThreadMain` 的有效字段是
`active_heap`、`thread_index` 和 process-lifetime thread inventory link；heap trace 不在本次
范围。`MemHeap` 私有记录 base、size、locked、name 和 backend state。

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

runtime thread 在自己的 OS-thread entry 登记 `thread_index`；未登记的外部线程仍可懒初始化，
其 index 为保留值且 active heap 为 main heap。登记只允许一次。runtime Data Workers 为
process lifetime，thread inventory 不提供 restart 或跨线程 mutable access。

### D3：普通 Rust/C 分配都走 active heap

`GlobalAlloc::{alloc,alloc_zeroed,dealloc,realloc}` 与当前 interposed
`malloc/calloc/free/realloc/aligned_alloc/posix_memalign/...` 先取得 active `MemHeap`，
再调用同一个 backend dispatch。Rust 和 C 不再各自维护默认 heap。

ready 前的 bootstrap System allocation 继续受支持。每个经该 allocation seam 返回的 block
带一个私有 `AllocationHeader`，记录 bootstrap 或 `MemHeap` provenance、backend 原始指针和
请求事实；user pointer 仍满足原 alignment。ready 后释放 bootstrap block 仍回到 System。
普通 managed block 的 header owner 必须等于当前 active heap，否则立即 abort；不得扫描其它
heap 后静默释放。这保留 VPP “free 使用 active heap”的错误检测，同时避免把 SVM block
误送给 System。header 是 allocator 私有布局，不进入公开 API。

OOM 对 Rust GlobalAlloc 返回 null，由 Rust allocation failure boundary 终止；C malloc-family
保持 C ABI 的 null/`ENOMEM`。显式 `MemHeap` nullable API 可返回 null。heap corruption、
provenance header corruption 和 wrong-active-heap free 是 programmer/shared-state violation，
立即 abort，不进入 `SvmRegionError`。

### D4：process heap 保留 mimalloc，shared heap 使用 mspace

`MemHeap` 有两个私有 backend：

- process main 与以后明确创建的 process-private heap：mimalloc first-class heap；
- SVM pvt/data heap：从 VPP 所用 public-domain dlmalloc 2.8.6 提取的 mspace，
  `create_mspace_with_base(base, size, locked)`，随后 disable expand。

backend 只在 `MemHeap` 内静态 match，不把函数指针放进 shared memory。mspace source 和窄 C
ABI 后续落到 `third_party/dlmalloc/`；`third_party/vpp` 仍是参考树，不作为隐藏的 build-time
dependency。shared heap 一律 `locked = true`，与 VPP 一致；region mutex保护复合 metadata
mutation，mspace 自身锁保护 allocator state。

`third_party/mimalloc_rust/libmimalloc-sys` 只补本设计实际使用的 v3 binding：
`mi_heap_main` 和 `mi_heap_contains`。表中其余缺口保留为调研记录，不因为“已经声明”就加入
Hammer API，也不把 unsupported `mi_theap_set_default` 作为 active selector。mimalloc 与
mspace 都不能在耗尽时向 SVM 外扩展或回落 main heap。

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

### D7：root init 精确对应 `svm_region_init_internal`

root region 使用 `NODATA`/subdivided flag，不创建 data heap。creator map path先创建 pvt
heap，并把 `NEED_DATA_INIT` 作为内部未完成状态。`svm_region_init_internal` 对应的 Hammer
owner随后持锁、清 pending flag、激活 pvt heap，并在其中构造：

- `SvmMainRegion`；
- `HashMap<String, u32>` 名字表；
- `Pool<SvmSubregion>`；
- Hammer rendezvous 所需但不进入 shared pointer layout 的 process-private fd table。

shared `SvmMainRegion` 不保存 process-private fd。构造完成后，header 的 `data_base` 指向
pvt heap 中的 `SvmMainRegion`。这条指针赋值不是 data heap，也不是 data-section offset。

Hammer 不移植 VPP 的 `root_path`/uid/gid 文件路径策略，因为当前产品通过 Unix socket
鉴权并用 `SCM_RIGHTS` 交付 fd；这是 transport divergence，不改变 pvt heap、bitmap 或
fixed-VA 语义。

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
version、只 warning data setup error 的行为相比，Hammer 有意把 version 作为最后一次
Release publish，失败绝不发布半初始化 region。

### D9：共享对象使用普通 collection，但不获得普通 Rust owner lifecycle

`SvmMainRegion`、region name、client pid list、bitmap、HashMap/Pool backing allocations 都由
active pvt heap提供；data-heap user objects同理。这些对象位于 fixed-VA mapping，内部 raw
pointer 在 peer 中保持同值。

进程本地 `SvmRegion` 只借用 shared object，不按值拥有它们。unmap 不运行整个 shared object
graph 的 Rust `Drop`；删除单个 name/member/value 时，必须在对应 heap active 的情况下执行
该对象的正常 drop。最后 region teardown 先使名字不可发现并等待使用者退出，再 unmap；
mapping 消失即同时结束 pvt/data heap storage 生命周期。

### D10：`DataPlaneMain` 构造不因 allocator TLS 改动

`MemThreadMain` 不放进 `DataPlaneMain`、`WorkerThread` 或共享 `Vec`，所以 ADR-0009 的
worker `DataPlaneMain` 预构造并 move 到 OS thread 与本文没有 ownership 冲突。worker closure
在执行可能切 heap 的 worker-init 前登记 allocator TLS；main thread在 `MemMain` 初始化时
登记 index 0；auxiliary runtime thread在自己的 entry登记。

如果后续设计要求 `DataPlaneMain` 按值拥有 `MemThreadMain`，那是不同设计，届时必须先修改
ADR-0009 和 worker construction。本文不做这个修改，也不为一个不存在的字段重排 worker
启动链。

## 5. 所有权、同步、发布与回收

| 状态 | Owner | Writers/readers | 同步与发布 | 回收 |
| --- | --- | --- | --- | --- |
| `MemMain` | process `hammer-infra` | startup写；allocator/diagnostics读 | existing one-time init Release/Acquire；inventory mutation是 control-plane bounded lock | process exit；main heap不提前销毁 |
| `MemThreadMain` | 当前 OS thread 的 allocator TLS | 仅当前 thread写/读；`MemMain` inventory只诊断 | TLS program order；无跨线程借用 | runtime thread process-lifetime；非 runtime thread由 TLS destructor清私有登记，不触碰 heap |
| process `MemHeap` | `MemMain` | 所有 thread经各自 active selection使用 | mimalloc thread-safe heap | process exit；live allocation存在时不得 destroy |
| pvt/data `MemHeap` | owning `SvmRegion` mapping | attached processes在 region contract下使用 | locked mspace；复合 region metadata另持 process-shared region mutex | region不可发现且无成员后随 mapping消失；attach不 destroy |
| active selection | 当前 `MemThreadMain` | 当前 thread唯一 writer/reader | `ActiveHeap` RAII nested restore | scope结束恢复 previous heap |
| root registry/bitmap | root region pvt heap | region owner和attached clients | root region mutex；version最终 Release publish | 名字先删除，bitmap后释放，backing最后关闭 |

不得在 active selector周围添加 `Mutex`、`RwLock`、atomic pointer publication 或
WorkerBarrier。WorkerBarrier不覆盖外部进程，不能证明 SVM heap安全。

## 6. 错误与失败原子性

新增的 `MemError` 归 `hammer-infra::mem`，只覆盖创建/初始化时可恢复、且调用者能采取动作的
失败：invalid size/alignment、mapping/reservation、mimalloc arena registration、mspace
creation、already initialized、fixed-address unavailable、unsupported platform。底层 OS error
保留 `#[source]`；mimalloc numeric status保留原值。

以下不是 `MemError`：

- active heap OOM：Rust allocation boundary终止，C ABI返回 null/`ENOMEM`；
- wrong active heap、double free、corrupt allocation header或mspace corruption：abort；
- SVM version/layout/base不匹配：`SvmRegionError`；
- region backing fd/map失败：`SvmRegionError` 保留 OS source；
- fixed VA冲突：attach/create失败且不覆盖现有 mapping。

初始化顺序为 reserve/map -> backend create -> private owner fields -> shared metadata ->
version Release。version之前的失败逆序关闭 fd/unmap/release reserve；mimalloc一旦接受无法安全
unregister的 arena，状态转为 failed且禁止重试，沿用当前 main heap policy。cleanup error可见，
但不替换 primary error。

## 7. 层隔离契约

| 层 | 允许调用 | 禁止调用/保存 | 验证 |
| --- | --- | --- | --- |
| `mem` | mimalloc process backend、mspace shared backend、System bootstrap、allocator TLS | SVM name/member policy、runtime graph/plugin状态、caller closure | infra allocation tests、C ABI tests、wrong-heap subprocess |
| `SvmRegion` | mapping/fd、region mutex、`MemHeap`、standard collection/Pool、root bitmap | `SsvmPrivate` header、offset heap、runtime WorkerBarrier、fallback Main Heap | same-VA subprocess creator/attach |
| `RegionLock` | 直接借 pvt/data `MemHeap` 并创建 `ActiveHeap` | 返回越过 lock lifetime 的 heap借用、跨 `.await` scope | compile-time lifetime用例、nested restore行为 |
| runtime thread entry | 登记当前 thread index，之后正常构造/运行 worker state | 拥有或跨线程 move `MemThreadMain`、为 allocator添加第二个 TLS | startup integration |
| App/session rendezvous | 交付 fd、base、size和协议版本 | 把 fd/进程本地表写入 shared `SvmMainRegion` | multi-process attach |

## 8. 变更清单

### 8.1 新增或修改，需批准

| ID | 类型/API | Owner / consumer | 最终结果与现有接口不足 |
| --- | --- | --- | --- |
| A1 | `MemMain`；`init`、`init_with`、`main_heap`、只读 diagnostics | `hammer-infra::mem`；daemon/ctl startup | 收拢当前 `main_heap.rs` atomics与 heap/thread inventory；现有 module没有 active heap owner |
| A2 | `MemThreadMain`；current-thread registration、`active_heap` | allocator TLS；runtime thread entries | `GlobalAlloc` 无 owner参数，必须有 VPP 等价 current-thread selector；批准包含 D2 的窄 TLS规则例外 |
| A3 | `MemHeap`；`create_at`、`allocate`、`allocate_zeroed`、`reallocate`、`deallocate`、`contains`、`base`、`size` | `MemMain`、SVM region、现有 explicit allocator consumers | 统一 process heap和pvt/data heap身份；替代 crate-private `Heap` 和 `SvmRegionHeap` |
| A4 | `ActiveHeap<'a>`；`MemHeap::activate` | 需要临时切换的 region/allocator caller | 保存并 RAII恢复 previous heap；不能用 closure或公开 raw setter表达 unwind-safe nested scope |
| A5 | `MemError` concrete variants | `hammer-infra::mem`；startup/region create seam | 合并当前 `MainHeapError` 并增加 mspace/fixed-map初始化错误；allocation violation不进入该 enum |
| A6 | `SvmRegionHeader` 的 `virtual_base`、`pvt_heap: *mut MemHeap`、`data_base`、`data_heap: *mut MemHeap`、bitmap/member/name fields | `hammer-infra::svm::region` | 恢复 VPP pointer/fixed-VA layout；删除两个内嵌 offset heap descriptor |
| A7 | `SvmRegion` creator/attach/find-or-create/unmap 与 `RegionLock::{pvt_heap,data_heap}` direct borrows | infra SVM consumers | region自己拥有 fixed-VA mapping并授予受锁约束的 heap borrow；不再嵌套 `SsvmPrivate` |
| A8 | `SvmMainRegion`、`SvmSubregion` 使用 `HashMap`/`Pool`/bitmap 的 active-pvt allocation | root region | 恢复 `svm_region_init_internal` 和 root VA切分；替代 `SvmRegionMain` 的 monotonic id模型 |
| A9 | `third_party/dlmalloc` mspace narrow C ABI + build integration | `hammer-infra::mem` private backend | mimalloc没有 shared live-heap attach；当前 offset allocator不属于VPP |
| A10 | `third_party/mimalloc_rust/libmimalloc-sys` 的 v3 `mi_heap_main`、`mi_heap_contains` bindings与path pin | process `MemHeap` backend | C已有实现而Rust binding缺失；不新增未使用的 theap API |
| A11 | private `AllocationHeader` 与统一 Rust/C routing | global allocator/interposition | 区分 late bootstrap与managed heap，并在跨 backend free前验证 active owner |

以上批准不自动授权公开 backend enum、allocator trait、generic TLS container、closure access、
`dyn` dispatch、不同 VA兼容模式或 mimalloc shared-heap fork。

### 8.2 修改调用方

| Surface | 处理 |
| --- | --- |
| `main_heap.rs` 和 startup callers | 状态迁入 `mem::MemMain`；`PageSize` 保留领域含义但由 `mem` 导出；所有 daemon/ctl/test startup调用迁移 |
| `heap.rs`、`heap_boxed.rs`、Bihash explicit allocator | crate-private `Heap` 改为直接借 `MemHeap`；不保留 vtable或 `Heap::svm_data` |
| Rust `GlobalAlloc` 与 C interpose | 改读 `MemThreadMain.active_heap`；bootstrap/managed header统一处理 |
| runtime main/worker/aux thread entry | 在各自 OS thread登记 index；不改变 `DataPlaneMain` ownership |
| `svm/region.rs` | 按 D5-D9 重写 mapping/header/root/subregion/heap操作；layout version bump，拒绝旧 layout |
| SVM region consumers | offset变成 same-VA pointer；mutation在 RegionLock + ActiveHeap scope内使用普通 collections |

### 8.3 删除

| 删除项 | 原因 | 迁移证明 |
| --- | --- | --- |
| `svm/region_heap.rs` 全文件、`SvmRegionHeap`、`SvmRegionHeapViolation` | VPP没有 offset heap；pvt/data 都是 `MemHeap` | type/API调用者归零，region subprocess走shared mspace |
| `svm/hash_map.rs` 全文件及全部 `Svm*Entry/Iter/Keys/Values` | active pvt heap已让普通 HashMap分配在region | 名字插入/查询/删除/rehash跨进程通过 |
| `SvmRegionHeader::{metadata_heap,data_heap: SvmRegionHeap,data_base_offset,...}` 的 offset layout | 与 VPP fixed pointer ABI相反 | layout probe和same-VA attach通过；旧版本明确拒绝 |
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
3. **用 mimalloc exclusive arena直接 attach。** 现有 supported API没有 shared live heap
   reopen，`mi_heap_t`/TLS/TLD仍是process local。
4. **启用 mimalloc中注释掉的 reload代码。** 该代码不是exported contract，也没有并发
   multi-process correctness证据；不能在本 ADR中冒充已支持能力。
5. **process heap也全部替换成dlmalloc。** 能获得单一 backend，但无必要地改变当前
   mimalloc性能与interposition基础；VPP alignment要求heap semantics，不要求复制同一
   process allocator实现。
6. **GlobalAlloc显式传 allocator或只给SVM raw API。** 标准 Vec/HashMap和第三方分配不会
   收到该参数，无法满足 active heap。
7. **把 `MemThreadMain` 放入 `DataPlaneMain`。** allocator在DataPlaneMain构造前、auxiliary
   thread和外部thread也会运行；这会制造不必要的Send/启动冲突。
8. **按 pointer range猜 owner并自动 free。** 破坏VPP active-free invariant，也会把
   wrong-scope SVM drop隐藏成成功。

## 10. 验证矩阵

测试遵守仓库 final pre-commit gate：实现、review和格式完成前不运行测试。

| Test | 层级与设置 | 必须断言 | 决策/证据 |
| --- | --- | --- | --- |
| `mem_main_initializes_once` | infra unit/integration | fixed capacity、main heap published一次；不同配置返回concrete `MemError`；失败不部分publish | D1/D4，V2/V5 |
| `thread_defaults_to_main_heap` | real OS threads | main、Data Worker、aux和未登记thread首次分配都落main；登记index正确 | D2/D10，V1-V4 |
| `active_heap_restores_nested_scopes` | infra unit | main -> heap A -> heap B -> A -> main；early return/unwind也恢复；token不可跨thread | D2，V4 |
| `rust_and_c_allocations_use_active_heap` | Rust + linked C probe | Vec/String/HashMap与malloc/calloc/aligned_alloc都落当前heap，alignment/zeroing正确 | D3，V6/V17 |
| `wrong_active_heap_free_aborts` | subprocess | 在heap A分配、heap B active时drop，进程abort并输出owner facts；不调用System/其它heap | D3，V6 |
| `bootstrap_block_frees_after_mem_main_init` | subprocess | ready前System block在ready后正常free/realloc；managed block不误判bootstrap | D3，H1 |
| `shared_mspace_uses_supplied_range` | C ABI + Rust integration | `MemHeap`控制块与所有block都在base/size内；disable expand；耗尽不回落main | D4/D6，V5/V7 |
| `root_init_allocates_main_region_from_pvt_heap` | infra subprocess | root无data heap；name/pids/bitmap/`SvmMainRegion`都由pvt heap拥有；`data_base`等于该对象pointer | D6/D7，V9/V12/V13 |
| `subregion_data_heap_follows_flag` | infra integration | 每个region有pvt heap；仅DATA_HEAP flag产生data heap；无flag时绝不fallback | D8，V15/V16 |
| `region_attach_reuses_heaps_at_same_va` | independent exec processes | peer先probe再映射同VA；pvt/data `MemHeap` pointer值相同；两进程交替allocate/free可见；attach没有create/reload | D5-D8，V10/V11 |
| `occupied_fixed_va_rejects_attach_atomically` | subprocess预占目标range | 返回fixed-address error，预占mapping未被覆盖，region/client list不变 | D5/D8，V10 + Hammer safety divergence |
| `root_bitmap_assigns_subregion_va` | multi-process integration | 连续page选择、名字幂等、pool index、删除后bitmap释放；subregion base由root计算 | D7/D8，V14 |
| `region_layout_rejects_old_offset_version` | attach integration | legacy offset region layout被version/fingerprint拒绝，未触碰旧heap字段 | D5，H3-H6 |
| `shared_collections_mutate_under_active_heap` | multi-process integration | Vec/HashMap/Pool grow、remove、drop均在pvt active；peer看到相同内容；unmap不运行whole-graph Drop | D6-D9，V17 |
| `allocator_ffi_symbols_link` | compile/link C probe | selected mimalloc v3 symbols和mspace ABI真实export并可调用；不靠header-only声明 | D4，Mimalloc ledger |
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

## 11. 批准项与结论

需要用户逐项批准 A1-A11，特别是：

- `MemMain` / `MemThreadMain` / `MemHeap` 命名与唯一 active authority；
- A2 的 allocator-owned C TLS 窄例外；
- process mimalloc + shared mspace 的双 backend 内部实现；
- SVM region 同 VA/raw-pointer ABI，以及 root bitmap VA切分；
- 删除已实现的 `SvmRegionHeap`、`SvmHashMap` 和不同 VA layout，不保留兼容层；
- `third_party/dlmalloc` 与 patched `third_party/mimalloc_rust` 的依赖变更。

设计 verdict：**Aligned，待批准**。active heap、pvt/data heap、root init和attach ownership与
vendored VPP一致；保留 mimalloc作为process backend、使用safe fixed-map而不是覆盖式
`MAP_FIXED`、通过SCM_RIGHTS交付backing fd、以及version最后发布是明确的Hammer差异，均有
对应验证。当前代码仍是H1-H6描述的旧实现，不能因本文完成而声称功能已对齐。

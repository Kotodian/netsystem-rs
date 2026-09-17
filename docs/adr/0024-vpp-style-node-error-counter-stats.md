# ADR-0024: Node Error Counter Stats 的注册、更新与 Stats Client 集成（`/node/errors` 与 `/err/<node>/<error>`）

Status: proposed
Date: 2026-09-17

> **前置依赖（已落地）：** ADR-0019（stats segment 所有权、目录 ABI、`validate`/symlink 机制）、
> ADR-0021（家族声明、owner 采集者登记、无锁轮次、stats client 分层契约）、ADR-0023（`/sys/node/*`）。
>
> **与 ADR-0023 的关系：** node counters 走 VPP 的采集者路径——worker 私有计数 + collector process
> 投影到 `/sys/node/*`（`third_party/vpp/src/vlib/stats/collector.c:154-176`）；**error counter 不走这条
> 路径**：计数器本身就在 stats 段内，记录点写的是本线程自己那一行
> （`third_party/vpp/src/vlib/error_funcs.h:24-37`）。本家族按 VPP 的形状单独设计。
>
> 本文只出设计：不写代码、不执行测试。§11 列出的每个新类型/方法/字段都是待批准项。

## 1. 形状与目录契约

| 目录名 | 类型（`DirectoryType`） | 形状 | 内容 |
| --- | --- | --- | --- |
| `/node/errors` | `CounterVectorSimple` | 行 = 线程（thread zero + Data Worker），列 = 全局错误槽 | 每线程累计错误计数 |
| `/err/<node>/<error>` | `Symlink` | 指向 `/node/errors` 第 `<全局槽号>` 列 | 该 `(node, error)` 的每线程计数 |

- **名字逐字采用 VPP**：`vlib_stats_add_counter_vector ("/node/errors")`
  （`src/vlib/error.c:158-159`）与
  `vlib_stats_add_symlink (em->stats_err_entry_index, n->error_heap_index + i, "/err/%v/%U", …)`
  （`src/vlib/error.c:182-186`）。不要改成 `/sys/node/errors`，也不要给 `/err/*` 加第二套名字向量。
- **没有开关。** VPP 的 error counter 不受 `node_counters_enabled` 影响：该开关只包住 collector
  process 建的 `/sys/node/*`（`stats/collector.c:154-176`）。`per_node_counters = false` 时 error 家族
  仍然完整存在。
- **惰性创建。** 只有至少一个节点声明了 ≥1 个错误时 `/node/errors` 才存在（`error.c:135` 的
  `n_errors == 0` 早退 + `error.c:158-159` 的创建）。没有错误槽的进程里目录中根本没有这个条目；
  client 读作"未发布该家族"，不是"计数为 0"。
- **列 = `NodeErrorIndex` 的值**（identity 映射）。VPP 里 buffer 携带的 `b->error` 就是列号：
  `drop_error = n->error_heap_index + drop_error_code; b0->error = drop_error;`
  （`src/vlib/error.c:26,52-55`），反推 node/code 也证明这点
  （`e - n->error_heap_index`，`src/vlib/node.h:799-812`；字段定义在 `src/vlib/buffer.h:104-105`）。
  Hammer 的 `NodeErrorIndex` 也是这个全局值，记录路径不做任何偏移。
- **列空间来自 VPP 的 heap**：VPP 为节点分配一段连续列区间的方法是
  `n->error_heap_index = heap_alloc (em->counters_heap, n_errors, n->error_heap_handle)`
  （`src/vlib/error.c:138-139`），字段注释写明用途："Counter structures in heap. Heap index
  indexes counter vector."（`src/vlib/error.h:38-39`）。Hammer 移植同一个数据结构的分配侧，
  列 = heap offset + 1（§3.1、§4）。
- **列 0 = 全局 no-error 哨兵**（`NodeErrorIndex` 是 NonZeroU16，0 表示无错误，
  `crates/hammer-core/src/graph/error.rs:6-8`）。列 0 不属于 heap：有效列是
  `1..=heap.len()`，列宽 = `heap.len() + 1`。VPP 的每节点枚举同样用 code 0 表示 `no error`
  并把它发布成 `/err/<node>/no_error`（`src/vnet/ethernet/error.def:40` 等）——区别只是 VPP 的
  no-error 槽是**每节点**一个，Hammer 的全局索引空间只有列 0 一个（§8 差异 3）。
- **行 = thread zero + Data Workers，行号 = `thread_index`**（`0..=worker_count`，与 ADR-0023 相同）。
  这不是新约定：VPP 也是用线程号取行——`vm_clone->error_main.counters = c[vm_clone->thread_index]`
  （`src/vlib/threads.c:680,772-773`）、`tem->counters = sc[i]`（`error.c:166-170`）。其它 runtime
  线程没有节点图，不占行。
- **声明错误的节点必须有名字**：`/err/<node>/<error>` 两段都来自注册名；无名节点（只在测试/bench
  出现）建不出别名。名字里不得含 `/`，否则别名路径无法解析；这是注册期 bug，用带 node/descriptor
  事实的断言终止（client 侧不做这类校验，见 §9）。长度不用自己查：机制层的名字上限
  就是 VPP 的 128 字节（`stats/shared.h:37` 的 `VLIB_STATS_MAX_NAME_SZ`；hammer-stats
  `protocol.rs:5` 的 `MAX_NAME_BYTES`），超长在创建名字时就会失败。

## 2. 归属：每线程 main 上的 `error_main`（不是 `NodeMain`）

VPP 的归属证据：

| 事实 | 位置 |
| --- | --- |
| `vlib_main_t` 里 `vlib_node_main_t node_main` 与 `vlib_error_main_t error_main` 是**兄弟**成员 | `src/vlib/main.h:134,140` |
| `vlib_error_main_t { u64 *counters; u64 *counters_last_clear; vlib_error_desc_t *counters_heap; u32 stats_err_entry_index; }` | `src/vlib/error.h:30-44` |
| 注册函数以 main 为参数，同时写 `vm->error_main` 与 `vm->node_main` 下的节点身份 | `src/vlib/error.c:113-141` |
| 记录点写行：`em->counters[counter] += increment` | `src/vlib/error_funcs.h:24-37` |
| 行指针的刷新点：注册时 `tem->counters = sc[i]`；worker clone `vm_clone->error_main.counters = c[worker_thread_index]`；refork 再取一次 | `error.c:164-170`；`threads.c:766-778`；`threads.c:930-944` |
| 目录条目数据是行指针向量（`u64 **data`），`validate` 逐行增长/替换 | `src/vlib/stats/stats.c:496-502`；`stats/stats.h:116-121` |

Hammer 的对应物是 `DataPlaneMain` ↔ `vlib_main_t`、`NodeMain` ↔ `vlib_node_main_t`（`DataPlaneMain` 与
`NodeMain` 的既有字段见 `data_plane/main.rs:33-42`、`node.rs:446-449`）。逐项对应：

| VPP | Hammer |
| --- | --- |
| `error_main.stats_err_entry_index`（error.h:43） | `DataPlaneMain.node_error_stats_entry_index: Cell<Option<DirectoryIndex>>` |
| `error_main.counters`（缓存的 `u64 *`，三个刷新点重取） | **不缓存地址**：记录点把 `(entry, thread_index, column)` 交给机制层，由 `hammer-stats` 内部解析行地址 |
| `em->counters[counter] += increment`（error_funcs.h:35） | `StatsSegment::increment_simple_counter (entry, row, column, increment)` |
| `error_main.counters_heap`（`vlib_error_desc_t *`：元素空间，heap index == 计数列号，error.h:38-39） | `NodeMain` 的 `Heap<NodeId>`（节点域，元素 = 列归属节点，§3.4）；列号 = heap offset + 1（§4） |

**为什么 Hammer 不缓存 `counters` 那样一个行地址（裸指针、`&'static [T]` 都不行）：**

1. `hammer-stats` 的既有契约就是"**句柄是 `DirectoryIndex`，寻址留在机制层**"：`set_gauge(index, …)`、
   `set_timestamp(index, …)`、`set_simple_counter(index, row, column, value)` 都只收索引，行/列的
   指针解析与形状断言都在机制层内部完成（`segment.rs:232-262`）；写入越界是 owner 的 bug，机制层
   **带事实断言**而不是返回 `Result`。给一个家族单独开一条"把行地址交给调用方"的口子，既多一套
   生命周期规则，也破坏这条契约。
2. VPP 之所以要三个刷新点，正是因为 `counters` 是**缓存地址**而 `validate` 会替换/释放行
   （`stats.c:496-502` 的 `vec_validate_aligned`；hammer-stats 同语义并释放被替换行，
   `segment.rs:676-706`）。Hammer 每次写都按 `(entry, row)` 解析，就没有"悬垂地址"这一整类问题，
   也不需要裸指针 + `unsafe impl Send`（`DataPlaneMain` 要移交进 Worker）。
3. 代价是每次写多一层目录/行解析（几次已发布内存的读，无锁、无分配）。错误计数不在成功路径上，
   这个代价可接受；VPP 省掉的正是这几次读。

**仍然保留的 VPP 语义**：行归该线程所有（`thread_index` 就是行号）、条目在第一个声明错误的节点上
惰性创建、列空间由注册顺序决定、buffer 里的错误值就是列号、别名与表都在段内、没有采集者参与。

**`counters_heap` 放在节点域（与 VPP 的一处结构差异）**：VPP 把它放在 `error_main`（`main.h:134,140`），
但只有 thread zero 的注册路径会分配它（`ASSERT (vlib_get_thread_index () == 0)`，`error.c:125`），
worker 的 `error_main` 只是拷贝、从不分配。Hammer 的"节点身份"（列 → 节点）本来就由 `NodeMain` 持有
（今天的 `error_indices`/`next_error_index` 就在这里），所以列空间分配器与它同属一个所有者；main 域
只保留本线程写计数需要的事实（条目索引 + 行号 = `thread_index`）。worker 克隆拿到只读副本，与 VPP
worker 持有同一 heap 的浅拷贝语义等价（两边都从不写）。差异记在 §8。

**仍然需要的一条纪律**：`validate` 只允许在"没有并发写入"时增长/替换行（机制层的 SAFETY 注释要求
调用方守启动期单写者纪律，`segment.rs:652-657,676-677`）。Hammer 的图在 Worker launch 前冻结，所以
冻结点之后不得再注册错误槽（§7）。

## 3. Rust 类型与方法

### 3.1 `hammer-infra`：`Heap<T>`（`vppinfra/heap` 的分配侧移植）

与 ADR-0012 的 `MemHeap` 无关：`MemHeap` 是 dlmalloc 内存堆，这里是 VPP `vppinfra/heap` 的
**offset 身份空间**（`third_party/vpp/src/vppinfra/heap.h:8-31`）。`hammer-infra` 今天没有这个模块
（`crates/hammer-infra/src/lib.rs:1-29` 的模块表里只有与它无关的 `heap_boxed`）。

```rust
// crates/hammer-infra/src/heap.rs（新模块；lib.rs 增加 pub mod heap;）
/// 一段只增的元素空间（VPP 的 `T *heap`），按区间分配；每次分配返回区间起始
/// offset（元素空间下标）。offset 是所有持有者眼里都稳定的身份——节点错误列号
/// 就建立在这个身份上（§4）。与 ADR-0012 的 `MemHeap`（dlmalloc 内存堆）是两回事。
pub struct Heap<T> {
    values: Vec<T>,
}

impl<T> Heap<T> {
    pub fn new() -> Self;

    /// VPP `heap_alloc (v, size, handle)`（heap.h:269-279）的分配路径
    /// （`_heap_alloc`，heap.c:353-466）：在元素空间尾部追加 `elements` 个
    /// `element`，返回区间起始 offset（元素空间下标，0 基；VPP `uword offset`，
    /// heap.h:60-63；`heap_elt_t.offset` 本身就是 u32）。
    /// `elements == 0` 是调用方 bug（VPP `_heap_alloc` 的 `size == 0` 早退，
    /// heap.c:361-362），按本地不变量断言。
    pub fn alloc(&mut self, elements: u32, element: T) -> u32
    where
        T: Clone;

    /// VPP `vec_len (em->counters_heap)`（error.c:140）：元素空间长度。
    pub fn len(&self) -> u32;

    pub fn is_empty(&self) -> bool;

    /// VPP `heap[offset]`（`elt_data` 的地址计算，heap.c:180-185）：读元素。
    pub fn get(&self, offset: u32) -> Option<&T>;
}
```

offset 就是元素空间下标，和 VPP 一样用裸 `u32`（`heap_elt_t.offset` 本身就是 u32，
`heap.h:33-63`），不包 newtype：它是 heap 内部的位置；领域身份是全局列号 `NodeErrorIndex`
（§3.4），两者只差 §1 的保留列 0——全局错误列号 = offset + 1。

**这里没有 `dealloc`/`free`。** 释放侧在 VPP 只服务 `vlib_unregister_errors`（`error.c:98-110`），
而 Hammer 今天没有单节点卸载：错误表的生命周期就是图的生命周期，重建时整个 `NodeRuntimeInner`
被替换（`crates/hammer-runtime/src/node.rs:1204-1240` 的 `detach_graph_for_rebuild`；状态替换在
:1223-1240），heap 随所有权一起 Drop。因此 free list、
size bins（`heap.h:86-88`）、`search_free_list`/`elt_delete`/`combine_free_blocks`/`dealloc_elt`
（`heap.c:77-140,250-352,494-566`）、`heap_dealloc`（`heap.c:467-491`）、`heap_alloc_aligned`
（`heap.h:269-277`）、`heap_dup`/`heap_free`/`heap_validate`/`format_heap`/`used_elt_bitmap`
（`CLIB_DEBUG` 专用）与静态 heap 都不移植：没有调用者的 API 不写。运行期 unregister 落地时，
区间归还与 free list 复用随它的第一个调用者一并批准（§14 项 1），形态是 Rust 所有权归还
（把区间值交回 heap），不是裸 handle。

### 3.2 `hammer-stats`（机制层唯一新增的段 API）

```rust
impl StatsSegment {
    /// VPP `em->counters[counter] += increment`（error_funcs.h:35）的机制实现：
    /// 解析已发布 simple counter vector 的第 `row` 行，把第 `column` 格加
    /// `increment`（relaxed 原子读改写，与 `set_simple_counter` 同一单元格纪律）。
    ///
    /// 形状必须已由 `validate` 发布；越界/类型不符是 owner 的 bug，按
    /// `set_simple_counter` 的同一纪律带 index/row/column 事实断言，不返回
    /// 新错误类别，也不扩大 `DirectoryEntry` 的公开面。
    pub fn increment_simple_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        increment: u64,
    );
}
```

这就是 `vlib_stats_get_entry_data_pointer (entry)[row]`（`stats/stats.h:116-121`）加一次
`fetch_add`，其中行解析逻辑与 `DirectoryEntry::set_simple_counter_cell`
（`protocol.rs:488-521`）共用；机制层不认识 node/error。

### 3.3 `hammer-runtime`：`DataPlaneMain` 的 per-thread error 状态

```rust
// crates/hammer-runtime/src/data_plane/main.rs
pub struct DataPlaneMain {
    // …既有字段…（thread_index: u32 在 main.rs:37）
    /// VPP `vlib_main_t.error_main`（main.h:140）里本线程需要的那个事实：
    /// `/node/errors` 的目录条目（`error_main.stats_err_entry_index`，error.h:43）。
    /// 行号不需要存：VPP 用 `vm->thread_index` 取行（threads.c:772-773），Hammer 同。
    /// 注册在 thread zero 上写、冻结期每个线程各自装一次（§7）。
    /// `Cell`：注册路径（`init_graph_from_declarations`）是 `&self` 方法，与
    /// `current_node: Cell<Option<NodeId>>`（main.rs:39）同一手法。
    pub(crate) node_error_stats_entry_index: Cell<Option<DirectoryIndex>>,
}

impl DataPlaneMain {
    /// 插件/服务唯一记录入口；签名与描述不变（既有 `data_plane/trace.rs:84-93`）。
    #[inline]
    pub fn record_current_node_error<E: NodeErrorCode>(
        &self,
        error: E,
    ) -> RuntimeResult<NodeErrorIndex>;

    /// VPP `vlib_register_errors`（error.c:113-200）的移植：节点域分配连续槽区间 →
    /// main 域惰性创建 `/node/errors`、`validate`、逐错误发 `/err/<node>/<error>`。
    pub(crate) fn register_node_errors(
        &self,
        node: NodeId,
        descriptors: &[NodeErrorDescriptor],
    ) -> RuntimeResult<()>;

    /// 冻结期装入本线程的条目（VPP threads.c:766-778 的
    /// `vm_clone->error_main.counters = c[worker_thread_index]`，Hammer 只需要条目：
    /// 行号 = `thread_index`）。`None` = 本线程不计数（没有错误槽的进程）。
    pub(crate) fn install_node_error_stats_entry(&self, entry: Option<DirectoryIndex>);
}
```

### 3.4 `hammer-runtime`：`NodeMain`（节点身份 + 列空间分配，不碰段）

```rust
// crates/hammer-runtime/src/node.rs
/// 一个节点占用的连续列区间（VPP `n->error_heap_index` + `n->n_errors`，
/// node.h:333,342）。`first` 已经是全局列号（= heap offset + 1）。
pub(crate) struct NodeErrorColumnRange {
    first: NodeErrorIndex,
    count: u16,
}
// 列空间本身：`error_column_heap: Heap<NodeId>`。元素只承载列归属，而归属
// 就是 `NodeId`（VPP 的元素是 `vlib_error_desc_t`：名字/描述/severity/symlink
// 索引，error.h:15-28；Hammer 这些都在静态 descriptor 里，元素没有读者）。

impl NodeMain {
    /// 节点域分配：`Heap::alloc (count, node)` 得到区间
    /// （VPP `heap_alloc (em->counters_heap, n_errors, …)` + `n->error_heap_index`，
    /// error.c:138-139）。既有 `materialize_node_errors` 按 VPP 的函数名
    /// （`vlib_register_errors`）改名为 `register_node_errors`；
    /// `NodeRuntimeInner` 的私有同名方法一并改；签名与语义不变。
    /// 插件不再直接调它：16 个调用点（`crates/hammer-plugins/net/{ip,icmp}`）
    /// 改成 `DataPlaneMain::register_node_errors`（§3.3），所以节点域的这份
    /// 收回到 `pub(crate)`——公开发布面只有 main 域那一个。
    pub(crate) fn register_node_errors(
        &self,
        node: NodeId,
        descriptors: &[NodeErrorDescriptor],
    ) -> RuntimeResult<()>;

    /// 已发布的列宽：没有错误槽时为 0，否则 = `heap.len() + 1`（含保留列 0；
    /// VPP `l = vec_len (em->counters_heap)`，error.c:140）。
    /// 注册期 `validate` 的入参与验收入口（`validate` 收的是最大列号，所以是 `columns - 1`）。
    pub(crate) fn node_error_columns(&self) -> u32;

    /// 本地 code → 全局列号（VPP `counter += n->error_heap_index`，error_funcs.h:32）：
    /// `range.first + code`，越界仍是既有的 `NodeErrorSlotOverflow`。
    /// 既有 `record_node_error` 的改名：它只解析身份，不再负责"记录"。
    pub(crate) fn node_error_index(&self, node: NodeId, code: u16) -> RuntimeResult<NodeErrorIndex>;
}
```

内部状态替换（净减两项）：

| 今天（`node.rs:448-450`） | 替换为 |
| --- | --- |
| `error_indices: Vec<Box<[NodeErrorIndex]>>`（每节点一张本地表） | `error_columns: Vec<Option<NodeErrorColumnRange>>`（每节点一个区间，`None` = 未注册） |
| `next_error_index: u32`（水位） | `error_column_heap: Heap<NodeId>`（列空间本身，元素 = 归谁） |
| `error_tables_installed: Vec<bool>` | 折进上面的 `Option` |

每个节点只存区间（起点 + 数量），不再存逐 code 的表：`record` 路径用 `first + code` 解析，
越界由 `count` 判定。

### 3.5 不改的既有 API

`record_current_node_error` 的签名、`set_index_node_error(runtime: &mut DataPlaneMain, …)`
（`hammer-service/src/data_plane.rs:8-24`）与全部插件调用点不变；`init_graph`/`init_graph_with_node_functions`/
`rebuild_graph`/`extend_graph_with_node_functions` 的公开签名不变；`NodeErrorCode`/`NodeErrorDescriptor`/
`NodeErrorIndex`/`NodeEntry.error_counters` 不变；`hammer-stats` 除 §3.2 外不加任何 surface。
唯一的既有名字改动是 §13 项 8 的内部改名（`materialize_node_errors` → `register_node_errors`、
`record_node_error` → `node_error_index`），签名与语义不变。

## 4. 错误槽索引空间：`vppinfra/heap` 的分配侧

### 4.1 VPP 事实

| 事实 | 位置 |
| --- | --- |
| `vlib_error_main_t` 的第三个字段是 `vlib_error_desc_t *counters_heap`，注释："Counter structures in heap. Heap index indexes counter vector." | `src/vlib/error.h:30-44`（注释 :38-39） |
| 注册：`n->error_heap_index = heap_alloc (em->counters_heap, n_errors, n->error_heap_handle)`；`n_errors` 就是连续列数 | `src/vlib/error.c:138-139` |
| 列宽 = 元素空间长度：`l = vec_len (em->counters_heap)` → `vlib_stats_validate (entry, n_threads - 1, l - 1)` | `src/vlib/error.c:140,163` |
| 列号 = offset：`cd = vec_elt_at_index (em->counters_heap, n->error_heap_index)`；`/err` symlink 指向 `n->error_heap_index + i` | `src/vlib/error.c:141,182-186` |
| buffer 携带同一个数字：`drop_error = n->error_heap_index + drop_error_code; b0->error = drop_error` | `src/vlib/error.c:26,52-55` |
| `heap_alloc` 返回区间起始 offset，并回写 handle（`*handle_return = e - h->elts`）；失败时两者都是 `~0` | `src/vppinfra/heap.c:458-465`；`heap.h:269-279` |
| 分配先查 size bin 的 free list，未命中才在尾部追加（`offset = vec_len (v)`） | `src/vppinfra/heap.c:354-400`；bins 定义 `heap.h:86-88` |
| 元素空间按 cache line 对齐（`HEAP_DATA_ALIGN = CLIB_CACHE_LINE_BYTES`） | `src/vppinfra/heap.h:126`；`heap.c:384-386` |
| 释放只发生在卸载：`vlib_unregister_errors` → `heap_dealloc (em->counters_heap, n->error_heap_handle)`；此外没有第二个调用者 | `src/vlib/error.c:98-110` |

### 4.2 Hammer 映射

| VPP | Hammer | 说明 |
| --- | --- | --- |
| `_heap_alloc`/`heap_alloc`（`heap.c:353-466`） | `Heap::alloc`（§3.1） | 元素空间尾部追加，返回区间起始 offset |
| `vec_len (em->counters_heap)`（`error.c:140`） | `Heap::len()` | 发布列宽 = `len() + 1` |
| `heap_offset`/`vec_elt_at_index` | `Heap::alloc` 返回的 `u32` offset / `Heap::get` | offset 是元素下标，不是指针 |
| `n->error_heap_index`（`node.h:333,342`） | `NodeErrorColumnRange.first` | 列起点（`NodeErrorIndex`） |
| `n->n_errors` | `NodeErrorColumnRange.count` | 区间长度 |
| `n->error_heap_handle` | **不保留** | handle 只被 `heap_dealloc`/`heap_elt_with_handle` 使用（`heap.h:249-263`），没有释放调用者 |
| free list、bins、`search_free_list`、`elt_delete`、`dealloc_elt`、`combine_free_blocks`、`heap_dealloc`（`heap.c:77-140,250-352,467-491`） | **不移植** | 唯一调用者是 `vlib_unregister_errors`（`error.c:98-110`）；Hammer 没有单节点卸载 |
| `heap_alloc_aligned`（`heap.h:269-277`） | **不移植** | 错误列不做对齐分配（`heap_alloc` = align 0）；有调用者时再批准 |
| `heap_dup`/`heap_free`/`heap_validate`/`format_heap`/`used_elt_bitmap`/静态 heap | **不移植** | 没有消费者；`used_elt_bitmap` 在 VPP 里是 `CLIB_DEBUG` 专用（`heap.c:452-456,474-481`） |

### 4.3 为什么 Rust 里没有 `dealloc`

1. **VPP 的释放只有一个调用者**：卸载节点时释放它的错误区间（`vlib_unregister_errors`，
   `error.c:98-110`）。Hammer 今天没有单节点卸载——错误表的生命周期就是图的生命周期，
   `detach_graph_for_rebuild` 重新构造整个 `NodeRuntimeInner`（`node.rs:1204-1240`），heap 随所有权
   一起 Drop；重建后从 offset 0 重新分配，与今天的 `next_error_index: 1` 行为一致。
2. **Rust 的释放是所有权，不是句柄**：任何"归还单个区间"的形态都要等它有第一个调用者（运行期
   unregister），届时按"把区间值交回 heap"批准（§14 项 1）。今天不写没有调用者的释放路径，
   也不会出现裸指针、`unsafe`、或 `Drop` 里访问别的对象——heap 的存储就是它自己的 `Vec<T>`，
   由 Main Heap 分配，没有任何跨对象地址。
3. **没有释放 ≠ 没有 heap**：heap 在这里不是内存分配器（内存是 Rust `Vec` 的事），而是
   **offset 身份空间**——同一区间在一次注册内不移动、offset 稳定，buffer 里的错误值、
   `/node/errors` 的列号、`/err/*` 指向的列必须是同一个数字。VPP 需要 free list 是为了让
   "释放过的区间可被复用"，Hammer 今天在同一张图内只增不减（水位的语义），因此只移植分配侧。

### 4.4 heap 元素 = 列归属；heap 归节点域

- **元素类型**：VPP 在注册时把传入的 `counters[]`（名字/描述/severity）`clib_memcpy` 进 heap
  （`error.c:154`），并在元素里保存 symlink 索引（`cd[i].stats_entry_index = vlib_stats_add_symlink
  (…)`，`error.c:182-186`），为的是注册之后还能回答"这一列是谁"（再注册、`show errors`、卸载）。
  Hammer 的名字/描述/severity 是节点注册里的 `&'static [NodeErrorDescriptor]`
  （`node.rs:302-321`），不需要第二份；symlink 的 `DirectoryIndex` 今天没有读者（注册路径用
  `find` 幂等判定，§5 步骤 5）。所以 heap 元素只存列归属，而且这个归属就是 `NodeId` 本身——
  不为一个只写不读的字段再建一个单字段结构
  （§3.4）。它是将来反查/卸载要读的唯一事实；不存它就得另建一张平行表。
- **放在 `NodeMain`（节点域）**：VPP 把 `counters_heap` 放在 `error_main`，但只有 thread zero 的
  注册路径分配它（`error.c:125` 的 thread-zero 断言），worker 只是拷贝。Hammer 的列归属本来就在
  节点域（`error_indices`/`next_error_index`），把分配器与它放在同一所有者里，main 域不必再加一个
  `RefCell<Heap>`（见 §2 的说明与 §8 差异）。

### 4.5 列值上限不变

heap 的 offset 是 u32（VPP 用最高位做 free bit，`heap.h:47-56`），但 Hammer 的全局列号要放进
`NodeErrorIndex`（u16，`crates/hammer-core/src/graph/error.rs:6-8`）。节点域在分配前检查
`heap.len() + 待分配列数 <= u16::MAX`（列号 = offset + 1，最大 65535），超限仍是既有的
`RuntimeError::NodeErrorSlotOverflow`（§3.4）。元素空间本身因此永远不需要超过 65535 个元素。

## 5. 注册路径（`vlib_register_errors` 的移植）

**调用点与时序：**thread zero 的 `DataPlaneMain::register_node_errors`，由
`init_graph_from_declarations` / `extend_graph_with_node_functions` 调用（VPP 的
`ASSERT (vlib_get_thread_index () == 0)`，error.c:125）。`init_stats_main` 是 init function，在 graph
init 之前发布 `StatsMain`，所以注册期能访问段（`config/stats.rs:189-207`、`main_loop.rs:39-49`）。

步骤（顺序 = error.c:135-186）：

1. **节点域**：校验节点、确认该 slot 未注册过；`descriptors.len() == 0` 时直接返回（VPP
   `n_errors == 0` 早退，error.c:135）——不建条目、不改列。
2. **节点域分配区间**：先做上限检查（`heap.len() + count <= u16::MAX`，超限仍是
   `NodeErrorSlotOverflow`，§4.5），再 `Heap::alloc(count, node)` 得到区间起点
   offset（`u32`），记下 `NodeErrorColumnRange { first: NodeErrorIndex::new(offset + 1)?, count }`
   （`+1` 跳过保留列 0；上限检查保证这一步必定成功）。等价于 `heap_alloc
   (em->counters_heap, n_errors, …)` + `n->error_heap_index`（error.c:138-139）。
3. **main 域解析条目**（`if (em->stats_err_entry_index == 0) vlib_stats_add_counter_vector (…)`，
   error.c:158-159）：`node_error_stats_entry_index.get()` 已是 `Some` → 复用；否则
   `StatsSegment::find("/node/errors", CounterVectorSimple)`
   （`crates/hammer-stats/src/segment.rs:200-215`）命中 → 复用，
   `MetricNotFound` → `add_simple_counter("/node/errors")`，其它错误（类型不符）翻译成
   `RuntimeError::Stats`；随后写回 `Cell`。
4. **main 域发布形状**：`columns = self.nodes.node_error_columns()`；
   `rows = ThreadMain::global().worker_count() + 1`；
   `segment.validate(entry, rows - 1, columns - 1)`（VPP `vlib_stats_validate (entry, n_threads - 1,
   l - 1)`，error.c:163）。形状已够时是 no-op；增长时换行、拷旧内容、释放旧行
   （`segment.rs:540-706`）——列可以随注册追加，已写进去的计数不丢。
5. **main 域发别名**：对每个 `i`，路径 `format!("/err/{node_name}/{descriptor.name}")`，目标列 =
   `self.nodes.node_error_index(node, i)`（节点域解析，= 区间起点 + code）：`find(path, Symlink)` 命中
   → 跳过（VPP `vlib_stats_add_symlink` 对已存在名字返回 `~0`，stats.c:528-559）；`MetricNotFound` →
   `add_symlink(entry, u32::from(index), &path)`（error.c:182-186）；其它错误向上翻译。

**冻结点断言：**`validate` 不得与写入并发，错误槽只能在装条目之前分配（heap 只增不减，冻结点后
不再 `alloc`）。节点域的
`register_node_errors` 在 `barrier::global().is_some()`（`barrier.rs:83-95`）之后调用时按本地不变量
违反终止。今天没有生产路径在冻结点之后分配错误槽（`rebuild_graph` 只被测试使用且不注册错误，
`node/frame.rs:679`）。

**重建与克隆：**`detach_graph_for_rebuild` 整体替换 `NodeRuntimeInner`（`node.rs:1223-1240`），heap
随所有权 Drop，重建从 offset 0 重新分配（等价于今天的 `next_error_index: 1`）；worker 通过
`NodeMain::clone`（`node.rs:461-486`）拿到 heap 的只读副本（§2、§4.4）。

**失败语义：**注册期失败（容量耗尽、类型不符、段错误）发生在启动期/插件加载期，`RuntimeResult` 原样
向上（`NodeErrorSlotOverflow`、`RuntimeError::Stats` 都是既有的）；不做半程回滚——图在这条路径上失败
即启动失败。

## 6. 记录路径（`vlib_error_count` 的移植）

```rust
#[inline]
pub fn record_current_node_error<E: NodeErrorCode>(&self, error: E) -> RuntimeResult<NodeErrorIndex> {
    let node = self.current_node().ok_or(RuntimeError::NodeDispatchContextMissing)?;
    let index = self.nodes.node_error_index(node, error.local_code())?;
    if let Some(entry) = self.node_error_stats_entry_index.get() {
        StatsMain::global()?.segment.increment_simple_counter(
            entry,
            self.thread_index(),
            u32::from(index.get()),
            1,
        );
    }
    Ok(index)
}
```

- **写入点 = VPP 的 `em->counters[counter] += increment`**（error_funcs.h:35）：行 = 本线程的
  `thread_index`，列 = 全局 `NodeErrorIndex` 值，increment = 1。
- **唯一写者**：行只由拥有它的线程写（thread zero 写第 0 行，worker `i` 写第 `i` 行），没有第二个
  写者，也没有跨线程句柄（§2）。`Relaxed` 足够：这是统计量自增，不发布所有权、不承诺跨列快照
  （ADR-0021 D4 的 "Relaxed 只用于统计"）。
- **没有新的 recoverable 错误**：唯一可能失败的一步仍是既有的索引解析
  （`NodeErrorSlotOverflow`）；没有错误槽的进程里 `node_error_stats_entry_index` 是 `None`，计数被
  跳过（与"整个家族不存在"一致）；单元格越界由机制层按 VPP 的
  `ASSERT (counter < vec_len (em->counters))`（error_funcs.h:34）同一纪律带事实断言。
- **increment 恒为 1**：Hammer 的记录 API 是每条错误一次调用
  （`hammer-service/src/data_plane.rs:8-24` 与各插件调用点），等于 VPP 里
  `vlib_error_count (…, 1)`；VPP 的 `increment` 参数服务于批量 drop，批量入口见 §14。
- **调用点不变**：`let _ = runtime.record_current_node_error(X)` 丢弃的是返回值（buffer 索引），
  计数照记——这正是 VPP 把"计数"（`error_funcs.h:24-37`）与"写 buffer error"
  （`b->error = …`，`error.c:52-55`）分开的语义。
- **可见性**：写立即进段，client 下一次读就能看到（没有 `update_interval` 延迟；这点要在 client 契约
  里写明，与 `/sys/node/*` 按轮次更新不同）。

## 7. 安装与冻结

VPP 在三个点重取 `error_main.counters`（注册 error.c:164-170、worker clone threads.c:766-778、
refork threads.c:930-944），因为它是**缓存地址**。Hammer 不缓存地址（§2），所以不需要重取；剩下的
唯一 per-thread 事实是**条目索引**，装一次：

- **注册期**（thread zero，§5）：创建/复用条目、`validate` 形状、发别名。
- **冻结期**（`start_workers`，main-loop-enter，`start_workers.rs:36-67`）：thread zero 的
  `DataPlaneMain` 与每个 Worker 的 `DataPlaneMain` 各自
  `install_node_error_stats_entry(entry)`；`None` 表示没有错误槽（没有条目）。这一步对应 VPP 的
  `vm_clone->error_main.counters = c[worker_thread_index]`，Hammer 只需要条目，因为行号就是
  `thread_index`。
- **运行期 refork / 运行期注册**：本设计不实现（§14 项 1）。行可以被 `validate` 替换这一点由机制层
  的每次解析消化，不需要调用方重取。

**冻结的前提（写进代码注释与验收）：**装条目之后不得再调用 `validate` 增长列宽；机制层的 SAFETY
注释要求"调用方守启动期单写者纪律"（`segment.rs:652-657,676-677`），因为增长会换掉并释放旧行。
冻结点判据是 `barrier::global().is_some()`。

## 8. 与 VPP 的有意差异

| # | 差异 | 理由 |
| --- | --- | --- |
| 1 | 不缓存行地址：每次写由机制层按 `(entry, row)` 解析（VPP 把 `get_entry_data_pointer(entry)[row]` 缓存在 `error_main.counters`，并靠三个刷新点重取） | 见 §2：机制层的契约是"句柄是 `DirectoryIndex`、寻址在机制内部"，缓存地址会引入裸指针（进而 `unsafe impl Send`）与一套刷新协议；per-write 解析无锁、无分配 |
| 2 | 单元格用 `AtomicU64::fetch_add(increment, Relaxed)`，VPP 是普通 `u64` 的 `+=`（error_funcs.h:35） | 段内单元格在 Hammer 机制层本来就是原子访问位置（`protocol.rs:515-521`），与 `set_simple_counter` 同一纪律；行仍是单写者，语义与 `+=` 相同 |
| 3 | 列 0 保留为全局 no-error 槽，列 = `NodeErrorIndex` 值 = heap offset + 1（VPP 里列号就是 offset） | Hammer 的 `NodeErrorIndex` 是 NonZeroU16（0 = 无错误），identity 映射同时保住"buffer 值 == 列号"。代价：与 VPP 的密集列空间差 1 列；`/node/errors` 的 shape 不逐字节等于 VPP |
| 4 | 不做 `counters_last_clear` / `clear errors` / `show errors` 差值（error.c:280-295、348-360） | 没有对应的 Hammer 控制面命令面；VPP 的基线是每线程进程内副本（error.h:36），补它不改段内布局 |
| 5 | 段内不发布 severity/description（VPP 的 `vlib_error_desc_t` 只在进程内，error.h:15-28） | 与 VPP 一致：描述不进段；需要时是新增条目，不靠 symlink 名字编码 |
| 6 | 不支持运行期注册错误槽（VPP 在段锁下增长并重指所有线程） | 机制层要求增长不与写入并发；Hammer 的图在 Worker launch 前冻结。对应能力见 §14 项 1 |
| 7 | 不做 `nm->node_by_error` 反查表（error.c:188-196） | 今天没有"由 buffer 错误值反查节点"的 Hammer 调用者 |
| 8 | heap 只移植分配侧：没有 `heap_dealloc`、free list、bins、合并；也没有 `error_heap_handle` | VPP 的释放路径只有 `vlib_unregister_errors` 一个调用者（error.c:98-110）；Hammer 没有单节点卸载，重建 = 整张 `NodeRuntimeInner` 替换（Rust Drop）。Rust 的释放是所有权，不是句柄（§4.3） |
| 9 | heap 归节点域（`NodeMain`），VPP 放在 `error_main` | 只有 thread zero 分配它（error.c:125），worker 只持有拷贝；Hammer 的列归属本来就在节点域，main 域不再加一个 `RefCell<Heap>`（§2、§4.4）。worker 克隆是只读副本，与 VPP 的浅拷贝等价 |
| 10 | heap 元素只存列归属（`NodeId`），不存名字/描述/severity/symlink 索引 | VPP 的 `vlib_error_desc_t` 元素承载这些是因为它注册后还要用（再注册、`show errors`、卸载）；Hammer 的 descriptor 是静态声明，symlink 索引用 `find` 幂等判定、今天没有读者（§4.4），所以元素退回它唯一还成立的事实 |

## 9. Stats Client 集成（node stats + node error stats）

**两个家族、两份独立 report，不合成快照：**

| 家族 | provider | 存在条件 | 更新方式 |
| --- | --- | --- | --- |
| `/sys/node/*`（ADR-0023） | `NodeStatsProvider` | `per_node_counters` 开关 | 采集者按 `update_interval` 投影 |
| `/err/*`（本 ADR） | `NodeErrorStatsProvider`（`PREFIX = "/err"`） | 有节点声明错误才有条目 | 记录点直接写，立即可见 |

服务端仍然发布 `/node/errors`（§1、§5 的契约不变）；client 不读它、只读 `/err/*`，因为 VPP 自带
客户端就是这么做的（`set_errors` 只按 `/err/` 前缀过滤）。

**`NodeErrorStatsProvider`（client 仓，外部 `netsystem-client`）：**

```rust
pub struct NodeErrorStatsProvider;

impl StatsProvider for NodeErrorStatsProvider {
    const PREFIX: &'static str = "/err";
    type Report = Option<NodeErrorStats>;

    fn report<R: StatsReader>(reader: &R) -> Result<Self::Report, Error>;
}

pub struct NodeErrorStats {
    pub errors: Vec<NodeErrorCounters>, // 按 (node, error) 排序
    pub sampled_at: Instant,
}

pub struct NodeErrorCounters {
    pub node: String,
    pub error: String,
    pub counts: Vec<u64>,             // 按线程顺序（thread zero 起）；len() 就是线程数
}

impl NodeErrorCounters { pub fn total(&self) -> u64; }
impl NodeErrorStats {
    pub fn error(&self, node: &str, error: &str) -> Option<&NodeErrorCounters>;
    pub fn node(&self, name: &str) -> impl Iterator<Item = &NodeErrorCounters> + '_;
    pub fn total(&self) -> u64;
}
```

读取规则（对齐 VPP 自带客户端的 `set_errors`，
`third_party/vpp/src/vpp-api/python/vpp_papi/vpp_stats.py:236-248`）：

1. `reader.names()` 一次，取 `/err/` 前缀条目；一条都没有 → `Ok(None)`（"未发布该家族"，不是 0）。
   VPP 的 `set_errors` 同样只按前缀过滤，从不读 `/node/errors`。
2. 逐条 `read("/err/<node>/<error>")`：必须是 `Simple(rows)` 且每行 1 列（symlink 裁列）；
   `counts` 就是这些行的值，线程数由 `counts.len()` 得到。VPP 在这一步做的是 `self[k].sum()`。
   VPP 的 report 是 `name → total` 映射，所以它跳过 total 为 0 的条目；Hammer 的 report 保留
   每个已发布错误的每线程计数（调用方按 `total()` 自行过滤），这是本仓与 `set_errors` 的
   唯一有意差异。
3. 名字解析：去掉 `/err/` 前缀后 `split_once('/')`，两段非空即 `(node, error)`；不满足就跳过该条
   （服务端注册期已断言节点名不含 `/`，§1），不构造新错误类别。
4. 列举与读取之间消失的条目（`MetricNotFound`）跳过——VPP 的 `except KeyError: pass`。
5. 其它读取失败（轮次变化、协议/类型不符）用既有 client `Error` 原样向上，不新增错误类别。
6. 不做完整性/列数校验（VPP 客户端没有 `columns - 1` 这类检查），不按列号与名字顺序对应，
   也不做跨条目的值校验（多次独立读取，不是同一瞬间，值还在自增）。

**与既有 node 家族的集成：**两份 report 由调用方按节点名 join（`NodeStats::node(name)` /
`NodeErrorStats::node(name)`），两次独立读取、两个 `sampled_at`；`StatsClient` 不加家族方法，
不加协议变体，不在 provider 里缓存 `DirectoryIndex`。

## 10. 分层隔离契约

| 层 | 可以做什么 | 不可以做什么 | 校验 |
| --- | --- | --- | --- |
| `hammer-infra` | 泛型 heap（元素空间 + offset 分配），`Vec<T>` 存储 | 认识 node/error/列号/stats/段；出现释放 API、释放相关字段或裸 handle | `rg -n "node_error|NodeError|node/errors" crates/hammer-infra/src` 无命中 |
| `hammer-stats` | `add_simple_counter`/`find`/`validate`/`add_symlink`/`set_simple_counter`/`increment_simple_counter`/采集者表/轮次 | 出现 node/error/`NodeErrorIndex`/`NodeErrorDescriptor` 名字；为 error 家族加第二套登记 | `rg -n "node_error|NodeError" crates/hammer-stats/src` 无命中 |
| `hammer-runtime` main 域（`DataPlaneMain`） | 持有本线程的条目索引；注册期发布形状；记录点写本线程行 | 把条目/行交给别的线程；在冻结点之后分配错误槽 | 字段与访问器 `pub(crate)`；冻结点断言 |
| `hammer-runtime` node 域（`NodeMain`） | 错误槽身份、列空间（`Heap<NodeId>`）、列宽、本地 code → 全局列号 | 碰段、碰行、记计数 | `NodeMain` 无任何 stats 段类型；访问器 `pub(crate)` |
| 插件/服务 | `record_current_node_error` + `NodeErrorCode`；按需 `set_index_node_error` | 认识 `/node/errors`、列号、段、轮次 | `rg -n "node/errors\|/err/" crates/hammer-plugins crates/hammer-service` 无命中 |
| client（外部仓） | `StatsReader`（names + read）、`StatsProvider` 投影、本地聚合 | `StatsClient` 家族方法、协议改动、跨 epoch 缓存 | in-memory fixture + e2e；`StatsClient` 公共面无 node error 名字 |

## 11. 新增/修改 API 审批清单

**新增（全部待批准）：**

| 文件 | 项 | 说明 |
| --- | --- | --- |
| `crates/hammer-infra/src/heap.rs`（新模块）+ `lib.rs` 的 `pub mod heap;` | `Heap<T>`、`Heap::new/alloc/len/is_empty/get`（offset 用 `u32`） | §3.1、§4：只移植分配侧，没有 `dealloc`/free list/裸 handle |
| `crates/hammer-runtime/src/node.rs`（状态替换） | `NodeMain.error_column_heap: Heap<NodeId>`（元素 = 列归属节点）；`NodeErrorColumnRange`；`error_indices`/`next_error_index`/`error_tables_installed` → `error_columns: Vec<Option<NodeErrorColumnRange>>` | §3.4、§4.2：净减两项状态 |
| `crates/hammer-stats/src/segment.rs` | `StatsSegment::increment_simple_counter(index, row, column, increment)` | §3.2，唯一机制改动；纪律与 `set_simple_counter` 相同 |
| `crates/hammer-runtime/src/data_plane/main.rs` | `DataPlaneMain.node_error_stats_entry_index: Cell<Option<DirectoryIndex>>`（`pub(crate)`：冻结点由 `start_workers` 读取后分发给每个 runtime） | §3.3 |
| 同上 | `DataPlaneMain::install_node_error_stats_entry(&self, entry: Option<DirectoryIndex>)` | §3.3、§7 |
| `crates/hammer-runtime/src/data_plane/dispatch.rs` | `DataPlaneMain::register_node_errors(&self, node, descriptors)` | §3.3、§5 |
| `crates/hammer-runtime/src/node.rs` | `NodeMain::node_error_columns`；`record_node_error` → `node_error_index` 改名 | §3.4 |
| `crates/hammer-runtime/src/node.rs` + `data_plane/dispatch.rs` + `crates/hammer-plugins/net/{ip,icmp}` | **改名（不是新 API）**：`materialize_node_errors` → `register_node_errors`（节点域收为 `pub(crate)`，`NodeRuntimeInner` 同名私有方法一并改）；插件 16 个调用点改调 `DataPlaneMain::register_node_errors` | §3.4、§4、§5：与 VPP `vlib_register_errors` 同名，语义不变 |
| `crates/hammer-runtime/src/start_workers.rs` | 每线程装条目（thread zero + 每个 Worker） | §7 |
| `crates/hammer/tests/stats_segment_mapping.rs` | error 家族的只读样例与断言 | §12 |
| `netsystem-client`（外部仓） | provider/report/两组用例（**不新增错误变体**） | §9 |

**不新增（明确不做）：**不引入行表、采集者、`#[derive(Stats)]` 的 error 家族、`main_loop_enter` 安装步
（`lib.rs` 的 registration image 不加项）、缓存的 `counters` 指针/`&'static [T]` 行引用、
`unsafe impl Send`、裸 node/counter 索引入口（VPP 的 `vlib_node_increment_counter`，
`node_funcs.h:1397-1405`）、`/sys/last_stats_clear`、段内 severity/description 条目；heap 的释放面
（`heap_dealloc`/free list/bins/合并）、`HeapHandle`/`error_heap_handle`、对齐分配
（`heap_alloc_aligned`）、`heap_dup`/`heap_free`/`heap_validate`/`format_heap`/静态 heap。
client 侧同样不加错误类别：没有 `columns - 1` 完整性校验，也没有"名字不合法"变体（§9 的读取规则）。

## 12. 迁移与验证

| 阶段 | 范围 | 完成证据 |
| --- | --- | --- |
| M0 机制原语 | `hammer-infra` heap（分配侧：offset 连续、`len` 单调、`get` 越界 `None`、`alloc(0)` 断言；文档写明没有释放面）+ `increment_simple_counter` + 文档（形状需已发布、越界断言） | heap 单测通过；`hammer-infra` 仍无 node/error 名字；机制层单测：行解析、自增、越界/类型断言；`hammer-stats` 仍无 node/error 名字 |
| M1 注册路径 | 条目字段 + heap 注册（上限检查 + `NodeErrorColumnRange`）+ 改名 `register_node_errors`（含 16 个插件调用点）+ `validate`/symlink/冻结点断言 + `node_error_columns` | 目录侧：`/node/errors` 形状、`/err/*` 完整；零错误时条目不存在；两次注册增长不丢已写计数；列号 = `heap.len()`（列宽 = `heap.len() + 1`）；插件全部编译通过 |
| M2 记录路径 | `increment_simple_counter` 接入 + `node_error_index` + `install_node_error_stats_entry` + `start_workers` | 两个 Worker 各自记录，读回各自行；thread zero 的 process 节点错误进第 0 行；buffer 值 == 列号 |
| M3 本仓 fixture | 真实 daemon 的只读断言（零错误配置 + 已注册错误表的节点，如 `ip4-local`/`icmp`） | fixture 通过（`crates/hammer/tests/stats_segment_mapping.rs` 的 `stats_segment_publishes_node_error_columns`）：零错误时 `/node/errors` 与 `/err/*` 都不存在；加载 ip/icmp 插件后形状 = 每线程一行、列 = 1 + 错误数、每个 `/err/*` 是 symlink 且列恰好覆盖 `1..columns`；`per_node_counters = false` 时 error 家族仍完整 |
| M4 client | §9 的全部用例（in-memory fixture + e2e） | 按 VPP `set_errors` 语义：只按 `/err/` 前缀列举、逐条读、消失就跳过；记录后立即可见 |

关键断言：写者唯一（只改自己那一行=自己 thread_index 那行）；thread zero 行身份；索引解析失败
不计数；形状 = 冻结事实（列宽 == `heap.len() + 1`，无错误槽时无条目）；记录即可见；冻结点之后
注册 → 断言；无名节点 → 注册期断言；协议不动（`STAT_SEGMENT_VERSION` 仍为 2）。

## 13. 需要一并修订/新增的其他东西（总清单）

1. **ADR-0019 §6.5 的表述**（`docs/adr/0019-…md:236-239`）：原文禁止"任意 row/thread_index
   getter"。本 ADR 的写法**不需要任何 row getter**：调用方只持有条目索引，行解析与单元格写入都在
   机制层的既有句柄纪律内（§2、§3.2）。修订后的表述应写成"§6.5 的禁止仍然成立；error 家族按
   `increment_simple_counter` 的句柄模型实现，不新增 row/thread_index getter"。
2. **ADR-0021 D4 的例外**（`docs/adr/0021-…md:577-582`）：原文决定"不让 worker 直接写共享单元格"。
   error counter 家族按 VPP 的形状改为"记录点写本线程那一行"（§2、§6）。例外依据：行归该线程所有
   （行号 = 自己的 `thread_index`）、条目索引在冻结点装好、增长只允许发生在冻结点之前、单元格在机制
   层本来就是原子访问位置。`/sys/*` 家族继续走"owner 原子计数 + 轮次投影"，不变。
3. **ADR-0023 D8 的 clear 面措辞**：继续推迟，但理由写成"没有对应的控制面命令面 + VPP 的基线是
   进程内副本"，而不是"与 VPP 无关"。
4. **本仓 fixture**：`crates/hammer/tests/stats_segment_mapping.rs` 追加 error 家族样例（§12 M3）。
5. **外部 `netsystem-client` 仓**：新增 provider/report/用例（§9、§11）；不新增错误变体。
6. **`hammer-infra` 的 heap 原语**：新增模块与单测（§12 M0）；它不认识 node/error，也不含释放面。
7. **生命周期阶段修正（本 ADR 的 M3 前置）**：`ip_feature_init` 与 `interface_feature_init`
   从 init-function 阶段移到 main-loop-enter 阶段（约束见 ADR-0006 的实现修订），因为 Hammer 在
   init functions 之后才 materialize 图节点，而这两个函数需要节点已经存在；`topological_order`
   同时改为 VPP 的稳定注册序拓扑序（`third_party/vpp/src/vlib/init.c:172-199`）。M3 的
   "加载插件即可看到 `/err/*`" 依赖这两处，否则带插件的 daemon 无法启动。
8. **既有插件缺口（本 ADR 不修，但影响验证选材）**：`ip4-input`/`ip6-input` 在记录点调用
   `record_current_node_error(IpInputError)`，但没有任何 call site 注册 `IpInputError::DESCRIPTORS`
   （该类型也没有 `DESCRIPTORS`），所以这些记录点今天返回 `NodeErrorSlotOverflow`。M2/M3 要选真实
   注册过错误表的节点（`ip4-local`、`icmp`），插件侧单独修。
9. **既有名字的重命名（语义不变）**：`NodeMain::materialize_node_errors`（`pub`，插件 16 个调用点：
   `crates/hammer-plugins/net/ip/src/local.rs:134,172,236,304,342,406`、
   `net/ip/src/icmp_error.rs:127,138`、`net/icmp/src/icmp.rs:252,281,407,458,771,782,806,815`）
   → `register_node_errors`；`NodeRuntimeInner` 的私有同名方法一并改；`record_node_error` →
   `node_error_index`（§3.4）。

## 14. 未决项

1. **运行期注册 / 运行期增删 Data Worker**：机制层要求 `validate` 的列增长不与写入并发
   （`segment.rs:652-657,676-677`）。将来要支持时，协议必须是"barrier 停住所有 Data Worker → thread
   zero 增长形状、发新别名 → 每个线程重新装自己的条目（若条目变了）→ 放行"；行地址不需要重取
   （§2）。届时 heap 才会第一次需要释放面：单节点卸载 = "把区间交回 heap"（Rust 所有权归还，
   不是裸 handle），free list/bins/合并随第一个调用者一并批准。本设计不实现。
2. **批量 increment**：记录路径恒记 1。若出现"一帧 N 个包只记一次"的 drop/punt 路径，用
   `increment_simple_counter` 的 `increment` 参数即可，需要单独批准的是调用侧语义（VPP 的
   `vlib_error_count (…, increment)`）。
3. **clear 面**：`counters_last_clear`（error.c:348-360）与 `/sys/last_stats_clear` 的写入者不在本文。
4. **severity/description 发布**：如果需要，属于新增条目与新增批准（不能靠 symlink 名字编码）。
5. **列 0 的取舍**：今天列号 = heap offset + 1（列 0 是 `NodeErrorIndex` 的 no-error 保留槽）。
   如果后续要求与 VPP 逐字节同形（列号 == offset），需要放弃全局 no-error 哨兵并改成每节点一个
   `NONE` 描述符（VPP `error.def:40` 的约定），那是要重新批准的改动。
6. **`node_by_error` 反查**：未来 trace/CLI 需要时单独批准（error.c:188-196）。
7. **重建/卸载的别名一致性**：重建沿用同一条目时已存在的 `/err/*` 按 VPP 语义跳过；若重建改变某
   节点的错误区间，旧别名会指向旧列。今天只有测试调用 `rebuild_graph` 且不注册错误。
8. **client 落地节奏**：与 `netsystem-client` 仓独立 issue/PR；四条边界不随命名变化（底层不认识家族、
   provider 只经 `StatsReader`、编译期单态化、家族缺失 → `Ok(None)`）。
9. **client 读取成本**：每个 `/err/*` 一次 `read`（线性目录查找 + 单独 epoch 校验）；错误槽数量级
   很小，可接受。

## 附录 A：VPP 源码位置索引

| 位置 | 内容 |
| --- | --- |
| `third_party/vpp/src/vlib/main.h:134,140` | `vlib_main_t` 的 `node_main` 与 `error_main` 兄弟成员 |
| `third_party/vpp/src/vlib/error.h:15-44` | `vlib_error_desc_t`（进程内）；`vlib_error_main_t { counters; counters_last_clear; counters_heap; stats_err_entry_index; }` |
| `third_party/vpp/src/vlib/error.h:38-39` | 注释 "Counter structures in heap. Heap index indexes counter vector."：heap 元素空间与计数列号是同一个索引 |
| `third_party/vpp/src/vlib/error.c:113-141` | `vlib_register_errors`：thread-zero 断言、`heap_alloc`、`error_heap_index` |
| `third_party/vpp/src/vlib/error.c:98-110` | `vlib_unregister_errors`：`heap_dealloc` 的唯一调用者 |
| `third_party/vpp/src/vlib/error.c:158-170` | 惰性创建 `/node/errors`、`validate`、`sc = get_entry_data_pointer`、`tem->counters = sc[i]` |
| `third_party/vpp/src/vlib/error.c:172-186` | 重注册时清基线与 `/err/%v/%U` 别名 |
| `third_party/vpp/src/vlib/error.c:188-196` | `nm->node_by_error` 反查表 |
| `third_party/vpp/src/vlib/error.c:280-295,348-360` | `show errors` 的差值读取、`clear errors` 基线 |
| `third_party/vpp/src/vlib/error_funcs.h:24-37` | `vlib_error_count`：断言 + `em->counters[counter] += increment` |
| `third_party/vpp/src/vlib/threads.c:575,766-778` | worker clone：`stats_err_entry_index = fvm->error_main.…`、`validate`、`vm_clone->error_main.counters = c[worker_thread_index]` |
| `third_party/vpp/src/vlib/threads.c:930-944` | refork：拷贝 `error_main` 后重取本线程行 |
| `third_party/vpp/src/vlib/stats/stats.h:116-121` | `vlib_stats_get_entry_data_pointer (entry)` 返回 `e->data` |
| `third_party/vpp/src/vlib/stats/stats.c:355-390,496-524` | `vlib_stats_add_counter_vector`；`validate` 增长/替换 `u64 **data` 的每一行 |
| `third_party/vpp/src/vlib/stats/stats.c:528-559` | `vlib_stats_add_symlink`：名字已存在则返回 `~0` |
| `third_party/vpp/src/vlib/stats/collector.c:154-176` | 对照面：`/sys/node/*` 由 collector process 按轮次投影、受开关控制 |
| `third_party/vpp/src/vlib/stats/shared.h:37` | `VLIB_STATS_MAX_NAME_SZ = 128` |
| `third_party/vpp/src/vlib/node.h:333,341-342` | `vlib_node_t.n_errors` / `error_heap_handle` / `error_heap_index` |
| `third_party/vpp/src/vlib/node_funcs.h:1397-1405` | `vlib_node_increment_counter`（裸索引入口，本设计不做） |
| `third_party/vpp/src/vlib/buffer.h:104-105`；`src/vlib/error.c:26,52-55`；`src/vlib/node.h:799-812` | buffer 的 `vlib_error_t error` = 绝对列号；`vlib_error_get_node`/`get_code` 反推 node/code |
| `third_party/vpp/src/vnet/ethernet/error.def:40` 等 | 每节点 code 0 = `NONE, "no error"` 约定 |
| `third_party/vpp/src/vpp-api/python/vpp_papi/vpp_stats.py:236-267` | VPP 自带客户端的 `/err/*` 读取行为 |
| `third_party/vpp/src/vppinfra/heap.h:8-31,33-63` | heap 用法与语义：`heap_alloc`/`heap_dealloc`/`heap_size`；`heap_elt_t` 与 `HEAP_ELT_FREE_BIT`；`heap_offset` |
| `third_party/vpp/src/vppinfra/heap.h:86-88,90-126,174-193,263-281` | size bins、`heap_header_t`/`HEAP_DATA_ALIGN`、`heap_elts`/`heap_new`、`heap_len`、`heap_alloc`/`heap_dealloc` 声明 |
| `third_party/vpp/src/vppinfra/heap.c:40-76,250-352,353-466,467-492` | `size_to_bin`/`bin_to_size`、`search_free_list`、`_heap_alloc`（尾部追加、offset/handle 回写、失败 `~0`）、`heap_dealloc` |
| `third_party/vpp/src/vppinfra/heap.c:77-140,494-566` | `elt_delete`、`elt_new`、`elt_data`、free-list 索引（`small_free_elt_free_index`）、`combine_free_blocks` |

## 附录 B：Hammer 现状位置索引

| 位置 | 内容 |
| --- | --- |
| `crates/hammer-runtime/src/data_plane/main.rs:33-42` | `DataPlaneMain` 字段（`thread_index`/`nodes`/`current_node`/…） |
| `crates/hammer-runtime/src/data_plane/trace.rs:84-93` | `record_current_node_error`（唯一记录入口） |
| `crates/hammer-service/src/data_plane.rs:8-24` | `set_index_node_error(runtime: &mut DataPlaneMain, …)` |
| `crates/hammer-runtime/src/node.rs:302-321,339,448-450,558-594,625-631,1774-1780` | 错误身份、错误表状态（`error_indices`/`next_error_index`/`error_tables_installed`，将换成 `error_column_heap`/`error_columns`）、注册、解析 |
| `crates/hammer-runtime/src/node.rs:461-486,1204-1240` | worker 克隆 `NodeRuntimeInner`；`detach_graph_for_rebuild` 整体替换（heap Drop 的落点） |
| `crates/hammer-runtime/src/data_plane/dispatch.rs:19-90` | `init_graph_from_declarations` / `extend_graph_with_node_functions` 的注册循环 |
| `crates/hammer-plugins/net/ip/src/local.rs:134,172,236,304,342,406`；`net/ip/src/icmp_error.rs:127,138`；`net/icmp/src/icmp.rs:252,281,407,458,771,782,806,815` | `NodeMain::materialize_node_errors` 的插件调用点（改名对象，§13 项 8） |
| `crates/hammer-runtime/src/start_workers.rs:36-67` | 行安装与冻结点（`barrier::install`） |
| `crates/hammer-stats/src/segment.rs:232-262,540-706` | `set_gauge`/`set_simple_counter` 的句柄纪律；`validate` 增长会换行并释放旧行 |
| `crates/hammer-stats/src/protocol.rs:488-521` | `DirectoryEntry::set_simple_counter_cell`：行解析、形状断言、kept-atomic 单元格写 |
| `crates/hammer-runtime/src/config/stats.rs:189-207` | `stats_main_init`：init function，发布 `StatsMain` |
| `crates/hammer-runtime/src/main_loop.rs:39-49` | `run_init_functions` → `init_graph_from_declarations` → `run_main_loop_enter` |
| `crates/hammer/tests/stats_segment_mapping.rs:487,884-960` | 本仓只读 fixture 风格 |
| `crates/hammer-infra/src/lib.rs:1-29` | 模块表（今天没有 vppinfra heap；`heap_boxed` 与之无关） |
| `crates/hammer-infra/src/heap.rs`（新，规划） | §3.1 的 `Heap<T>`：分配侧移植，无释放面 |

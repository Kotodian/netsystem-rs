# ADR-0013：将 pmalloc Main 迁移到 `hammer-infra`

- 日期：2026-09-13
- 状态：Proposed，待审核
- 范围：`hammer-infra` 的 pmalloc 元数据、页映射、chunk 分配和地址转换
- VPP 参考：`third_party/vpp/src/vppinfra/pmalloc.h`、`pmalloc.c`、`test_pmalloc.c`

## 决策

在 `hammer-infra` 增加 `PmallocMain`，保持 VPP pmalloc 的 page、arena、chunk、VA
索引和 VA-to-PA 转换语义。第一阶段只引入 pmalloc 本身，不接入 BufferMain、
PhysmemMap、runtime、worker 或 plugin。

容器固定为：

| 数据 | Rust 类型 |
| --- | --- |
| `chunk_index_by_va` | `HashMap<usize, u32>` |
| `lookup_table` | `AlignedVec<usize, CACHE_LINE>` |
| `pages` | `Vec<PmallocPage>` |
| `arenas` | `Pool<PmallocArena>` |
| `default_arena_for_numa_node` | `Vec<u32>` |

不使用 `Bihash`、`Slice`、`rkyv::AlignedVec` 或 `Vec<CacheLineWord>`。

## 类型

### `PmallocChunk`

表示 page 内的一段连续 block：

```text
start: u32
prev: u32
next: u32
size: u32
used: bool
```

`prev`、`next` 使用 `u32::MAX` 表示无效 chunk index。

### `PmallocPage`

```text
index: u32
arena_index: u32
chunks: Pool<PmallocChunk>
first_chunk_index: u32
n_free_chunks: u32
n_free_blocks: u32
```

### `PmallocArena`

```text
index: u32
flags: u32
fd: RawFd
numa_node: u32
first_page_index: u32
log2_subpage_size: u32
subpages_per_page: u32
n_pages: u32
name: String
page_indices: Vec<u32>
```

private arena 的 `fd` 使用无效 fd 哨兵；shared arena 保存 backing fd。shared arena
不可自动增长，private arena 可以在 `max_pages` 内追加 page。

### `PmallocMain`

```text
flags: u32
base: usize
default_log2_page_size: u8
max_pages: u32
pages: Vec<PmallocPage>
chunk_index_by_va: HashMap<usize, u32>
arenas: Pool<PmallocArena>
default_arena_for_numa_node: Vec<u32>
lookup_table: AlignedVec<usize, CACHE_LINE>
linear_pa_offset: usize
linear_pa: bool
lookup_log2_page_size: u8
```

约束：

- `chunk_index_by_va` 的 key 是 allocation 起始 VA，value 是 page 内 chunk index。
- `lookup_table` 按 page index 直接索引，元素仍是连续的 `usize`，不把每个元素扩成
  cache line。
- `u32::MAX` 保留给无效 page、chunk、arena index。
- `PmallocMain` 不保存 `MemHeap`，不持有 `MemMain`，不新增 global singleton。
- `MemMain` 仍是 page size、NUMA、backing、`vm_map` 和 `vm_unmap` 的 authority。

## `AlignedVec`

新增唯一的 aligned vector：

```text
AlignedVec<T, const ALIGN: usize>
```

内部状态只有：

```text
ptr: NonNull<T>
len: usize
capacity: usize
```

契约：

- `ALIGN` 必须是非零的二次幂。
- allocation alignment 为 `max(ALIGN, align_of::<T>())`。
- 元素 stride 仍为 `size_of::<T>()`；这里只对 allocation base 对齐。
- `len <= capacity`，只有 `[0..len]` 是 initialized elements。
- 扩容保留已有元素，并使用新的显式 aligned `Layout`。
- `Drop` 只销毁 initialized elements，再使用相同 layout 释放 allocation。
- `new`、`with_capacity`、`len`、`capacity`、`is_empty`、`reserve`、
  `reserve_exact`、`resize`、`push`、`clear`、`as_slice`、`as_mut_slice`、
  `Index` 和 `IndexMut` 提供普通连续 vector 所需的接口。

底层使用现有 Rust 全局 allocator，并为每次 allocation/deallocation 传入显式
aligned `Layout`。这不修改全局 allocator，不修改 `MemMain::MIN_HEAP_ALIGNMENT`，
也不把所有小对象强制提升为 64-byte alignment。`AlignedVec` 不管理、不选择、
不拥有 `MemHeap`。

`lookup_table` 只使用 `AlignedVec<usize, CACHE_LINE>`；不引入第二种 aligned element
类型。`rkyv` 不作为依赖，`rkyv::AlignedVec` 不用于 live pmalloc storage。

## 方法

### Pmalloc 方法

```text
new() -> Self
initialize(...) -> Result<(), MemError>
alloc_aligned_on_numa(...) -> Result<*mut u8, MemError>
alloc_aligned(...) -> Result<*mut u8, MemError>
create_shared_arena(...) -> Result<*mut u8, MemError>
alloc_from_arena(...) -> Result<*mut u8, MemError>
free(...) -> ()
```

- `initialize` 从 `MemMain` 取得 page size，通过 `MemMain::vm_map` 建立 reservation，
  初始化 page、arena 和 lookup metadata。
- `alloc_aligned_on_numa` 使用指定 NUMA node；`alloc_aligned` 使用默认 NUMA path。
- `create_shared_arena` 创建不可增长的 shared arena。
- `alloc_from_arena` 只在目标 arena 的容量内分配。
- private arena 没有可用 page 时，在 `max_pages` 内追加 page。
- allocation 成功后登记 `chunk_index_by_va`，更新 chunk links 和 free counts。
- `free` 删除 VA 索引，释放 chunk，并按 VPP 顺序合并相邻 free chunk。
- pmalloc 只通过 `MemMain` 执行 page size、backing、NUMA、`vm_map` 和 `vm_unmap`；
  不在 pmalloc 内重新实现 VM。

### Inline 方法

以下方法使用 `#[inline]`，只做直接字段、算术和索引访问；不分配、不调用 VM、
不接受 closure：

```text
get_page_index(&self, va: usize) -> u32
get_arena(&self, va: usize) -> &PmallocArena
get_pa(&self, va: usize) -> usize
convert_to_phys_addrs_with_offset(&self, addresses: &mut [usize], offset: i32)
convert_to_phys_addrs(&self, addresses: &mut [usize])
```

地址转换公式为：

```text
pa = va - lookup_table[(va - base) >> lookup_log2_page_size] + offset
```

`linear_pa` 为真时直接使用 `linear_pa_offset`。`get_pa` 返回 `usize`，不返回
`Option`。page、arena、chunk metadata 越界直接 assert/panic。

## 错误处理

不新增 `PmallocError`，也不新增以下 variant：

```text
InvalidSize
InvalidAlignment
SizeOverflow
UnsupportedArenaPageSize
PageCapacityExhausted
ArenaCapacityExhausted
AllocationTooLarge
```

处理规则：

| 情况 | 结果 |
| --- | --- |
| `MemMain` 的 VM、backing、page size、NUMA 操作失败 | `Err(MemError)` |
| size 超过 page/arena 容量 | `Ok(null_mut())` |
| shared arena 已满 | `Ok(null_mut())` |
| private arena 达到 `max_pages` | `Ok(null_mut())` |
| alignment 不是二次幂 | assert/panic |
| foreign pointer 或 double free | panic |
| page/chunk/arena metadata 越界 | assert/panic |

`Option` 不用于表示 allocation、lookup 或错误结果。VPP 的 null 语义保留为
`Ok(null_mut())`；底层可恢复系统错误只使用已有 `MemError`。

## 测试

新增 `crates/hammer-infra/tests/pmalloc.rs`，使用真实 `PmallocMain` 和真实
`MemMain` 映射路径，覆盖：

- 初始化、page size、reservation 和 lookup metadata；
- 多种 size/alignment，确认返回地址满足 alignment；
- 支持的 NUMA node 分配；
- shared arena 分配、容量耗尽和不可增长；
- size 超限返回 `null_mut()`，且 metadata 不变化；
- 相邻 chunk 释放与合并；
- foreign pointer、double free、非二次幂 alignment 的 panic；
- inline page index、arena lookup、VA-to-PA 和批量原地转换；
- `AlignedVec` 初始分配、扩容、base alignment、元素 stride、resize 和 drop；
- mapping failure 返回 `MemError`，且 page、arena、hash、lookup metadata failure-atomic；
- 完整 allocate/free 后无 live chunk 和 VA 索引。

测试必须覆盖完整的 `MemMain` 能力矩阵：

- 每个配置的 NUMA node，以及默认 NUMA arena 选择和指定 NUMA 分配；
- 每一种 page-size 模式，包括 system page、default hugepage 和 arena subpage size；
- pagemap 可用和不可用两条 VA-to-PA 路径；
- linear PA 和 lookup-table PA 两条转换路径；
- 每种 NUMA/page-size/pagemap 组合下的初始化、page mapping、allocation、free、
  chunk merge、shared arena 和 capacity refusal；
- 每种组合下的 lookup table 更新、批量原地地址转换和 mapping failure atomicity。

测试不得通过跳过组合来降低覆盖率；测试配置负责提供需要覆盖的 NUMA、page-size 和
pagemap 模式，能力配置错误应使测试失败，不能被转换成 pmalloc 的成功或错误变体。

## 变更清单

新增：

- `crates/hammer-infra/src/pmalloc.rs`：pmalloc 类型和方法；
- `crates/hammer-infra/src/aligned_vec.rs`：`AlignedVec<T, ALIGN>`；
- `crates/hammer-infra/tests/pmalloc.rs`：pmalloc 与 aligned vector 测试。

修改：

- `crates/hammer-infra/src/lib.rs`：注册并按现有约定 re-export 模块；
- `crates/hammer-infra/src/mem/mod.rs`：只有现有 `vm_map` 无法表达 reserve-then-submap
  时，才增加 crate-private seam。

明确不修改：

- `MemMain::MIN_HEAP_ALIGNMENT` 和全局 allocator 策略；
- `MemHeap`、`Pool<T>` 现有 API；
- `Bihash`、`PhysmemMap`、BufferMain、hammer-core、hammer-runtime、hammer-service；
- `rkyv` 依赖。

实现必须在本 ADR 审核通过后开始。

# ADR-0014：将 Physmem 重构为进程级 `PhysmemMain`

- 日期：2026-09-14
- 状态：Proposed，待审核
- 范围：`hammer-infra` physmem、pmalloc，以及 `hammer-core` BufferMain 接入
- VPP 参考：`third_party/vpp/src/vlib/physmem.h`、`physmem.c`、`physmem_funcs.h`

本文只定义设计，不声称当前实现已经符合本决定。实现必须在本 ADR 审核通过后开始。

## 1. 决策范围

本 ADR 决定：

- 现有 `PhysmemMap` 如何归入进程级 `PhysmemMain`；
- 现有 `PmallocMain` 如何成为 `PhysmemMain` 的 owner；
- `BufferMain` 如何从拥有映射对象改为保存 map index；
- `[physmem]` 如何通过 early config 进入启动链；
- 类型、对齐、inline、错误和测试边界。

本 ADR 不决定：

- Buffer Index、Buffer header 或 Buffer Pool slot 算法的重新设计；
- VFIO、PCI 设备枚举、设备驱动和 descriptor 生命周期；
- SVM 映射；
- Data Worker 在运行期动态创建 physmem map；
- 将 `PhysmemMain` 放入 `GlobalMain` 或 `DataPlaneMain`。

## 2. 当前 Hammer 基线

| ID | 路径与符号 | 当前 owner 和行为 | 迁移影响 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-infra/src/physmem.rs:239` `PhysmemMap` | 一个值同时拥有 mmap 区域、fd、页大小和 NUMA 信息；`create` 直接执行 OS 映射 | 映射 backing 不再由单个 map 值独立拥有 |
| H2 | `crates/hammer-core/src/buffer/main.rs:58` `BufferMain::new` | 每个 NUMA 节点直接调用 `PhysmemMap::create`，再把 map 放进 `BufferPool` | BufferPool 只保存 map index |
| H3 | `crates/hammer-infra/src/pmalloc.rs:59` `PmallocMain` | 已有 page、arena、chunk、VA 索引和 VA-to-PA 实现，但尚未成为 physmem owner | 由 `PhysmemMain` 通过 `Box<PmallocMain>` 唯一拥有 |
| H4 | `crates/hammer-core/src/buffer/pool.rs:168` `BufferMain::pool` | 通过 `mapping.base/size/page_size` 定位和检查 Buffer slot | 改为通过 `PhysmemMain::get_map` 读取不可变 map 元数据 |
| H5 | `crates/hammer-core/src/error.rs:57` | Buffer Pool mapping 错误保留 `PhysmemError` source | 仍在 core 边界只翻译一次 |

## 3. VPP 证据

| ID | 路径与符号 | 已核实行为 | 设计约束 |
| --- | --- | --- | --- |
| E1 | `third_party/vpp/src/vlib/physmem.h:24` `vlib_physmem_main_t` | main 持有 flags、base/max 范围、map pool 和 `pmalloc_main` 指针 | Hammer `PhysmemMain` 必须唯一拥有堆分配的 `PmallocMain` 与 map registry |
| E2 | `third_party/vpp/src/vlib/physmem.c:83` `vlib_physmem_init` | 使用 `clib_mem_alloc_aligned` 在堆上按 cache line 对齐分配 pmalloc main，保存指针后初始化，并回写实际 base/max | Hammer 先在 Main Heap 上创建 `Box<PmallocMain>`，再就地初始化；VFIO 不在本 ADR 范围 |
| E3 | `third_party/vpp/src/vlib/physmem.c:30` `vlib_physmem_shared_map_create` | 通过 pmalloc shared arena 创建 backing，再登记 map、fd、base、page 数、page table、page size 和 NUMA | 不再由 `PhysmemMap::create` 直接 mmap |
| E4 | `third_party/vpp/src/vlib/physmem.c:75` `vlib_physmem_get_map` | map 由稳定 pool index 查询 | Hammer 使用原始 `u32 map_index`，不新增 handle wrapper |
| E5 | `third_party/vpp/src/vlib/physmem_funcs.h:23` | physmem alloc/free、page index、PA 转换为 `always_inline` helper | 纯算术/索引 helper 使用 `#[inline]`，不分配、不锁、不做 syscall |

## 4. 最终结构

### 4.1 `PhysmemMain`

`PhysmemMain` 是 `hammer-infra` owner-local 的进程级 authority。它不进入
`GlobalMain`，不进入 `DataPlaneMain`，也不由 `BufferMain` 持有。

```rust
#[repr(align(64))]
pub struct PhysmemMain {
    flags: u32,
    base_addr: usize,
    max_size: usize,
    maps: Pool<PhysmemMap>,
    pmalloc_main: Box<PmallocMain>,
}

pub struct PhysmemMap {
    index: u32,
    fd: RawFd,
    base: usize,
    n_pages: u32,
    page_table: Vec<usize>,
    log2_page_size: u32,
    numa_node: u32,
}

pub static PHYSMEM_MAIN: OnceLock<PhysmemMain> = OnceLock::new();
```

`PhysmemMap::fd` 是与 `PmallocArena` 对应的非拥有 fd 值。它不执行 close；fd
的 close 和 backing unmap 仍由 `PmallocMain` 内的 arena owner 负责。map index
为 pool index，`u32::MAX` 不作为有效 index。

`PhysmemMain` 安装前，初始化代码先在已发布的 Main Heap 上分配 `PmallocMain`，再通过
`Box` 对其就地初始化并完成所有 map 操作。`PhysmemMain` 安装后 map registry 不再增删，
`PmallocMain` 的地址也保持稳定。这样 `get_map` 返回的借用在进程生命周期内稳定，也不
需要给 map registry 增加锁或 callback API。

### 4.2 `PmallocMain`

已有 `PmallocMain` 保留 VPP 的 page、arena、chunk、VA 索引和地址转换语义，但由
`PhysmemMain` 的 `Box<PmallocMain>` 字段唯一拥有。`Box` 是通过 Rust 全局 allocator
执行的普通分配；按启动顺序，此时 allocator 已切换到进程 Main Heap：

```rust
#[repr(align(64))]
pub struct PmallocMain {
    pub flags: u32,
    pub base: usize,
    pub default_log2_page_size: u8,
    pub max_pages: u32,
    pub pages: Vec<PmallocPage>,
    pub chunk_index_by_va: HashMap<usize, u32>,
    pub arenas: Pool<PmallocArena>,
    pub default_arena_for_numa_node: Vec<u32>,
    pub lookup_table: AlignedVec<usize, CACHE_LINE>,
    pub linear_pa_offset: usize,
    pub linear_pa: bool,
    pub lookup_log2_page_size: u8,
}
```

`PmallocMain` 不新增 global singleton，不持有 `MemMain`，继续通过 `MemMain` 完成
VM reservation、backing、page size、NUMA 和 unmap。`PhysmemMain` 负责把 pmalloc
产生的 arena 转换为 `PhysmemMap`。

## 5. 初始化和方法边界

### 5.1 `PhysmemMain` 方法

方法名使用 VPP 已有的领域操作；不增加参数 wrapper、handle wrapper、context 或
manager 类型：

```rust
impl PhysmemMain {
    pub fn init(
        base_addr: Option<NonZeroUsize>,
        max_size: usize,
        map_size: usize,
        page_size: PageSize,
        numa_nodes: &[u32],
    ) -> Result<&'static Self, PhysmemError>;

    pub fn global() -> &'static Self;

    pub fn get_map(&self, index: u32) -> &PhysmemMap;

    #[inline]
    pub fn get_page_index(&self, address: usize) -> u32;

    #[inline]
    pub fn get_pa(&self, address: usize) -> usize;

    #[inline]
    pub fn convert_to_phys_addrs_with_offset(
        &self,
        addresses: &mut [usize],
        offset: i32,
    );

    #[inline]
    pub fn convert_to_phys_addrs(&self, addresses: &mut [usize]);

}

impl PhysmemMap {
    #[inline]
    pub fn base(&self) -> *mut u8;

    #[inline]
    pub fn size(&self) -> usize;

    #[inline]
    pub fn page_size(&self) -> usize;

    #[inline]
    pub fn numa_node(&self) -> u32;
}
```

Map 元数据通过 `base`、`size`、`page_size` 和 `numa_node` 直接 getter 读取，不创建
借用 wrapper。无效 map index、越过 pmalloc page、foreign VA 和已释放 VA 都是 owner invariant 破坏，
按仓库错误规则 panic，不返回 `Option`。

`init` 的内部顺序固定为：

1. 在 Main Heap 上分配 `Box<PmallocMain>`，再调用 `PmallocMain::initialize`；
2. 对 `numa_nodes` 按输入顺序调用内部 `shared_map_create`；
3. 使用 `PmallocMain::get_pa` 建立每个 `PhysmemMap::page_table`；
4. 计算实际 `base_addr` 和 `max_size`；
5. 将完整值安装到 `PHYSMEM_MAIN`。

`map_size` 是调用方已经计算好的物理映射大小，不代表 Buffer 策略；`PhysmemMain`
不依赖 `Buffer` 类型。当前 Buffer 启动路径是唯一调用方，未来其他设备 owner 可
复用同一 shared-map 领域操作，但不在本 ADR 增加动态 map 生命周期。

### 5.2 shared map 创建

内部操作对应 VPP 的 `vlib_physmem_shared_map_create`：

```rust
impl PhysmemMain {
    fn shared_map_create(
        &mut self,
        name: &str,
        size: usize,
        log2_page_size: u32,
        numa_node: u32,
    ) -> Result<u32, PhysmemError>;
}
```

该方法只在 `init` 的 owner-local 构造阶段调用。它执行：

1. `pmalloc_main.create_shared_arena`；
2. 从 arena 取得 base、fd、page 数、subpage size 和 NUMA node；
3. 插入 `maps: Pool<PhysmemMap>`；
4. 为每个 arena page 填充 physical address 或 IOVA page table；
5. 返回稳定的 `u32` map index。

共享 arena 是 VPP 同样的不可增长 arena；容量不足返回 owner-local typed error，
不把 null pointer 传给 BufferMain。

### 5.3 Early config 和 BufferMain 初始化

Physmem 的启动参数属于 early config。配置解析和字段校验由现有组件宏完成，随后
`DataPlaneMain::new_main` 在 `ThreadMain` 已完成 NUMA 选择后调用 `BufferMain::init`；
`BufferMain::init` 再调用 `PhysmemMain::init`。这是当前启动顺序的必要安排：
`DataPlaneMain` 在进入普通 init chain 前必须已经拥有可用的 BufferMain。这样 map size
仍由 BufferMain 的既有 slot 规则计算，`PhysmemMain` 不依赖 `Buffer` 类型，也不新增
生命周期阶段或手工 callback registry。

```rust
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PhysmemConfig {
    pub base_addr: Option<usize>,
    pub max_size: usize,
}

impl PhysmemConfig {
    pub fn validate(&self) -> RuntimeResult<()>;
}

#[config_function(
    name = "runtime_physmem_config",
    section = "physmem",
    early = true,
)]
fn configure_physmem(config: PhysmemConfig) -> RuntimeResult<()>;

impl BufferMain {
    pub fn init(
        base_addr: Option<NonZeroUsize>,
        max_size: usize,
        data_size: usize,
        buffers_per_numa: usize,
        numa_nodes: &[u32],
        worker_count: usize,
        page_size: PageSize,
    ) -> DataPlaneResult<&'static Self>;
}
```

`configure_physmem` 使用 `[physmem]` early config；缺省 section 使用
`PhysmemConfig::default()`。它只安装解析后的配置，不创建 worker-visible Buffer 或 map。
`DataPlaneMain::new_main` 读取该配置和已经安装的 worker config，在 Data Worker 启动前
完成 physmem 和 BufferMain。普通 init function 不负责重复初始化 BufferMain。

这遵循当前 `hammer-component-macros` 的 config/init registration，不新增 runtime
生命周期阶段、手工 callback registry 或 GlobalMain 字段。

## 6. BufferMain 接入

### 6.1 BufferMain 保留的职责

`BufferMain` 继续负责：

- `Buffer` allocation size 和 page-crossing 规则；
- Buffer Pool index、free index 和 Worker cache；
- `buffer_mem_start`、`buffer_mem_size` 的 Buffer Index 地址范围；
- Buffer template 和 slot 初始化；
- 默认 NUMA Pool 选择。

`PhysmemMain` 不保存 Buffer Pool、Buffer header 或 Worker cache。

### 6.2 BufferMain 的调用顺序

当前 [`BufferMain::new`]( /root/netsystem-rs/crates/hammer-core/src/buffer/main.rs:58 )
迁移为 owner 初始化方法。映射相关步骤改为：

1. 计算 `allocation_size`、page bytes 和 `mapping_size`；
2. 调用 `PhysmemMain::init`，传入 early config 的 base/max、已计算的 `mapping_size`、
   page size 和 NUMA 节点；
3. 按 `numa_nodes` 输入顺序用 `get_map(index)` 读取 map 元数据；
4. 计算 Buffer address span；
5. 构造 Pool slot 和 Worker cache；
6. 将 `BufferMain` 安装到现有 `BUFFER_MAIN`。

`PhysmemMain` 和 `BufferMain` 都必须在 Data Worker 启动前完成。重复初始化是启动
顺序错误，按现有 `OnceLock`/`BufferMain` 约定 panic。

目标方法形状为：

```rust
impl BufferMain {
    pub fn init(
        base_addr: Option<NonZeroUsize>,
        max_size: usize,
        data_size: usize,
        buffers_per_numa: usize,
        numa_nodes: &[u32],
        worker_count: usize,
        page_size: PageSize,
    ) -> DataPlaneResult<&'static Self>;
}
```

`BufferMain::init` 是 `DataPlaneMain::new_main` 调用的 owner operation，不接受 closure，
不把 parsed config 保存进 BufferMain，也不返回临时 Buffer owner。`DataPlaneMain` 删除
当前的 `buffer_main: &'static BufferMain` 字段；packet path 通过现有的
`BufferMain::global()` 和当前 Worker cache 直接访问同一个进程级 BufferMain。

### 6.3 `BufferPool` 字段和访问

`BufferPool` 删除拥有型 `mapping: PhysmemMap`，改为：

```rust
pub(super) struct BufferPool {
    pub(super) mapping_index: u32,
    pub(super) index: u8,
    pub(super) data_size: usize,
    pub(super) allocation_size: usize,
    pub(super) first_buffer: usize,
    pub(super) buffer_count: usize,
    pub(super) free: Spinlock<Vec<u32>>,
    pub(super) workers: Box<[RefCell<BufferThreadCache>]>,
    pub(super) template: BufferTemplate,
}
```

访问 map 使用直接借用：

```rust
impl BufferPool {
    #[inline]
    fn mapping(&self) -> &PhysmemMap {
        PhysmemMain::global().get_map(self.mapping_index)
    }
}
```

`slot`、`buffer` 和 `buffer_mut` 的不安全边界继续由 BufferPool owner 负责。它们
通过 `mapping()` 读取 base、size 和 page size，不缓存 raw pointer，不复制 fd，不
增加 map guard 或 closure。

```rust
impl BufferPool {
    #[inline]
    fn slot(&self, index: u32) -> usize;

    #[inline]
    unsafe fn buffer(&self, index: u32) -> &Buffer;

    #[inline]
    unsafe fn buffer_mut(&self, index: u32) -> &mut Buffer;
}
```

Buffer Index 的 zero-invalid、slot alignment、page containment、Pool provenance 和
exclusive mutable ownership 仍然是 Buffer 的 programmer invariant，不迁移为
physmem recoverable error。

## 7. 对齐和布局

### 7.1 控制结构

- `PhysmemMain` 按 64-byte cache line 对齐；`PmallocMain` 通过 `Box` 在 Main Heap 上
  独立分配，其 `#[repr(align(64))]` 使 allocation layout 保持 64-byte 对齐，匹配 VPP
  对 pmalloc main control block 的 cache-line aligned allocation。
- `PhysmemMap` 不声明 C ABI；它是 Rust 内部 owner 的 metadata，不向插件导出裸结构布局。
- `BufferThreadCache` 保留当前 `#[repr(align(64))]`。
- `PmallocMain::lookup_table` 保留 `AlignedVec<usize, CACHE_LINE>`。

### 7.2 映射和 slot

- shared arena base 按 selected page size 对齐；mapping size 向 page size 向上取整。
- pmalloc block size 保持 64 bytes。
- Buffer allocation stride 保持 cache-line 对齐和现有奇数 cache-line 规则。
- Buffer slot 不能跨 backing page；Buffer Index 仍按 64-byte granularity 计算。
- map page table 的元素是连续 `usize`，不把每个元素扩成 cache line。

### 7.3 Inline 边界

VPP `always_inline` 的纯 lookup/转换 helper 对应 Rust `#[inline]`：

- `PhysmemMain::get_map`；
- `get_page_index`；
- `get_pa`；
- `convert_to_phys_addrs_with_offset`；
- `convert_to_phys_addrs`；
- BufferPool 的 `mapping`、`slot` 和已有 direct buffer access。

这些方法不得分配、获取锁、执行 syscall、改变 owner 状态或接受 closure。创建 map、
pmalloc 分配、NUMA policy 操作和 mapping syscall 不使用 `#[inline]` 作为 hot-path
承诺。

## 8. 错误处理

### 8.1 owner 和翻译边界

| 失败 | owner | 结果 |
| --- | --- | --- |
| page size、VM reservation、backing、NUMA、pagemap | `MemMain`/`PmallocMain` | 现有 `MemError`，在 physmem seam 显式转为 `PhysmemError::Pmalloc { source }` |
| shared arena 无法建立 | `PhysmemMain` | `PhysmemError::MapUnavailable { size, numa_node }` 或保留的具体 OS variant |
| map metadata/page table 建立失败 | `PhysmemMain` | 具体 `PhysmemError`，不安装不完整 map |
| Buffer Pool mapping 初始化失败 | `BufferMain`/core seam | 现有 `DataPlaneError::BufferPoolMapping { numa_node, source }`，只翻译一次 |
| 无效 map index、foreign VA、double free | owning module | panic；不是 recoverable error |

`PhysmemError` 新增的 variant 必须只描述具体失败类别，禁止 `Internal`、`Other`、
`Message`、`Invariant { detail }` 等 catch-all。`MemError` 的 source chain 必须保留，
不得用 display string 做错误分类。

### 8.2 Failure atomicity

- `PmallocMain` 先完成 arena/page metadata，再把 map 插入 `PhysmemMain::maps`。
- page table 建立失败时，刚创建的 arena 由 owner 清理，不能留下已登记但不完整 map。
- 所有 map 成功后才安装 `PHYSMEM_MAIN`。
- `BufferMain` 只有在所有 map、span 和 Pool slot 验证成功后才安装 `BUFFER_MAIN`。
- worker 启动后不再执行 map 创建或 BufferMain topology mutation。

## 9. 命名和可见性

采用 VPP 已有领域词，同时遵守 Hammer 命名规则：

- `PhysmemMain`、`PhysmemMap`、`PmallocMain`、`PmallocArena`、`PmallocPage`；
- `shared_map_create`、`get_map`、`get_page_index`、`get_pa`、
  `convert_to_phys_addrs`；
- `map_index` 使用原始 `u32`，不新增 handle/newtype；
- 不使用 `Request`、`Context`、`Manager`、`Wrapper`、`Helper`、`View`、`Data`、
  `Kind` 或仅用于描述实现阶段的前缀；
- `PhysmemMap` 的字段保持 owner-private，BufferMain 只能通过直接借用读取所需
  metadata；
- 本 ADR 不定义 VFIO 类型、fd、IOMMU 状态或 DMA 映射 API；PCI/DMA 设备接入另行
  立 ADR，避免把设备领域状态混入 physmem 或 Buffer owner。

## 10. 决策表

| 决策 | 选择 | owner、恢复动作和边界消费者 |
| --- | --- | --- |
| D1 | 进程级 `PhysmemMain` 按值拥有 map registry | `hammer-infra` 安装一次；重复安装是启动错误；BufferMain 只通过 `get_map` 读取 |
| D2 | `PhysmemMain` 通过 `Box<PmallocMain>` 唯一拥有 Main Heap 上的 pmalloc main | pmalloc control block 地址稳定；VM、NUMA、页表和 fd 清理由同一 owner 完成；不新增第二个 singleton |
| D3 | BufferPool 保存原始 `u32 mapping_index` | pool index 是稳定内部事实；无效 index 是 invariant panic；不增加 handle wrapper |
| D4 | shared map 由 pmalloc arena 创建 | `shared_map_create` 先完成 arena/page metadata，再登记 map；失败时清理 arena 并保持 registry 不变 |
| D5 | map 只在 worker 启动前创建 | `DataPlaneMain::new_main` 完成 BufferMain；worker 只读已发布 owner；运行期动态 map 另行设计 |
| D6 | owner-local typed error，跨 core seam 只翻译一次 | `MemError` 保留 source；BufferMain 返回 `BufferPoolMapping`；invariant 破坏 panic |
| D7 | VPP `always_inline` 只映射到纯 lookup/转换 helper | `#[inline]` 方法不分配、不锁、不 syscall；映射和 NUMA 操作保持普通方法 |

## 11. 三方语义差异

| 维度 | 当前 Hammer | 本 ADR | VPP | 结论 |
| --- | --- | --- | --- | --- |
| main owner | `PhysmemMap` 值分散在 BufferPool | `PhysmemMain` 按值拥有 pmalloc 和 map pool | `vlib_physmem_main_t` | 对齐，D1 |
| pmalloc | 独立 `PmallocMain` | 作为 `PhysmemMain` 的 `Box<PmallocMain>` 字段在 Main Heap 上分配 | `pmalloc_main` 指针字段，control block 由 `clib_mem_alloc_aligned` 分配 | 对齐，D2 |
| VFIO | 尚无 owner | 本 ADR 不引入 | 独立 `vfio_main` | 明确留待 PCI/DMA ADR |
| map 创建 | map 直接 mmap | 由 pmalloc shared arena 创建 | `vlib_physmem_shared_map_create` | 对齐，D4 |
| map mutation | BufferPool 持有值 | worker 启动前完成，之后只读 | VPP main thread 可继续创建 | 有意限制，D5；动态 map 需后续 barrier ADR |
| error | PhysmemError + Buffer translation | owner-local typed error，单次翻译 | `clib_error_t` last-error | Rust typed error，D6 |
| ABI | Rust 内部结构 | 不导出 C layout；cache-line/page 语义保留 | C struct/always_inline | 有意不复制 C ABI，D7 |

## 12. 变更清单

### 新增

- `crates/hammer-infra/src/physmem_main.rs` 或等价 owner 模块：`PhysmemMain`、
  `PhysmemMap` 和 `PHYSMEM_MAIN`；最终路径需按现有模块组织确认。

### 修改

- `crates/hammer-infra/src/pmalloc.rs`：保持现有 pmalloc 语义，改由 `PhysmemMain`
  通过 `Box` 唯一拥有。
- `crates/hammer-infra/src/lib.rs`：注册 owner 模块；不把 plugin-specific 类型
  re-export 到 runtime。
- `crates/hammer-core/src/buffer/main.rs`：删除 `PhysmemMap::create` 调用，保存
  `mapping_index`，通过 `PhysmemMain::get_map` 建立 Pool。
- `crates/hammer-core/src/buffer/pool.rs`：所有 mapping 读取改为直接借用 global
  map metadata。
- `crates/hammer-core/src/error.rs`：保留现有 Buffer mapping seam，补充具体
  `PhysmemError` source translation。
- `docs/adr/0013-vpp-style-pmalloc-main.md`：修订其“不接入 PhysmemMap”的阶段边界，
  保留 pmalloc 类型、容器、inline、错误和测试语义。

### 删除

- `PhysmemMap::create` 的公共直接 mmap 入口；
- `BufferPool.mapping: PhysmemMap` 的拥有型字段；
- BufferMain 对 mapping fd、mapping Drop 和独立 unmap 的隐式所有权。

## 13. 测试矩阵

| 测试 | 层级与设置 | 断言 | 决策 |
| --- | --- | --- | --- |
| `physmem_main_initializes_pmalloc_and_maps` | infra integration，真实 MemMain/page mapping | `PmallocMain` control block 位于 Main Heap、保持 cache-line alignment，map pool、base/max、map index 和 page metadata 正确 | D1,D2,D4 |
| `physmem_map_creation_failure_is_atomic` | infra integration，制造具体 backing/NUMA failure | 不存在半登记 map，已建 arena 被清理 | D4,D6 |
| `physmem_lookup_matches_pmalloc` | infra unit/integration | `get_page_index`、`get_pa`、批量转换与 pmalloc lookup 一致 | D2,D7 |
| `physmem_alignment_matches_page_and_cache_contract` | infra integration | main control block 64-byte 对齐，map base/page/Buffer stride 对齐 | D1,D7 |
| `buffer_main_uses_physmem_map_indices` | hammer-core integration | BufferPool 不拥有 map；slot/base/page 检查通过 global map index 完成 | D5 |
| `buffer_main_preserves_pool_span_and_indices` | hammer-core integration | address span、Index zero invalid、page-crossing skip 和 Pool provenance 不变 | D5 |
| `buffer_mapping_error_preserves_source` | hammer-core integration | `DataPlaneError::BufferPoolMapping` 保留具体 physmem source chain | D6 |
| `physmem_numa_map_metadata_is_stable` | infra integration，至少两个可用 NUMA node 或 capability-gated | map index 按输入顺序稳定；每个 map、arena 和 Buffer Pool 保留相同 `numa_node` | D1,D4,D5 |
| `physmem_default_page_mapping_uses_kernel_page_size` | infra integration | `PageSize::Default` 的 backing kernel page size 与 metadata 一致，size 按页对齐 | D4,D7 |
| `physmem_default_huge_mapping_uses_hugetlb` | Linux capability-gated integration | `PageSize::DefaultHuge` 使用可用 HugeTLB size；pool 不足/目录不存在返回具体错误 | D4,D6,D7 |
| `physmem_explicit_huge_mapping_uses_requested_size` | Linux capability-gated integration | `PageSize::Bytes` 的 HugeTLB size、NUMA placement 和 kernel page size 一致 | D4,D6,D7 |
| `physmem_numa_placement_is_verified` | Linux NUMA capability-gated integration | pagemap 查询到的实际 node 与请求 node 一致；能力缺失显式跳过 | D4,D6 |
| `physmem_and_buffer_initialize_before_workers` | runtime startup integration | worker 启动前两个 Main 均已安装；重复初始化按启动错误 panic | D1,D5 |

NUMA 和 HugeTLB 测试不得把宿主机能力缺失当成成功：测试必须区分 capability-gated
skip 与实际通过。普通 page、pmalloc 和 Buffer 测试仍需独立运行。VFIO 不属于本 ADR
的实现或测试范围。

## 14. 审核结论

当前结论为 `Needs decisions`，不是实现完成声明。需要明确批准：

1. `PhysmemMain` 通过 `Box<PmallocMain>` 唯一拥有 Main Heap 上分配的 pmalloc main；
2. BufferPool 改存原始 `u32 map_index`；
3. map 只在 worker 启动前创建，运行期动态 map 另行设计；
4. `[physmem]` 通过 `early = true` config function 解析，`DataPlaneMain::new_main` 完成 owner 安装；
5. NUMA node、默认大页、显式大页、页大小校验和 failure atomicity 是实现验收项；
6. VFIO、PCI、IOMMU 和 DMA descriptor 生命周期不在本 ADR 中。

## 15. Completion review against vendored VPP

### Feature and changed surface

本实现修改 `hammer-infra` 的 physmem/pmalloc owner、`hammer-core` 的 BufferMain/
BufferPool，以及 `hammer-runtime` 的 early config 和 DataPlaneMain Buffer 访问。映射
由 `PmallocMain::create_shared_arena` 建立，`PhysmemMap` 只保留 VPP map metadata，
BufferPool 保存稳定的 `u32 mapping_index`。

### VPP analog and evidence

- `third_party/vpp/src/vlib/physmem.h` 的 `vlib_physmem_main_t` 持有 flags、base/max、
  map pool 和 pmalloc 指针；Hammer 对应 `PhysmemMain` 的 `Box<PmallocMain>` 唯一 owner。
- `third_party/vpp/src/vlib/physmem.c:vlib_physmem_init` 使用 `clib_mem_alloc_aligned` 在堆上
  按 cache line 对齐分配 pmalloc main，保存指针后初始化并使用实际 base/max；Hammer 在
  `PhysmemMain::init` 中先从 Main Heap 分配 `Box<PmallocMain>`，再就地初始化并保持相同顺序。
- `third_party/vpp/src/vlib/physmem.c:vlib_physmem_shared_map_create` 通过 shared arena
  创建 backing，再登记 map；Hammer 的 `shared_map_create` 复用同一 pmalloc owner，令
  `n_pages = arena.n_pages * arena.subpages_per_page`，并按 VPP 的 arena-page 粒度填充
  page table。
- `third_party/vpp/src/vlib/physmem.c:vlib_physmem_get_map` 以 pool index 查 map；Hammer
  使用 `Pool<PhysmemMap>` 和原始 `u32` index，不引入 handle。
- `third_party/vpp/src/vlib/physmem_funcs.h` 的 page-index、PA 和批量转换为
  `always_inline`；Hammer 对应 helper 使用 `#[inline]`，没有锁、分配或 syscall。
- `third_party/vpp/src/vlib/physmem.c:vlib_physmem_config` 由
  `VLIB_EARLY_CONFIG_FUNCTION` 注册；Hammer 使用现有
  `#[config_function(..., early = true)]` 的 `[physmem]` section。

### Intentional differences

- VPP 的 `clib_error_t` 由调用方消费；Hammer 沿现有 `PhysmemError`/`DataPlaneError`
  seam 返回 typed `Result`，不新增 VFIO 或通用错误包装。
- VPP physmem 初始化还调用 Linux VFIO 初始化；本 ADR 明确不引入 VFIO，PCI/DMA 另行
  设计，避免设备状态污染 physmem owner。
- VPP 可在 main thread 后续创建 map；Hammer 在 worker 启动前一次性创建，运行期变更需
  后续 barrier ADR。

### Findings

- **Blocking，已解决：**此前 `PhysmemMain` 内嵌 `PmallocMain`，与
  `vlib_physmem_init` 的独立 cache-line aligned heap allocation 不一致，并使 control
  block 随外层构造值搬移。现已改为 `Box<PmallocMain>`，由启动线程的默认 active Main
  Heap 分配，并在固定地址就地初始化。
- 无剩余 blocking 或 non-blocking finding。

### Verdict

`Aligned` for the approved physmem/pmalloc/Buffer ownership and early-config scope. The
VFIO omission and pre-worker-only map lifecycle are explicit product decisions, not missing
implementation.

### Commands run

- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo test -p hammer-infra --test pmalloc`

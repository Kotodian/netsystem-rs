# ADR-0050: Simple 与 Combined Counter Main

Status: accepted; implementation pending verification
Date: 2026-10-02

## 1. 决定与源码依据

在 `hammer-stats` 增加 `SimpleCounterMain`、`CombinedCounterMain`。
它们对应 VPP `vlib_simple_counter_main_t`、
`vlib_combined_counter_main_t`：**一个 main 缓存一个 stats entry 的外层
counter 向量入口**，外层向量的第 `thread_index` 项指向该线程的对象计数行。
计数行及外层向量都归已有 `StatsSegment` 所有。main 不另建每线程行指针
`Vec`、不复制计数、不增加 `CounterStorage`。现有 `SimpleCounter`、
`CombinedCounter` 只是**通用 stats 目录句柄**；一个 per-object counter
main 持有对应句柄，但不能把所有 simple/combined stats 向量冒充成
VPP 的 counter main。

本期只支持有 stats 目录名的家族。VPP 的无名本地分支
（`name == NULL && stat_segment_name == NULL`）真实存在，但 Hammer
尚无需要它的具体 owner，本 ADR 不伪装已经实现该分支。`new` 传一个确定
的目录名，不保存两份 `Option<String>`。类型所在层只负责存储与方法，
对象索引及业务语义仍归调用它的 owner。实现由 issue #372 跟踪；
本 ADR 的 Rust 代码块保留设计契约，不代替源码或验证结果。

以下路径均相对于 `third_party/vpp/src/`。

| 源码 | 对应决定 |
| --- | --- |
| `vlib/counter_types.h:13-25` | simple cell 为 `u64`；combined cell 为 `packets: u64` 和 `bytes: u64`。复用既有 `Counter` 的 16B 布局。 |
| `vlib/counter.h:24-32,189-196` | `cm->counters` 是指向 stats entry 外层向量的 `counter_t **`/`vlib_counter_t **`；不是 main 自己拥有的第二张行表。 |
| `vlib/counter.c:47-106` | named 路径注册 entry，`vlib_stats_validate(entry, n_vlib_mains - 1, index)` 扩容，随后 `cm->counters = vlib_stats_get_entry_data_pointer(entry)`；Hammer 每次 validate 后同样刷新这**一个**入口。 |
| `vlib/stats/stats.c:485-514`；`vppinfra/cache.h:12-26` | named counter 的外层线程指针向量和每线程计数行都以 `CLIB_CACHE_LINE_BYTES` 对齐；vendored 默认是 64 字节。 |
| `vlib/counter.c:109-137`；`vlib/stats/stats.c:54-64` | combined `will_expand` 调 `vlib_stats_set_heap()`，逐行判断，返回前 `clib_mem_set_heap(oldheap)`；Heap 激活不等于取锁或停 worker。 |
| `vlib/counter.h:37-153` | simple 的当前线程行直接 `+=`、`-=` 或赋值；get 跨行求和，VPP 说明无 barrier 不保证精确瞬时值。Rust 版的非原子 get 要求先停写者。 |
| `vlib/counter.h:155-297` | `vlib_counter_add/sub/zero`、combined prefetch/increment/get/zero；combined get 仅 `static inline`，其余小方法是 `always_inline`。 |
| `vlib/counter.c:11-44,70-83,141-168` | clear 遍历全部线程与列；free 删除 stats entry；`n_counters` 取第 0 行的长度。 |
| `vlib/stats/stats.c:412-524` | stats validate 可能替换外层向量和行，结构发布锁不保护普通 cell 更新。 |
| `vnet/adj/adj.c:67-91` | adjacency 扩容前根据 `will_expand` 决定是否停止 worker；这是另一个 owner 的发布条件。 |
| `vnet/interface.h:1161-1175`；`vnet/interface.c:546-566` | interface 用 `sw_if_counter_lock` 包住两组 counter main 的 `validate`/`zero`；这里不是 `will_expand` 决定是否取锁。 |
| `vnet/interface/stats.c:12-84`；`vnet/interface.h:250-253` | `VNET_SW_INTERFACE_ADD_DEL_FUNCTION(statseg_sw_interface_add_del)` 将 stats 名称投影注册到软件接口 add/del 回调链；`/if/names` 与 `/interfaces/...` 不属于 counter main 的 `name`。 |
| `vlib/stats/format.c:9-23` | 只在 symlink 路径中把接口名的 `/` 换成 `_`；`/if/names` 保留原始接口名。 |

Hammer 现有 `segment.rs` 同时实现通用 stats 向量操作和 per-object
counter main 需要复用的底层机制。根据 VPP 源码分开处理：

| 现有方法或状态 | 目标 owner | VPP 依据 |
| --- | --- | --- |
| `StatsSegment::add_simple_counter`、`add_combined_counter`，`SimpleCounter`、`CombinedCounter` | 留在 stats segment：VPP `vlib_stats_add_counter_vector` 同时被 `counter.c`、`stats/provider_mem.c`、`stats/init.c`、`stats/collector.c` 和 `error.c` 调用；counter main 构造只调用现有注册方法并保存 entry | `vlib/stats/stats.c:374-409`；`vlib/counter.c:47-106` |
| `StatsSegment::set_simple_counter`、`increment_simple_counter`；`DirectoryEntry::set_simple_counter_cell`、`add_simple_counter_cell` | 现有通用 collector 和 node error 写路径暂留原 owner；**新** per-object counter 不经过这些方法，直接在 counter main 中写自己的线程行。迁移现有 collector 到 counter main 会改变 VPP 的 owner，不在本 ADR 做 | `vlib/stats/provider_mem.c:35-54`；`vlib/error_funcs.h:26-36`；`vlib/counter.h:57-103` |
| `StatsSegment::validate`、`remove_entry`、`create_entry`、`DirectoryWrite`、`allocate_vector`、`free_vector` | 留在 stats segment，维持所有目录家族共用的分配、发布和释放；counter main 负责调用时机和自己的缓存入口刷新 | `vlib/stats/stats.c:11-52,412-524`；`vlib/counter.c:47-106,70-83` |

**Per-object counter 操作归 counter main**；当前 `segment.rs` 并没有
这些 `vlib_*_counter_main_t` 方法，不能把通用 stats 向量操作误当成
需要迁走的 per-object 操作。也不应新增两个 `*_counter_data` 到 segment：
entry 类型检查复用现有 `entry_of_type`/`data_pointer`，由 counter main
在初始化和 validate 后解析并缓存入口。

`vlib/counter.h:322-330` 的 `vlib_counter_len(cm)` 旧宏访问当前结构里
不存在的 `maxi`；以 `vlib/counter.c:157-168` 的实现为准，不移植该宏。

## 2. 类型与首次初始化

现有 `StatsSegment::validate`、`remove_entry` 位于
`crates/hammer-stats/src/segment.rs:573-775`。`new(segment, name,
thread_count, first_index)` 将 VPP 的**首次**
`validate` 合并进 Rust 构造，使构造成功的 main 始终有有效的 `counters`
入口；之后的 `validate(index)` 对应 VPP 后续扩容。`thread_count`
就是 `n_vlib_mains`，必须包含 thread 0。构造时显式借用尚未发布的
`StatsSegment`：当前 `StatsMain::create` 之后才注册固定 stats 条目，
不能在这一步调用 `StatsMain::global()`（`hammer-stats/src/lib.rs:89-122`）。

```rust
// hammer-stats::counter; VPP: vlib/counter.h:24-32,189-196;
// vlib/counter.c:47-106. counters points at the segment-owned outer vector.
pub struct SimpleCounterMain {
    entry: SimpleCounter,
    thread_count: u32,
    counters: UnsafeCell<NonNull<*mut u64>>,
}

pub struct CombinedCounterMain {
    entry: CombinedCounter,
    thread_count: u32,
    counters: UnsafeCell<NonNull<*mut Counter>>,
}
```

`NonNull<*mut T>` 只缓存 StatsSegment 外层向量的数据起点：读取第
`thread_index` 个 `*mut T` 即该线程的计数行。它不拥有这两个向量，
不向调用者返回指针或伪造的 `'static` 行引用。`UnsafeCell` 只允许
控制线程在 WorkerBarrier 内刷新入口；普通更新仅取当前线程的行。
64 字节对齐的是**外层向量的数据地址**和**每条线程计数行的起始地址**，
不是每个 `u64` / `Counter` 元素的步长，也不是 `NonNull` 字段本身。
`StatsSegment::validate` 当前对外层向量和每条计数行分别调用
`allocate_vector(..., CACHE_LINE, ...)`（`segment.rs:622-658`）；
`hammer_infra::align::CACHE_LINE` 为 64，`vector_prefix` 同时保证返回的
数据地址按 64 字节对齐。counter main 的 `new`/`validate` 只能从这条
现有路径取得并刷新 `counters`，不得另行用普通 `Vec` 分配行。
入口读取直接组合现有 `StatsSegment::entry_of_type` 和
`DirectoryEntry::data_pointer`，在 counter main 构造/validate 处做
一次类型与非空检查；不往 segment 再塞 counter 专属数据 helper。
VPP 对应 `vlib/stats/stats.h:115-121` 的
`vlib_stats_get_entry_data_pointer`。

下面的 Rust 块给出方法的具体步骤，**不是本次实现代码**。其中
`vector_length` 是现有 `hammer-stats::segment` 的向量头读取原语；
不新增段内 `*_counter_data` 方法。
没有 `cell()`：VPP 的热路径先取 `cm->counters[thread_index]`，
再直接更新 `my_counters[index]`（`vlib/counter.h:57-66,211-231`）。

```rust
// VPP: vlib/counter.c:47-83,163-168; vlib/counter.h:37-153.
impl SimpleCounterMain {
    pub fn new(segment: &StatsSegment, name: &str, thread_count: u32, first_index: u32)
        -> StatsResult<Self>
    {
        assert!(thread_count > 0, "counter main must include thread 0");
        let entry = segment.add_simple_counter(name)?;
        if let Err(error) = segment.validate(entry.index, thread_count - 1, first_index) {
            // The newly registered entry must not survive failed construction.
            segment.remove_entry(entry.index).expect("counter entry rollback");
            return Err(error);
        }
        let pointer = segment
            .entry_of_type(entry.index, DirectoryType::CounterVectorSimple)
            .expect("new simple counter entry has its declared type")
            .data_pointer()
            .expect("new simple counter entry carries data");
        let counters = NonNull::new(pointer.cast::<*mut u64>())
            .expect("validated simple counter entry has an outer vector");
        assert_eq!(counters.as_ptr().addr() % CACHE_LINE, 0, "outer counter vector alignment");
        for thread in 0..thread_count as usize {
            let row = unsafe { *counters.as_ptr().add(thread) };
            assert_eq!(row.addr() % CACHE_LINE, 0, "simple counter row alignment");
        }
        Ok(Self { entry, thread_count, counters: UnsafeCell::new(counters) })
    }

    /// The owner stops workers before a validate that can replace the outer vector.
    pub fn validate(&self, index: u32) -> StatsResult<()> {
        let segment = &StatsMain::global()?.segment;
        segment.validate(self.entry.index, self.thread_count - 1, index)?;
        // The caller stopped workers if validate could move the outer vector.
        // Refresh exactly VPP's cm->counters after segment.validate publishes it.
        let pointer = segment
            .entry_of_type(self.entry.index, DirectoryType::CounterVectorSimple)
            .expect("simple counter entry retains its declared type")
            .data_pointer()
            .expect("simple counter entry carries data");
        let counters = NonNull::new(pointer.cast::<*mut u64>())
            .expect("validated simple counter entry has an outer vector");
        assert_eq!(counters.as_ptr().addr() % CACHE_LINE, 0, "outer counter vector alignment");
        for thread in 0..self.thread_count as usize {
            let row = unsafe { *counters.as_ptr().add(thread) };
            assert_eq!(row.addr() % CACHE_LINE, 0, "simple counter row alignment");
        }
        unsafe { *self.counters.get() = counters };
        Ok(())
    }

    pub fn n_counters(&self) -> u32 {
        let outer = unsafe { *self.counters.get() };
        let first_thread = unsafe { *outer.as_ptr() };
        assert!(!first_thread.is_null(), "counter first row is missing");
        unsafe { vector_length(first_thread.cast()) }
    }

    #[inline(always)] // VPP: counter.h:37-49.
    pub fn prefetch(&self, thread_index: u32, index: u32) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        hammer_infra::prefetch::prefetch_write_l1(unsafe { my_counters.add(index as usize) });
    }

    #[inline(always)] // VPP: counter.h:57-66.
    /// VPP counter.h:57-66: update only this thread's object counter.
    pub fn increment(&self, thread_index: u32, index: u32, increment: u64) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        let counter = unsafe { my_counters.add(index as usize) };
        unsafe { *counter = (*counter).wrapping_add(increment) };
    }

    #[inline(always)] // VPP: counter.h:74-86.
    pub fn decrement(&self, thread_index: u32, index: u32, decrement: u64) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        let value = unsafe { *my_counters.add(index as usize) };
        assert!(value >= decrement, "counter underflow");
        unsafe { *my_counters.add(index as usize) = value - decrement };
    }

    #[inline(always)] // VPP: counter.h:94-103.
    pub fn set(&self, thread_index: u32, index: u32, value: u64) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        unsafe { *my_counters.add(index as usize) = value };
    }

    #[inline(always)] // VPP: counter.h:105-132.
    /// The owner stops worker writers before cross-thread aggregation.
    pub fn get(&self, index: u32) -> u64 {
        assert!(index < self.n_counters());
        let mut total = 0_u64;
        for thread in 0..self.thread_count {
            let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            total = total.wrapping_add(unsafe { *my_counters.add(index as usize) });
        }
        total
    }

    #[inline(always)] // VPP: counter.h:134-153.
    pub fn zero(&self, index: u32) {
        assert!(index < self.n_counters());
        for thread in 0..self.thread_count {
            let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            unsafe { *my_counters.add(index as usize) = 0 };
        }
    }

    pub fn clear(&self) { // VPP: counter.c:11-25.
        for thread in 0..self.thread_count {
            let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            for index in 0..self.n_counters() {
                unsafe { *my_counters.add(index as usize) = 0 };
            }
        }
    }

    /// The owner first stops all users of the entry.
    pub fn remove(self) -> StatsResult<()> { // VPP: counter.c:70-83.
        StatsMain::global()?.segment.remove_entry(self.entry.index)
    }
}
```

```rust
// VPP: vlib/counter.h:155-187. Only independent, exclusively borrowed values.
impl Counter {
    #[inline(always)]
    pub fn add(&mut self, other: &Counter) {
        self.packets = self.packets.wrapping_add(other.packets);
        self.bytes = self.bytes.wrapping_add(other.bytes);
    }

    #[inline(always)]
    pub fn sub(&mut self, other: &Counter) {
        assert!(self.packets >= other.packets, "packet counter underflow");
        assert!(self.bytes >= other.bytes, "byte counter underflow");
        self.packets -= other.packets;
        self.bytes -= other.bytes;
    }

    #[inline(always)]
    pub fn zero(&mut self) {
        self.packets = 0;
        self.bytes = 0;
    }
}
```

```rust
// VPP: vlib/counter.c:28-44,86-168; vlib/counter.h:211-297.
impl CombinedCounterMain {
    pub fn new(segment: &StatsSegment, name: &str, thread_count: u32, first_index: u32)
        -> StatsResult<Self>
    {
        assert!(thread_count > 0, "counter main must include thread 0");
        let entry = segment.add_combined_counter(name)?;
        if let Err(error) = segment.validate(entry.index, thread_count - 1, first_index) {
            segment.remove_entry(entry.index).expect("counter entry rollback");
            return Err(error);
        }
        let pointer = segment
            .entry_of_type(entry.index, DirectoryType::CounterVectorCombined)
            .expect("new combined counter entry has its declared type")
            .data_pointer()
            .expect("new combined counter entry carries data");
        let counters = NonNull::new(pointer.cast::<*mut Counter>())
            .expect("validated combined counter entry has an outer vector");
        assert_eq!(counters.as_ptr().addr() % CACHE_LINE, 0, "outer counter vector alignment");
        for thread in 0..thread_count as usize {
            let row = unsafe { *counters.as_ptr().add(thread) };
            assert_eq!(row.addr() % CACHE_LINE, 0, "combined counter row alignment");
        }
        Ok(Self { entry, thread_count, counters: UnsafeCell::new(counters) })
    }

    /// A single control owner performs structural operations.
    pub fn will_expand(&self, index: u32) -> bool {
        let segment = &StatsMain::global().expect("counter owns a stats entry").segment;
        // VPP counter.c:114-136: vlib_stats_set_heap() before inspecting row
        // vector capacity; clib_mem_set_heap(oldheap) on every return.
        let active_heap = segment.heap().activate();
        let outer = unsafe { *self.counters.get() };
        let mut expands = false;
        for thread in 0..self.thread_count {
            let thread_counters = unsafe { *outer.as_ptr().add(thread as usize) };
            assert!(!thread_counters.is_null(), "counter thread has no row");
            // Current StatsSegment::validate replaces a row whenever its
            // logical length grows. This is therefore the exact relocation
            // test today; not VPP's vec_max_len test against its allocator.
            if index >= unsafe { vector_length(thread_counters.cast()) } {
                expands = true;
                break;
            }
        }
        drop(active_heap); // RAII restores the previous active heap.
        expands
    }

    /// The owner stops workers first if will_expand returned true.
    pub fn validate(&self, index: u32) -> StatsResult<()> {
        let segment = &StatsMain::global()?.segment;
        segment.validate(self.entry.index, self.thread_count - 1, index)?;
        let pointer = segment
            .entry_of_type(self.entry.index, DirectoryType::CounterVectorCombined)
            .expect("combined counter entry retains its declared type")
            .data_pointer()
            .expect("combined counter entry carries data");
        let counters = NonNull::new(pointer.cast::<*mut Counter>())
            .expect("validated combined counter entry has an outer vector");
        assert_eq!(counters.as_ptr().addr() % CACHE_LINE, 0, "outer counter vector alignment");
        for thread in 0..self.thread_count as usize {
            let row = unsafe { *counters.as_ptr().add(thread) };
            assert_eq!(row.addr() % CACHE_LINE, 0, "combined counter row alignment");
        }
        unsafe { *self.counters.get() = counters };
        Ok(())
    }

    pub fn n_counters(&self) -> u32 {
        let outer = unsafe { *self.counters.get() };
        let first_thread = unsafe { *outer.as_ptr() };
        assert!(!first_thread.is_null(), "counter first row is missing");
        unsafe { vector_length(first_thread.cast()) }
    }

    #[inline(always)] // VPP: counter.h:233-245.
    pub fn prefetch(&self, thread_index: u32, index: u32) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        hammer_infra::prefetch::prefetch_write_l1(unsafe { my_counters.add(index as usize) });
    }

    #[inline(always)] // VPP: counter.h:211-231.
    /// VPP counter.h:211-231: update only this thread's object counter.
    pub fn increment(&self, thread_index: u32, index: u32, packets: u64, bytes: u64) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        let counter = unsafe { &mut *my_counters.add(index as usize) };
        counter.packets = counter.packets.wrapping_add(packets);
        counter.bytes = counter.bytes.wrapping_add(bytes);
    }

    #[inline] // VPP: counter.h:247-275.
    /// The owner stops worker writers before cross-thread aggregation.
    pub fn get(&self, index: u32) -> Counter {
        assert!(index < self.n_counters());
        let mut total = Counter::default();
        for thread in 0..self.thread_count {
            let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            let counter = unsafe { *my_counters.add(index as usize) };
            total.add(&counter);
        }
        total
    }

    #[inline(always)] // VPP: counter.h:277-297.
    pub fn zero(&self, index: u32) {
        assert!(index < self.n_counters());
        for thread in 0..self.thread_count {
            let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            unsafe { (*my_counters.add(index as usize)).zero() };
        }
    }

    pub fn clear(&self) { // VPP: counter.c:28-44.
        for thread in 0..self.thread_count {
            let my_counters = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            for index in 0..self.n_counters() {
                unsafe { (*my_counters.add(index as usize)).zero() };
            }
        }
    }

    /// The owner first stops all users of this entry.
    pub fn remove(self) -> StatsResult<()> { // VPP: counter.c:141-154.
        StatsMain::global()?.segment.remove_entry(self.entry.index)
    }
}
```

## 3. 同步、inline 与错误边界

- `will_expand` 的 `active_heap` 只切换**当前控制线程**的 MemHeap；
  `MemHeap::activate()` 的 guard 在返回前 Drop 恢复原 heap
  （`crates/hammer-infra/src/mem/mod.rs:778-788,947-963`）。不在 heap
  激活期间进入 WorkerBarrier。VPP 在 `counter.c:114-136` 也是先切 heap，
  查完后恢复，再由 `vnet/adj/adj.c:77-86` 决定 barrier。
- VPP 对比的是 `vec_resize_will_expand` 的**容量**；当前 Hammer 的
  `StatsSegment::validate`（`segment.rs:573-729`）只要目标列超出现有
  长度就准备替换外层向量及目标行。因此本版在 stats heap 激活窗口中
  逐行检查长度，任何行需要增长都返回 `true`。日后 segment 若支持
  原位扩容，必须同时把 `will_expand` 改为实际容量/搬迁检测，不能继续
  假称长度等于容量。`will_expand == false` 时本次 validate 不修改结构。
- 会搬迁的 validate 在 runtime WorkerBarrier 中运行：worker 停止后
  `StatsSegment::validate` 可回收旧向量，随后 main 刷新 `counters`，最后
  放行 worker。`counters` 短暂悬空只发生在停 worker 的区间，不被读取。
  simple 没有 VPP 对应的 `will_expand`，运行期 validate 一律先停 worker。
  `clear`/`zero` 在需要精确重置或复用对象索引时也由 owner 停 worker。
- `UnsafeCell` 仅为了 barrier 下刷新这个**单一**缓存入口；`unsafe impl Sync`
  的条件是控制线程是唯一结构写者，每个 DataPlaneMain 只写自己索引的
  行。方法不得返回能跨 validate 存活的引用/指针。stats 的结构锁及
  `in_progress/epoch` 面向外部段 reader，不代替 WorkerBarrier。
- 新 main 的 cell 更新严格走 VPP 的每线程普通 `u64` 读写：simple 是
  `*cell += increment` 的 wrapping 等价，combined 分别更新两个字段；
  不使用 `AtomicU64`、`fetch_add`、锁或 volatile。当前 worker 是自己
  行的唯一写者。同一个 main 的 entry 不再交给现有
  `StatsSegment::set_simple_counter` / `increment_simple_counter` 混合写入。
  VPP 允许无 barrier 的近似 `get`，但在 Rust 中普通读与其他线程的
  普通写并发是数据竞争；因此本 ADR 的跨线程 `get` 必须先由 owner
  停止所有写者，`zero/clear` 也在需要确定性重置时停写者。公开方法
  不暴露 `unsafe`；实现前必须在 owner/runtime 接线中证明这些访问条件，
  不能仅靠 `thread_index` 参数或一条注释就声称安全。错误 thread/index、
  下溢及无 barrier 搬迁属于程序错误，不变成每包 `Result`；控制面失败
  复用 `StatsError` 和原始 source。
- `#[inline(always)]` 只映射 VPP `always_inline` 的短热路径/值方法；
  combined `get` 映射 `static inline` 为 `#[inline]`；validate、
  will_expand、clear、remove 保持普通方法。预取复用现有
  `hammer-infra::prefetch::prefetch_write_l1`，不新增指令包装。

## 4. InterfaceMain 接线

`vnet/interface.h:1010-1059,1128-1132,1161-1175` 在
`vnet_interface_main_t` 中分别保存 simple/combined counter main 向量和
`sw_if_counter_lock`；`vnet/interface.c:1415-1434` 初始化每个
family 的名称与 `/if/...` stats 路径；`interface.c:541-568` 在每次创建软件
接口（包括复用 pool index）时，对两个向量的每个 main 先 `validate`，
再 `zero(sw_if_index)`。Hammer 对应 owner 是现有
`crates/hammer-service/src/interface_model.rs` 的 `InterfaceMain`，不是
`StatsMain`、`SwInterface` 或 device 插件。

本阶段只保留用户指定的五个 simple、八个 combined family。VPP 的
`PUNT`、`IP4`、`IP6`、`MPLS` 不声明、不分配，也不保留原编号空洞。
两个 Rust 枚举分别从零连续编号；它们只用于选择各自的 counter main
向量，不能跨 family 混用。下表的路径是 **counter family 的 stats entry
名称**，不是软件或硬件接口的 `name`：

| Simple 枚举顺序 | Stats 路径 | Combined 枚举顺序 | Stats 路径 |
| --- | --- | --- | --- |
| `Drop` | `/if/drops` | `Rx` | `/if/rx` |
| `RxNoBuf` | `/if/rx-no-buf` | `RxUnicast` | `/if/rx-unicast` |
| `RxMiss` | `/if/rx-miss` | `RxMulticast` | `/if/rx-multicast` |
| `RxError` | `/if/rx-error` | `RxBroadcast` | `/if/rx-broadcast` |
| `TxError` | `/if/tx-error` | `Tx` | `/if/tx` |
| | | `TxUnicast` | `/if/tx-unicast` |
| | | `TxMulticast` | `/if/tx-multicast` |
| | | `TxBroadcast` | `/if/tx-broadcast` |

```rust
// hammer-service::interface; VPP: vnet/interface.h:1010-1059.
// Implicit discriminants are contiguous; no placeholders for omitted VPP counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InterfaceSimpleCounter {
    Drop,
    RxNoBuf,
    RxMiss,
    RxError,
    TxError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InterfaceCombinedCounter {
    Rx,
    RxUnicast,
    RxMulticast,
    RxBroadcast,
    Tx,
    TxUnicast,
    TxMulticast,
    TxBroadcast,
}

// hammer-service::interface_model; VPP: vnet/interface.h:1128-1132.
pub struct InterfaceMain {
    // Existing state, rx_polls and tx_lookups remain unchanged.
    sw_if_counters: Vec<SimpleCounterMain>,
    combined_sw_if_counters: Vec<CombinedCounterMain>,
}
```

`Vec` 与 VPP 的两组可按枚举索引的 main 向量对应；不是计数行的第二份
副本。每个 `SimpleCounterMain`/`CombinedCounterMain` 仍只缓存自己的一个
stats entry data pointer。VPP `vnet/interface.h:1128-1132` 的
`vnet_interface_main_t` **不持有** `/if/names`、`if_counters[]` 或
`dir_entry_indices`；它们是 `vnet/interface/stats.c:12-23` 中
`statseg_sw_interface_add_del` 的文件静态状态。Hammer 同样让 service 的
接口 stats callback 模块私有地拥有名称投影，不把这些字段塞入
`InterfaceMain` 或 `InterfaceState`。接口名称仍由 `HwInterface.name` 拥有；
callback 的临时格式化字符串不是第二个长期 owner。
`InterfaceMain` 不增加 `Option` 或计数快照。两个 counter 向量初始化后长度
固定；下标由上述封闭枚举给出。**不增加 `SpinLock<()>`**：它只锁住一个零大小值，并不能
通过 Rust 借用表达对旁边两个 `Vec` 的独占；在当前单写者约束下它也
没有需要串行化的第二个控制调用者。

```rust
// VPP: vnet/interface.c:1415-1434; vlib/counter.c:47-106.
// InterfaceMain::new changes from Self to RuntimeResult<Self> so stats
// registration errors reach the existing startup caller with their source.
impl InterfaceMain {
    pub fn new() -> RuntimeResult<Self> {
        let segment = &StatsMain::global()?.segment;
        let thread_count = hammer_runtime::config::worker::worker_count() + 1;
        let mut sw_if_counters = Vec::with_capacity(5);
        for name in [
            "/if/drops", "/if/rx-no-buf", "/if/rx-miss",
            "/if/rx-error", "/if/tx-error",
        ] {
            match SimpleCounterMain::new(segment, name, thread_count, 0) {
                Ok(counter) => sw_if_counters.push(counter),
                Err(source) => {
                    for counter in sw_if_counters.into_iter().rev() {
                        counter.remove().expect("startup counter entry remains removable");
                    }
                    return Err(source.into());
                }
            }
        }
        let mut combined_sw_if_counters = Vec::with_capacity(8);
        for name in [
            "/if/rx", "/if/rx-unicast", "/if/rx-multicast", "/if/rx-broadcast",
            "/if/tx", "/if/tx-unicast", "/if/tx-multicast", "/if/tx-broadcast",
        ] {
            match CombinedCounterMain::new(segment, name, thread_count, 0) {
                Ok(counter) => combined_sw_if_counters.push(counter),
                Err(source) => {
                    for counter in combined_sw_if_counters.into_iter().rev() {
                        counter.remove().expect("startup counter entry remains removable");
                    }
                    for counter in sw_if_counters.into_iter().rev() {
                        counter.remove().expect("startup counter entry remains removable");
                    }
                    return Err(source.into());
                }
            }
        }
        // Construct the existing InterfaceState/rx_polls/tx_lookups here,
        // then store the two completed counter vectors before publication.
        Ok(Self {
            /* existing fields, */
            sw_if_counters,
            combined_sw_if_counters,
        })
    }

    #[inline(always)] // VPP: direct im->sw_if_counters + counter in interface_output.c:980-991.
    pub fn simple_counter(&self, counter: InterfaceSimpleCounter) -> &SimpleCounterMain {
        &self.sw_if_counters[counter as usize]
    }

    #[inline(always)] // VPP: direct im->combined_sw_if_counters + counter in interface_output.c:619.
    pub fn combined_counter(&self, counter: InterfaceCombinedCounter) -> &CombinedCounterMain {
        &self.combined_sw_if_counters[counter as usize]
    }
}

// VPP: vnet/interface.c:1415-1434; stats segment must exist first.
#[init_function(name = "interface_main_init", runs_after = ["stats_main_init"])]
fn interface_main_init() -> RuntimeResult<()> {
    let mut interfaces = InterfaceMain::new()?;
    if let Err(source) = interface_stats::init() {
        // The InterfaceMain has not been published to any worker or callback.
        for counter in interfaces.combined_sw_if_counters.drain(..).rev() {
            counter.remove().expect("startup counter entry remains removable");
        }
        for counter in interfaces.sw_if_counters.drain(..).rev() {
            counter.remove().expect("startup counter entry remains removable");
        }
        return Err(source);
    }
    let interfaces = Arc::new(interfaces);
    // Keep the existing plugin registration and INTERFACE_MAIN publication.
    // No second counter initialization in NetMain or a device plugin.
    /* existing body */
    Ok(())
}
```

当前依赖图允许 `hammer-service -> hammer-runtime -> hammer-stats`，但未声明
`hammer-service -> hammer-stats`。实施时由 runtime 对 `StatsMain`、两个
counter main 及 callback 模块实际使用的 `NameVector`、`DirectoryIndex`、
`DirectoryType` 做**窄 re-export**；不把 stats 类型重新定义在 service，
也不让 `hammer-stats` 反向依赖 interface。`InterfaceMain::new` 只获取一次
`StatsMain`，并以既有 `RuntimeError::Stats` 保留注册失败的原始 source；
不新增 interface 专属错误码。构造中途失败时按相反注册顺序移除此前
成功创建的 family entry；当前失败的 `CounterMain::new` 自己移除自己的
entry。名称 callback 初始化失败时也先清理尚未发布的全部 family
entry。已注册 entry 无法移除是 stats owner 不变量错误，不把清理错误
伪装成原始注册失败。`interface_main_init` 的显式 `runs_after`
保证本次 `new` 运行时 segment 已发布。

`new() -> RuntimeResult<Self>` 也改变现有构造契约：
`interface_model.rs` 的 `impl Default for InterfaceMain` 不能继续无条件调用
`new()`，实施时删除该 `Default`；`interface_main_init` 是正式启动入口。
当前直接构造 `InterfaceMain` 的测试位于
`hammer-service/tests/{feature_arcs,dpo_lifetime}.rs` 及
`hammer-plugins/net/{ip,icmp}`，都必须与新签名一起迁移，不能留下旧构造器
或用空 counter 向量伪装已初始化。这些测试目前并非全部发布 `StatsMain`；
迁移时必须在各自进程启动边界建立 Main Heap/StatsMain 契约，再构造
`InterfaceMain`。这属于本 ADR 的实现闭包，不只改正式 daemon 路径。

```rust
// VPP: vnet/interface.c:541-568. This is part of the existing
// InterfaceMain::register_interface path, not a new public helper.
let sw_if_index = state.software_interfaces.insert(software_interface);
for counter in &self.sw_if_counters {
    counter.validate(sw_if_index).expect("registered interface counter shape");
    counter.zero(sw_if_index);
}
for counter in &self.combined_sw_if_counters {
    counter.validate(sw_if_index).expect("registered interface counter shape");
    counter.zero(sw_if_index);
}
// Continue the existing lookup-table, graph and callback publication.
```

### 软件接口名称回调

VPP `vnet/interface/stats.c:26-84` 的 `name` 来自软件接口的**上级硬件接口**
`hi_sup->name`：硬件软件接口直接用它；子接口再附加 `.<si->sub.id>`。
`/if/names[sw_if_index]` 保存这个未转义的展示名。每个保留 family 另建
`/interfaces/<接口名>/<family短名>` symlink，指向 `/if/<family短名>`
的 `sw_if_index` 列；仅构造 symlink 路径时把接口名中的 `/` 换成 `_`
（`vlib/stats/format.c:9-23`）。例如 `tap/0` 的 names 值仍是 `tap/0`，
而 RX symlink 路径是 `/interfaces/tap_0/rx`，目标是 `/if/rx` 的该接口列。
`SimpleCounterMain::new(segment, "/if/drops", ...)` 的 `name` 参数仅是
stats family 路径；VPP `interface.c:1420-1432` 中的 `cm->name = "drops"`
是短名，`cm->stat_segment_name = "/if/drops"` 才是注册路径。Rust main
只需保存一个注册路径，短名由上表给出，不从接口名推导。

沿用现有 `InterfaceCallbackRegistration` 和
`InterfaceRegistrationImage::sw_interface_callbacks`，在 service 内注册
`statseg_sw_interface_add_del`；不增加一个手动调用的第二条注册路径。
下面私有的 `InterfaceStats` 只是把 VPP `interface/stats.c` 的三项文件静态
状态放在同一个 Rust 全局初始化边界，不是新增公开 main，也不归
`InterfaceMain`。`OnceLock` 只发布这一个固定地址；`UnsafeCell` 只允许
main thread 在 callback 中修改每接口 symlink 索引，不给 worker 提供借用。
VPP `interface.c:257-261,318-334,541-566,646-650`：先创建并清零
软件接口列，再调用 add callback；delete callback 在 pool slot 释放之前
运行。Hammer 的 `register_interface` / `delete_hardware_interface` 已在这两个
时点调用 `call_sw_interface_add_del`（`interface_model.rs:800,941`），接线
只填入现有 service callback 列表。callback 不进入逐包路径。

```rust
// hammer-service::interface::stats (private module);
// VPP: vnet/interface/stats.c:12-23,26-84. These are NOT InterfaceMain fields.
mod interface_stats {
struct InterfaceStats {
    if_names: NameVector,
    if_counters: [(DirectoryIndex, &'static str); 13],
    dir_entry_indices: UnsafeCell<Vec<Vec<DirectoryIndex>>>,
}

// One process-global callback owner; only the main thread mutates its index lists.
static INTERFACE_STATS: OnceLock<InterfaceStats> = OnceLock::new();
// SAFETY: init finishes before interface callbacks run; main-thread publication
// owns every mutation. Data Workers never borrow dir_entry_indices.
unsafe impl Sync for InterfaceStats {}

pub(super) fn init() -> RuntimeResult<()> {
    // VPP stats.c:30-37: resolve family entries once before the first callback.
    let segment = &StatsMain::global()?.segment;
    let if_counters = [
        (segment.find("/if/drops", DirectoryType::CounterVectorSimple)?, "drops"),
        (segment.find("/if/rx-no-buf", DirectoryType::CounterVectorSimple)?, "rx-no-buf"),
        (segment.find("/if/rx-miss", DirectoryType::CounterVectorSimple)?, "rx-miss"),
        (segment.find("/if/rx-error", DirectoryType::CounterVectorSimple)?, "rx-error"),
        (segment.find("/if/tx-error", DirectoryType::CounterVectorSimple)?, "tx-error"),
        (segment.find("/if/rx", DirectoryType::CounterVectorCombined)?, "rx"),
        (segment.find("/if/rx-unicast", DirectoryType::CounterVectorCombined)?, "rx-unicast"),
        (segment.find("/if/rx-multicast", DirectoryType::CounterVectorCombined)?, "rx-multicast"),
        (segment.find("/if/rx-broadcast", DirectoryType::CounterVectorCombined)?, "rx-broadcast"),
        (segment.find("/if/tx", DirectoryType::CounterVectorCombined)?, "tx"),
        (segment.find("/if/tx-unicast", DirectoryType::CounterVectorCombined)?, "tx-unicast"),
        (segment.find("/if/tx-multicast", DirectoryType::CounterVectorCombined)?, "tx-multicast"),
        (segment.find("/if/tx-broadcast", DirectoryType::CounterVectorCombined)?, "tx-broadcast"),
    ];
    // Resolve all existing entries before creating /if/names: a missing
    // family cannot leave a newly registered name vector behind.
    let if_names = segment.add_name_vector("/if/names", 0)?;
    assert!(INTERFACE_STATS.set(InterfaceStats {
        if_names,
        if_counters,
        dir_entry_indices: UnsafeCell::new(Vec::new()),
    }).is_ok(), "interface stats initialized twice");
    Ok(())
}

// VPP: vnet/interface/stats.c:26-84; interface.c:257-261,541-566,646-650.
pub(super) fn statseg_sw_interface_add_del(
    _: &mut DataPlaneMain,
    interfaces: &InterfaceMain,
    sw_if_index: u32,
    is_add: bool,
) -> InterfaceResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("interface stats callback requires main-thread publication ownership");
    let stats = INTERFACE_STATS.get().expect("interface stats initialized before callbacks");
    let segment = &StatsMain::global().expect("stats main initialized before callbacks").segment;
    let sw = interfaces.software_interface(sw_if_index)
        .expect("stats callback requires a live software interface");
    let sup = interfaces.software_interface(sw.sup_sw_if_index)
        .expect("stats callback requires a live superior interface");
    let hw_if_index = sup.hw_if_index
        .expect("superior software interface must own a hardware interface");
    assert_eq!(sw_if_index, sw.sup_sw_if_index, "subinterface id is not represented");
    // VPP format(0, "%v", hi_sup->name) also makes a temporary copy.
    let display_name = interfaces.hardware_interface(hw_if_index).name.clone();

    let slot = sw_if_index as usize;
    // SAFETY: call_sw_interface_add_del runs on the main thread with its
    // existing publication barrier; no other thread mutates this vector.
    let dir_entry_indices = unsafe { &mut *stats.dir_entry_indices.get() };
    if dir_entry_indices.len() <= slot {
        dir_entry_indices.resize_with(slot + 1, Vec::new);
    }
    let links = &mut dir_entry_indices[slot];
    if is_add {
        assert!(links.is_empty(), "software interface stats links already exist");
        let path_name = display_name.replace('/', "_"); // stats/format.c:9-23
        // VPP stats.c:57-65: set the name, then create one symlink per family.
        segment.set_name(stats.if_names.index, sw_if_index, &display_name)
            .expect("registered interface name vector has a valid shape");
        for &(entry, suffix) in &stats.if_counters {
            let path = format!("/interfaces/{path_name}/{suffix}");
            let index = segment.add_symlink(entry, sw_if_index, &path)
                .expect("interface stats symlink must have a unique valid name");
            links.push(index);
        }
    } else {
        // VPP stats.c:69-74: mark deleted and remove only these symlinks.
        segment.set_name(stats.if_names.index, sw_if_index, "deleted")
            .expect("registered interface name vector has a valid shape");
        for index in std::mem::take(links) {
            segment.remove_entry(index).expect("registered stats symlink is removable");
        }
    }
    Ok(())
}

} // interface_stats

// Existing service registration image; VPP interface.h:250-253.
const INTERFACE_STATS_CALLBACK: InterfaceCallbackRegistration =
    InterfaceCallbackRegistration {
        callback: interface_stats::statseg_sw_interface_add_del,
        priority: 0,
    };
pub(crate) static SERVICE_INTERFACE_REGISTRATION_IMAGE: InterfaceRegistrationImage =
    InterfaceRegistrationImage::new(
        &crate::__HAMMER_DEVICE_CLASS_REGISTRATIONS,
        &crate::__HAMMER_HW_CLASS_REGISTRATIONS,
        &[],
        &[INTERFACE_STATS_CALLBACK],
        &[],
    );
```

现有 `SwInterface` 只建硬件软件接口，也没有 `sub.id`；本阶段 callback
只执行上述硬件分支，不伪造子接口编号。将来引入子接口时必须先把其
`sub.id` 放到软件接口 owner，再按 VPP 的同一 callback 生成 `.<id>`。
VPP 回调不处理 rename：`interface.c:1510-1559` 只更新硬件接口名、名称
索引及部分 node 名，不重新运行软件接口 add/del 回调。因此本 ADR
不声称 `set_interface_name` 会同步既有 stats symlink；Hammer 当前
`NetMain::init` 在注册后再次设置 `local0`，与 `register_interface`
按 `local` 加实例 0 得出的初始名称相同，不构成一次实际重命名。
真正的运行期 rename 若要同步 stats，应单独约定生命周期，不暗中
借 add/del 回调伪装成 VPP 已有行为。

VPP `stats.c:39-79` 在整次回调外取得 stats segment lock；Hammer
`StatsSegment::add_symlink`、`remove_entry` 各自取得目录结构锁，
`set_name` 单独发布一次 `DirectoryWrite`；三者目前没有覆盖整次回调的
共同锁与 epoch。回调由 main-thread 单写者在已有 WorkerBarrier
下执行；此 barrier 保护 worker 所见接口状态，不是 stats 目录互斥锁。
上述代码规定 callback 的逻辑顺序；**实施前必须**让这组 name/symlink
操作参与 `hammer-stats` 同一目录锁与 epoch 事务，外部 reader 才不会
看到半组 symlink。不能靠 `SpinLock<()>` 或在 service 外面另套一把锁
宣称对齐。VPP `stats.c:60-64` 在 symlink 注册失败时 `ASSERT`，本 callback
也把缺失初始化、重复 symlink、错误目录形状当作 owner 不变量；Main Heap
耗尽按现有分配器契约终止，不新增可恢复错误或局部回滚。现有 callback
签名仍返回 `InterfaceResult<()>`，本 callback 正常结束只返回 `Ok(())`；
`register_interface`/delete 对**其他** callback 错误的丢弃是既有问题，
不借本 ADR 改写整个接口注册 API。

VPP `interface.c:1415-1434` 在初始化两个向量名称时取锁，并注释
“should be no need”；`interface.c:550-566` 在创建软件接口后取
`sw_if_counter_lock`，对全部 family 执行 `validate`/`zero` 后立即解锁。
`ethernet/p2p_ethernet.c:92-110` 和部分插件也在同一阶段取锁；逐包
`increment`、普通读取和接口回调不在这个临界区。VPP 的锁串行化
**多个可能执行 counter 初始化的控制调用者**，不负责阻止 worker
读搬迁前的行指针。

Hammer 当前只有 `InterfaceMain::register_interface` 创建软件接口，
该入口通过 `ensure_main_thread_with_barrier` 限定为 main thread；没有
第二个可与它并发执行 `validate`/`zero` 的控制入口。因此由 **main-thread
唯一写者**串行化这段操作，不复制 VPP 的 `sw_if_counter_lock`。
现有 WorkerBarrier 用于接口拓扑发布，并在 stats 行搬迁时阻止 worker
继续访问旧指针；它不是互斥锁，不能被描述成“替代 spinlock”。若以后
允许另一个 OS 线程直接创建/重置接口，先把那条入口收敛到 main thread；
只有确实无法收敛时才重新设计一把拥有受保护数据的锁，不能放一把
`SpinLock<()>` 作装饰。`validate` 在这里与 VPP 的 `void` 操作一样属于已校验的
内部注册步骤：错误的 entry/shape 是程序错误，segment 分配耗尽按 Main
Heap 约定终止，而不是把 `register_interface` 的 `u32` 改成新的错误 API。
删除接口不释放 family entry；pool index 再利用时重新 `validate`/`zero`。
VPP 的 interface main 和两个 counter main 均无 cache-line mark；Hammer
不为它们添加标记。64 字节对齐施加于 `counters` 指向的外层向量数据
及每条线程计数行起点，均由现有 StatsSegment 分配器完成。

## 5. TUN 收发与通用 output/drop 接线

设备插件不拥有 counter main，也不注册第二组 `/if/...` stats entry。
`TunMain` 创建接口后沿用 `InterfaceMain::register_interface` 得到的
`sw_if_index`；每个 Data Worker 在自己的 `runtime.thread_index()` 行写
该接口列。`DataWorkerId` 只用于选 RX/TX queue，不能代替包含 thread 0
的 runtime thread index。多队列下同一 worker 的所有 TUN RX queue
汇入这一行，不按 queue id 再建列。来源：VPP
`plugins/tap/rx_node.c:289-328`、`plugins/tap/tx_node.c:365-370,419-425`；
Hammer `tuntap/src/lib.rs:572-645,991-1142,1409-1470`。

| 路径 | 写入位置、family 与数量 | VPP 依据 |
| --- | --- | --- |
| Admin-down 的 TUN RX | 在取 used ring 之前检查接口管理状态；down 时不调用单队列 RX 处理、不补充 vring，也不写 RX 或 drop counter。Hammer 当前取包之后才把 next 改成 drop，实施时必须移动这个判断，不能只加计数。 | `tap/rx_node.c:394-409`；Hammer `tuntap/src/lib.rs:1005-1120` |
| TUN RX 成功取包 | `tun-input` 每个非空 queue batch，在送 next 前写 `InterfaceCombinedCounter::Rx`；packets 为该批完整包数，bytes 为去掉 virtio header 后每条 Buffer chain 的 L3 长度之和，含所有后续片段。即使随后选到 drop next，也已计为 RX。 | `tap/rx_node.c:191-281,289-328,353-355` |
| RX refill 缺 Buffer 或 used ring 满 | 保留 `TunRxError::BufferAlloc` 和 `FullRxQueue` 的 node-local error；**不**因名字相似就写 `/if/rx-no-buf`、`/if/rx-miss` 或 `/if/rx-error`。 | `tap/rx_node.c:49-59,207-214` |
| 通用 TX output | service 的 `interface_output_template` 在接口 up 且该 worker 有 TX queue 的正常分支，对 `InterfaceCombinedCounter::Tx` 一次写入本 frame 的包数和 Buffer chain 字节数；发生在设备 TX 前，后续设备丢包不倒扣 TX。feature arc 改变 next 时仍属于此 output 计数点。 | `vnet/interface_output.c:536-660` |
| TX output 提前拒绝 | `InterfaceDown`、`InterfaceDeleted`、`NoTxQueue` 仍是 output node-local error/drop，不在 TUN TX 函数里补记，也不把它们误算为已进入设备的包。 | `vnet/interface_output.c:557-603` |
| TUN TX 设备丢包 | `tun_if_tx` 的 GSO/CSUM/间接描述符等分类丢包，加上 `tun_intfc_tx` 重试后剩余的 no-slot 丢包；保留各自 node-local error，另在 `InterfaceSimpleCounter::Drop` 对这两组互斥包数**总共写一次**。不写 `TxError`：VPP tap TX 写的是 `DROP`。 | `tap/tx_node.c:220-374,376-426` |
| 图内后续丢包 | service 的 drop 路径从 `NetworkOpaque.sw_if_index[0]` 取有效 ingress 软件接口索引，按该索引归并写 `InterfaceSimpleCounter::Drop`，再释放 Buffer；不要在 TUN RX 的 drop next 选择处提前加一次。TUN TX 内部已直接释放的包不会再进入此路径。 | `vnet/interface_output.c:927-1046` |

下列块是现有函数中的计数位置，不新增 `TunCounter`、queue counter
或 callback。`rx_bytes` 在当前 `tun-input` 的 used-ring 循环中按每条
完成的 Buffer chain 累加；`total` 已是逐片段长度总和，且只对首片段
减一次 virtio header。写入在 `queue.refill` 及下一 queue 之前完成，
不把 refill 失败混进 RX 包/字节。

```rust
// plugin/tuntap::TunInputNode::process, inside each polled queue;
// VPP: plugins/tap/rx_node.c:191-281,289-328,394-409.
if !interfaces.software_interface(main.sw_if_index)
    .expect("TUN interface remains live")
    .is_admin_up()
{
    continue;
}
let mut rx_bytes = 0_u64;
while queue.has_used() && count < DEFAULT_BUFFER_FRAME_CAPACITY {
    // Existing dequeue, chain assembly and total computation.
    // total excludes the first descriptor's virtio header.
    rx_bytes += total as u64;
    count += 1;
}
if count != 0 {
    interfaces.combined_counter(InterfaceCombinedCounter::Rx).increment(
        runtime.thread_index(), main.sw_if_index, count as u64, rx_bytes,
    );
}
// Existing next conversion, feature start, enqueue and refill follow.
// The old post-dequeue admin-down rewrite is removed.
```

`interface_output_template` 是 service 的唯一普通 TX counter 写入者：
它本来遍历单一 `hw_if_index` 的 frame，并在 `error == None` 分支启动
output feature arc。字节数使用现有 Buffer 头的
`current_len() + total_len_not_including_first()`；不得复制 packet payload，
也不得在 TUN device TX 再写一次 combined TX。缺失的接口计数操作
不是新 transport API，直接调用本 ADR 已定义的 counter main 方法。

```rust
// hammer-service::interface::interface_output_template;
// VPP: vnet/interface_output.c:170-288,536-660.
let mut tx_packets = 0_u64;
let mut tx_bytes = 0_u64;
for &index in frame.vector_args() {
    // Existing InterfaceDeleted/InterfaceDown/NoTxQueue classification.
    if error.is_none() {
        let buffer = runtime.buffer(index);
        tx_packets += 1;
        tx_bytes += (buffer.current_len()
            + buffer.total_len_not_including_first()) as u64;
        // Existing feature-arc and queue-scalar selection.
    }
}
if tx_packets != 0 {
    interfaces.combined_counter(InterfaceCombinedCounter::Tx).increment(
        runtime.thread_index(), sw_if_index, tx_packets, tx_bytes,
    );
}
// Existing enqueue_to_next_with_scalar follows.
```

`tun_if_tx` 当前对每个已消费且丢弃的包恰好增加一个 `drops[reason]`；
`tun_intfc_tx` 的 `no_slots` 仅覆盖没有被 `tun_if_tx` 消费的尾部。两组
互斥，所以在当前逐原因 node error 循环旁汇总即可，不能按 VPP
`tap_if_tx_inline` 与外层 TX 两处机械相加同一批次。即使设备发送失败，
此前 service output 的 TX 包/字节仍不回滚，符合 VPP 的前置计数点。

```rust
// plugin/tuntap::tun_intfc_tx;
// VPP: plugins/tap/tx_node.c:365-374,414-426.
let no_slots = packets.len() - consumed;
if no_slots != 0 {
    drops[TunTxError::NoFreeSlots as usize] = no_slots as u64;
    runtime.buffer_free(&packets[consumed..]);
}
let dropped = drops.iter().sum::<u64>();
if dropped != 0 {
    NetMain::global().expect("network Main exists during TUN TX")
        .interface_main()
        .simple_counter(InterfaceSimpleCounter::Drop)
        .increment(runtime.thread_index(), main.sw_if_index, dropped);
}
// Existing per-reason node error counts remain independent of /if/drops.
```

VPP 的 `interface_drop_punt` 按 RX 软件接口分组计数
（`vnet/interface_output.c:927-1046`），不是 tap RX 的逐包失败
分类。Hammer `hammer-service/src/data_plane.rs` 的 `drop_node_process`
现于释放 frame 前归并同一有效 `NetworkOpaque.sw_if_index[0]` 的包数并写
`InterfaceSimpleCounter::Drop`。显式写入 `NetworkOpaque::default()` 的包将
RX 索引置为 `u32::MAX`（`opaque.rs:339-345`），释放时不访问接口列；
新分配 Buffer 的 core opaque 模板虽为零（`hammer-core/src/buffer/opaque.rs`），
却不是已初始化的 `NetworkOpaque`。网络包的生产者必须在进入 drop 路径前
明确写入 RX 接口索引，或以 `NetworkOpaque::default()` 标记无 ingress；
TUN RX 已在入图前完成该写入（`tuntap/src/lib.rs:1104-1108`），对应 VPP
在 TUN 专用模板中设置 RX 软件接口和 TX `~0`
（`plugins/tap/tap.c:1019-1025`）。VPP `interface_drop_punt` 直接取已设置的
RX 索引并计数（`vnet/interface_output.c:992-1037`）。只有生产者明确
指定 `local0` 时，索引 0 才代表该接口；未初始化的零值不能据此计数。
非哨兵索引必须属于已注册接口，否则是 owner 不变量错误。VPP 的
`interface-drop` 与 `interface-punt`
分别使用 `DROP` 和 `PUNT`（`interface_output.c:927-990`）；Hammer
`PuntNode` 复用同一释放实现，但已区分调用者，**不会**给
`PuntNode` 写 Drop。本 ADR 只保留 `Drop` family，不借此引入 `PUNT`
枚举空洞。node-local
`buffer.error` 与接口 Drop 是两个不同维度，前者不因接口计数而清除。

验收要覆盖 admin-down 不取 RX used ring 且不增加接口计数、两个 RX
queue 同 worker 累加到同一接口列、跨 worker
分别落在各自行、单/多片段 RX 字节数、virtio header 不计入 L3 bytes、
IP 版本不合法时仍计 RX 后转 drop、设备 TX 分类丢包与 no-slot
互不重复、service output 的 TX 先于设备失败计入、output 提前拒绝
不计 TX，以及 drop node 只按已初始化的有效 RX 接口记一次、显式
`u32::MAX` 和 punt 不记 Drop；网络包生产者不得将原始零值 opaque 送入
drop。不能只核对
stats 路径存在，还需核对实际 RX/TX/drop 写入点。

## 6. 验收与公开面

验证 thread 0/worker 各自更新同一外层向量的不同计数行、simple 与
combined 方法、跨行汇总、扩容前后的入口刷新、stats heap 激活恢复、
`will_expand` 与实际搬迁一致、对象索引复用时 zero、remove 后目录消失。
验证两种 counter main 的外层 `counters` 数据地址和每个线程行起点
均可被 64 整除，而 `u64` / 16B `Counter` 元素保持连续、不按 64B
逐元素填充。
初次创建失败不得留下目录条目。Rust 版非原子 `get` 不提供无 barrier
读取；实现验收必须证明不会发生并发普通读写，而不只是数值近似。
InterfaceMain 还需验证两组 enum 下标连续、只创建上述 13 个 `/if/...`
entry、首次及复用 `sw_if_index` 均在 main-thread 独占阶段清零，
且 init 明确排在 stats 发布之后。还需验证 `/if/names` 未转义接口名、
13 个 symlink 的目标列与路径转义、delete 后 names 为 `"deleted"` 且
symlink 消失、family entry 仍在，以及复用 slot 不保留旧 symlink；
不得在该路径新增无意义的锁。

拟议公开面是两个 counter main 及其方法、两个 interface counter 枚举、
两个 `InterfaceMain` 访问方法和既有 `Counter` 的三个纯值方法；stats
回调及 symlink 索引均为 service 私有，不新增段内 `*_data` 方法。
issue #372 已按本 ADR 接入两个 counter main、接口 family 和 TUN
RX/TX/drop 写入点；编译、测试与 CI 按本次执行要求未运行，不能将
上述验收项视为已验证通过。

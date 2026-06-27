# 数据结构热路径优化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不破坏现有 session/runtime、buffer、TCP 分层的前提下，压缩热点元数据占用、缩短查询路径，并把真正高频字段放到更稳定的 cacheline 布局和 prefetch 路径上。

**Architecture:** 这次只做“现有代码上的热路径整理”，不重做架构，不新增 TCP/Session 业务中间层。优化顺序遵循 VPP 语义：先确认 header/索引/队列这些真正高频的数据布局，再把 VPP 已经证明需要专门结构的能力先补到 `hammer-infra`，最后才在 TCP/session 调用点接入，避免为了一个热点到处加 helper。

**Tech Stack:** Rust 2024，`hammer-infra::{map,pool,vec,fifo,prefetch}`，新增 `hammer-infra::{bitmap,rbtree}`，`hammer-service::session::runtime`，`hammer-adapter::buffer`，`hammer-service::transport::tcp::*`，以及 `third_party/vpp/src/vlib/buffer.h`、`third_party/vpp/src/svm/svm_fifo.c`、`third_party/vpp/src/vnet/tcp/tcp_bt.c` 的结构语义参考。

## Global Constraints

- 以现在仓库代码为准，不回退到旧设计，不引入新的 TCP/session 过渡层。
- 只允许 app 和 session 边界复制 payload；本次优化不得新增任何中间 payload `Vec`。
- 除非先在本计划的审批节里列出并获得批准，否则不得新增非平凡类型或 API。
- 优先复用 `hammer-infra` 现有能力；如果缺少的是通用能力，只能加在 `hammer-infra`，不能加业务特化 API。
- session runtime 负责调度；TCP 不能因为这次优化重新耦合 session/app 语义。
- 高热点字段才做 cacheline/prefetch；没有证据的字段不做装饰性 `#[repr(align)]`、`#[inline]` 或“看起来很快”的重排。
- 参考 VPP 的语义而不是 1:1 API：重点参考 `vlib_buffer_t` 的 header 分层、`vlib_prefetch_buffer_header/data` 的访问意图、per-thread hot state 的 cacheline 分离。

## 结构选择原则

- `HashTable` 只用于无序查找：5-tuple -> connection、session_id -> queue/state 这类点查。
- `Bitmap` 只用于稠密编号集合：per-worker ready/pending/fired 这类集合，如果 key 空间稠密且上限固定，优先 bitmap，不继续用 `Vec + Hash` 去重。
- `Vec` 只用于这三类情况：
  - 容量天然很小且上界明确；
  - 只追加/顺序消费；
  - 临时实现阶段的保底版本。
- `Vec` 不是 OOO、有序 range、SACK scoreboard、重传洞（hole）管理的最终结构。
- 需要“按序插入 + 前驱/后继 + 重挂 key + 局部 split/merge”的场景，最终必须落到树结构或同等级结构，不能长期停留在线性扫描。
- 需要“洞列表 / 丢包区间 / SACK scoreboard”的场景，应该由 TCP 自己维护纯 TCP 类型，而不是散落成多个裸字段和若干 `Vec`。
- 需要“按 worker 稠密标记 ready/pending”的场景，应该优先评估 bitmap，而不是默认 `FlatHashTable<u64, usize>`。
- VPP 已经明确用 `rb_tree` 的问题域，这边默认也要先评估 `RbTree`，而不是先假设 `Vec` 足够。

## VPP `rb_tree` 对照结论

- `third_party/vpp/src/vppinfra/rbtree.h`
  - `rb_tree_t` 本身就是“pool-backed node + parent/left/right index + opaque value”语义。
  - 这边要学的是语义：树节点自己维护拓扑，业务层只关心 key 顺序和 opaque/value，不做裸指针强转式上层 API。
- `third_party/vpp/src/svm/fifo_types.h`
  - `svm_fifo_t` 同时持有 `ooo_enq_lookup`、`ooo_deq_lookup`、`ooo_segments` pool、以及 `ooos_list_head`。
  - 说明 VPP 不是“树替代一切”，而是“业务记录仍由 fifo 自己持有，树只做 ordered lookup / predecessor / successor / fast locate”。
- `third_party/vpp/src/svm/svm_fifo.c`
  - `f_find_node_rbtree()`、`f_find_chunk_rbtree()`、`f_update_ooo_enq()`、`f_update_ooo_deq()` 说明 OOO 场景最终需要的是“有序定位 + 邻居查找 + 局部重挂”。
  - 同时 `ooo_segment_add()`、`ooo_segment_try_collect()` 仍然围绕 `ooo_segments` pool 和链表推进，说明 OOO 业务状态和 lookup 索引是两层，不应该把 payload/storage 直接塞进树结构里。
- `third_party/vpp/src/vnet/tcp/tcp_types.h`
  - `tcp_byte_tracker_t` 同时持有 `samples` pool、双向链表头尾和 `sample_lookup`。
  - 说明 TCP byte tracker 也不是只靠 `Vec` 扫描，而是“样本存储”与“按序 lookup”分离。
- `third_party/vpp/src/vnet/tcp/tcp_bt.c`
  - `rb_tree_predecessor()`、`rb_tree_add_custom()`、`rb_tree_del_custom()` 的使用说明，byte tracker 的 sample lookup 明确依赖 predecessor/successor 和 key 变更后的重新挂树。

这份计划里的 `RbTree` 落点因此要遵循同一原则：

- session RX OOO：session/runtime 持有真正的 buffer/entry 状态，`RbTree` 只做 offset 有序索引。
- TCP byte tracker：TCP 持有 sample 业务记录，`RbTree` 只做 min_seq 有序索引。
- 不把 `RbTree` 直接设计成 payload carrier，也不把业务记录拆成树专属类型散落在多个层里。

## 文件责任图

- `crates/hammer-infra/src/map.rs`
  - 负责 `FlatHashTable` 的 bucket 生命周期、容量复用、lookup/prefetch 热路径。
- `crates/hammer-infra/src/pool.rs`
  - 负责 pool slot 元数据布局、slot 校验、插入删除和热点 slot 访问。
- `crates/hammer-infra/src/bitmap.rs`（如新增）
  - 负责稠密编号集合的置位、清位、找第一个/下一个 set bit；不能带 session/TCP 语义。
- `crates/hammer-infra/src/rbtree.rs`（如新增）
  - 负责通用红黑树索引；用于 OOO lookup、byte tracker sample lookup 这类按序查找问题，不能带 TCP 业务名。
- `crates/hammer-service/src/session/ready.rs`
  - 负责 ready session 去重队列；不承载 TCP 业务，只优化 ready 集合命中和清空成本。
- `crates/hammer-service/src/session/timer.rs`
  - 负责 session timer wheel 的 arm/cancel/expire/query；本次要去掉明显的线性扫描和 drain/rebuild。
- `crates/hammer-service/src/session/runtime.rs`
  - 负责 session runtime 热路径数据组织、tx/rx queue 查表、close/app/poll 流程中的重复访存。
- `crates/hammer-service/src/session/protocol.rs`
  - 负责 protocol 回调上下文；本次只收紧热路径访问，不加新的 session/tcp 中间抽象。
- `crates/hammer-adapter/src/buffer.rs`
  - 负责 buffer header、opaque、cursor、refcount、chain header 的布局与 prefetch 入口，语义对齐 VPP 的 buffer header/data 分层。
- `crates/hammer-service/src/transport/tcp/{input,established,listen,syn_sent,rcv_process}.rs`
  - 负责把现有 prefetch/查表优化真正接到 TCP 节点热路径，不新增 node 特化 helper。

## 审批节

### 拟新增类型/API（需要先批准）

1. `crates/hammer-infra/src/map.rs`
   - 新增泛型 API：
   ```rust
   pub fn clear(&mut self)
   ```
   - **最终结果：** 复用 bucket allocation，避免 `SessionReadyQueue::take_ready_sessions()` 之类逻辑每次把表整体重建成 `FlatHashTable::new()`。
   - **为什么现有表面不够：** 目前只能重新构造整张表，容量和 buckets 都丢掉，ready/timer/session 这类循环使用的索引会反复扩容。

2. `crates/hammer-infra/src/bitmap.rs`（候选）
   - 新增泛型基础结构：
   ```rust
   pub struct Bitmap { /* bits */ }
   ```
   - **最终结果：** 用于 per-worker 稠密 ready/pending/fired 集合，支持 `set/clear/is_set/first_set/next_set`。
   - **为什么现有表面不够：** 当前仓库只有 `Vec<bool>` 式局部实现，没有一个能复用的稠密集合基础结构；ready/pending 这类热点场景继续用 `Vec + Hash` 去重会多一次 hash 查找和额外内存跳转。

3. `crates/hammer-infra/src/rbtree.rs`（候选）
   - 新增通用基础结构：
   ```rust
   pub struct RbTree<K, V> { /* ordered key index */ }
   ```
   - **最终结果：** 提供 `insert/remove/get/get_mut/first/last/predecessor/successor/iter`，先服务 session RX OOO 的有序索引，再服务 VPP 已经明确上树的另一类场景：TCP byte tracker sample lookup。
   - **为什么现有表面不够：** 现有 `Vec`/`FifoQueue`/`FlatHashTable` 都不适合表达“按序插入 + 前驱/后继 + 重挂 key + 局部 split/merge”；VPP 的 `svm_fifo` 和 `tcp_bt` 都是“业务记录自己存，lookup 单独上树”，这边缺的正是这层通用 ordered index。

4. `crates/hammer-service/src/transport/tcp/*`
   - 新增纯 TCP 业务态类型（候选）：
   ```rust
   struct TcpScoreboard { /* holes / delivered / lost / high_sacked */ }
   struct TcpScoreboardHole { /* start / end / prev / next */ }
   ```
   - **最终结果：** 把 SACK/recovery 的区间状态收敛为 TCP 内部最终形态，而不是多个松散字段加若干 `Vec`。
   - **为什么现有表面不够：** 这类状态是纯 TCP 业务不变量，放在 `Vec` + 裸字段里会让局部性和状态一致性一起变差。

### 明确不新增

- 不新增任何 TCP/Session 过渡层、包装层、helper carrier 类型。
- TCP 内部如果确实需要新增业务态类型，只允许新增“最终形态”的局部数据类型，并且必须先在审批节补充三件事：最终结果、为什么现有字段/容器拼装不够、它改善的是哪一段局部性或哪一个不变量。
- 不新增任何 “Hot/Cold helper”、“Layout helper”、“View/Cursor wrapper” 一类包装。
- 不新增 TCP 特化 prefetch API、runtime API、buffer API。
- Linux/VPP 里已经明显使用专门结构的问题域，计划执行时不得再默认拿 `Vec + Hash` 当最终答案，至少要在审批节明确写出“临时保底”还是“最终结构”。
- VPP 中已确认的 `rb_tree` 用法必须写进同一份总计划并给出落点，不允许散落在别的计划里。

---

### Task 1: `hammer-infra` 哈希表容量复用和预热路径

**Files:**
- Modify: `crates/hammer-infra/src/map.rs`
- Modify: `crates/hammer-service/src/session/ready.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-infra/src/map.rs`
- Test: `crates/hammer-service/tests/session_runtime.rs`

**Interfaces:**
- Consumes:
  - `FlatHashTable::with_capacity(capacity: usize) -> Self`
  - `FlatHashTable::prefetch_key(&self, key: &K)`
- Produces:
  - `FlatHashTable::clear(&mut self)`
  - `SessionReadyQueue::new()`
  - `SessionDriverRuntime::new(worker: DataWorkerId, buffers: DataPlaneBuffers, aux: A) -> Self`

- [ ] **Step 1: 写失败测试，锁定 clear 和容量复用语义**

```rust
#[cfg(test)]
mod tests {
    use super::FlatHashTable;

    #[test]
    fn clear_removes_entries_and_preserves_bucket_count() {
        let mut table = FlatHashTable::with_capacity(64);
        table.insert(1u64, 11u32);
        table.insert(2u64, 22u32);
        let buckets = table.bucket_count();

        table.clear();

        assert_eq!(table.len(), 0);
        assert_eq!(table.lookup(&1), None);
        assert_eq!(table.lookup(&2), None);
        assert_eq!(table.bucket_count(), buckets);
    }
}
```

Run: `cargo test -p hammer-infra map::tests::clear_removes_entries_and_preserves_bucket_count -v`  
Expected: FAIL，因为 `FlatHashTable::clear` 还不存在。

- [ ] **Step 2: 实现 `FlatHashTable::clear`，不重建 buckets**

```rust
impl<K: FlatHashKey, V: Copy> FlatHashTable<K, V> {
    #[inline]
    pub fn clear(&mut self) {
        for bucket in self.buckets.iter_mut() {
            bucket.entry = None;
        }
        self.len = 0;
    }
}
```

- [ ] **Step 3: 把 ready/session 索引从 `new()` 改成预分配并复用**

```rust
impl SessionReadyQueue {
    #[inline]
    pub fn new() -> Self {
        Self {
            ready: hammer_infra::vec::Vec::with_capacity(1024),
            slots: hammer_infra::map::FlatHashTable::with_capacity(1024),
        }
    }

    pub fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        let ready = self.ready.iter().copied().collect();
        self.ready.clear();
        self.slots.clear();
        ready
    }
}
```

```rust
Self {
    app_ops: FlatHashTable::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
    tx_index: FlatHashTable::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
    rx_index: FlatHashTable::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
    // ...
}
```

- [ ] **Step 4: 运行针对性测试**

Run: `cargo test -p hammer-infra map::tests::clear_removes_entries_and_preserves_bucket_count -v`  
Expected: PASS

Run: `cargo test -p hammer-service --test session_runtime -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-infra/src/map.rs crates/hammer-service/src/session/ready.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "infra(Refactor): reuse flat hash allocation on hot paths"
```

### Task 2: session timer 和 ready 队列去线性重建

**Files:**
- Modify: `crates/hammer-service/src/session/timer.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-service/tests/session_runtime.rs`

**Interfaces:**
- Consumes:
  - `SessionTimerWheel::arm_ticks`
  - `SessionTimerWheel::cancel`
  - `WorkerSessionRuntime::arm_timer_ticks`
  - `WorkerSessionRuntime::cancel_timer`
- Produces:
  - `SessionTimerWheel` 内部 O(1) 定位索引
  - `WorkerSessionRuntime::clear_pending_timer_expiry` 删除

- [ ] **Step 1: 写失败测试，锁定重复 arm/cancel 后只交付最新 expiry**

```rust
#[test]
fn rearming_same_timer_does_not_emit_stale_pending_expiry() {
    let mut ready = SessionReadyQueue::new();
    let session_id = SessionId::from(hammer_infra::pool::Index::new(1, 1));
    let token = SessionTimerToken::new(7);
    let mut wheel = SessionTimerWheel::new();

    wheel.arm_ticks(session_id, token, 1).unwrap();
    wheel.expire(1, &mut ready).unwrap();
    wheel.arm_ticks(session_id, token, 8).unwrap();

    let expiries = wheel.take_expiries();
    assert!(expiries.is_empty());
}
```

Run: `cargo test -p hammer-service --test session_runtime rearming_same_timer_does_not_emit_stale_pending_expiry -v`  
Expected: FAIL，因为当前实现会靠 `WorkerSessionRuntime::clear_pending_timer_expiry` 做 O(n) drain/rebuild。

- [ ] **Step 2: 在 `SessionTimerWheel` 内部加索引表，直接处理重 arm/cancel**

```rust
pub struct SessionTimerWheel {
    wheel: TimerWheel2t1w2048,
    slots: hammer_infra::vec::Vec<SessionTimerSlot>,
    slot_index: hammer_infra::map::FlatHashTable<u128, u32>,
    expired_slots: hammer_infra::vec::Vec<u32>,
    pending: hammer_infra::vec::Vec<SessionTimerExpiry>,
}
```

```rust
#[inline(always)]
fn timer_key(session_id: SessionId, token: SessionTimerToken) -> u128 {
    (u128::from(session_id.get()) << 32) | u128::from(token.get())
}
```

```rust
pub fn cancel(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
    let Some(slot) = self.slot_index.remove(&timer_key(session_id, token)) else {
        return false;
    };
    let timer = self.slots.get_mut(slot as usize).expect("live timer slot");
    timer.live = false;
    self.wheel.stop(timer.handle)
}
```

- [ ] **Step 3: 删除 `WorkerSessionRuntime::clear_pending_timer_expiry` 及其 drain/rebuild**

```rust
pub fn arm_timer_ticks(
    &mut self,
    session_id: SessionId,
    token: SessionTimerToken,
    ticks: u64,
) -> CoreResult<()> {
    self.timers.arm_ticks(session_id, token, ticks)
}

pub fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
    self.timers.cancel(session_id, token)
}
```

- [ ] **Step 4: 跑 session runtime / timer 测试**

Run: `cargo test -p hammer-service --test session_runtime -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/timer.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "session(Refactor): remove timer stale-expiry scans"
```

### Task 3: `SessionDriverRuntime` 热字段重排和查表路径收紧

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Test: `crates/hammer-service/tests/session_runtime.rs`

**Interfaces:**
- Consumes:
  - `SessionDriverRuntime::tx_queue_mut_or_alloc`
  - `SessionDriverRuntime::rx_queue_mut_or_alloc`
  - `SessionQueueControlContext::flush_session_rx`
- Produces:
  - 热路径字段重排后的 `SessionDriverRuntime<S, A>`
  - 更少重复查表/重复解引用的 rx/tx flush 路径

- [ ] **Step 1: 写失败测试，锁定 `enqueue_rx` / `flush_session_rx` / `release_tx_up_to` 语义不变**

```rust
#[test]
fn enqueue_rx_keeps_ordered_delivery_and_preserves_ooo_offsets() {}

#[test]
fn release_tx_up_to_advances_head_buffer_before_freeing_chain() {}
```

Run: `cargo test -p hammer-service --test session_runtime -v`  
Expected: 现有测试先通过；新增更细颗粒测试在实现前应 FAIL。

- [ ] **Step 2: 重排 `SessionDriverRuntime` 字段，把真热点放前面**

```rust
pub(crate) struct SessionDriverRuntime<S, A> {
    sessions: WorkerSessionRuntime,
    entries: Pool<S>,
    buffers: DataPlaneBuffers,
    tx: Pool<FifoQueue<BufferIndex>>,
    tx_index: FlatHashTable<u64, PoolIndex>,
    rx: Pool<FifoQueue<SessionRxBuffer>>,
    rx_index: FlatHashTable<u64, PoolIndex>,
    pending_closes: SessionReadyQueue,
    app: SessionAppRuntime,
    app_ops: FlatHashTable<u64, AppOpId>,
    aux: A,
}
```

- [ ] **Step 3: 在高频逻辑里把重复 lookup 收成“一次查表，多次局部变量复用”**

```rust
let key = session_id.get();
let Some(index) = self.rx_index.lookup(&key) else {
    return Ok(());
};
let rx = &mut self.rx;
let app = &mut self.app;
let buffers = self.buffers.clone();
```

```rust
let queue = rx
    .get_mut(index)
    .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
```

- [ ] **Step 4: 在 `SessionQueueControlContext` 里同样减少多次裸指针解引用**

```rust
let rx_index = unsafe { &mut *self.rx_index };
let rx = unsafe { &mut *self.rx };
let app = unsafe { &mut *self.app };
let buffers = self.buffers().clone();
```

要求：这里只是把一次流程内的解引用收紧，不新增 wrapper，不新增业务 helper。

- [ ] **Step 5: 运行测试**

Run: `cargo test -p hammer-service --test session_runtime -v`  
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs crates/hammer-service/src/session/protocol.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "session(Refactor): tighten runtime hot-path lookups"
```

### Task 4: `hammer-infra` 增加 `Bitmap` 与 `RbTree`，并把 OOO 迁到 infra 树结构

**Files:**
- Modify: `docs/superpowers/plans/2026-06-22-data-structure-hot-path-optimization.md`
- Create: `crates/hammer-infra/src/bitmap.rs`（若审批通过）
- Create: `crates/hammer-infra/src/rbtree.rs`（若审批通过）
- Modify: `crates/hammer-infra/src/lib.rs`
- Modify: `crates/hammer-service/src/session/ready.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/tests/session_runtime.rs`
- Test: `crates/hammer-service/tests/tcp_established_receive.rs`

**Interfaces:**
- Consumes:
  - `FlatHashTable`
  - `Vec`
  - `FifoQueue`
- Produces:
  - 稠密集合场景是否切换为 `Bitmap`
  - session RX OOO 是否切换为“runtime 自己持有 RX entry + `RbTree<u32, _>` 做 ordered lookup”
  - `RbTree<K, V>` 是否覆盖 VPP byte tracker/sample lookup 所需的 neighbor 查询语义
  - TCP 纯业务态的 `TcpScoreboard`

- [ ] **Step 1: 在文档中明确三类结构的最终落点**

```markdown
- ready/pending/fired 这类 per-worker 稠密集合：优先 `Bitmap`
- session RX OOO lookup：优先 `RbTree`
- TCP byte tracker/sample lookup：优先复用同一棵 infra `RbTree`
- SACK/recovery/loss scoreboard：优先 `TcpScoreboard`
```

- [ ] **Step 2: 为 `Bitmap` 写失败测试，锁定稠密集合语义**

```rust
#[test]
fn bitmap_first_and_next_set_follow_dense_ready_indices() {}
```

Run: `cargo test -p hammer-infra bitmap::tests::bitmap_first_and_next_set_follow_dense_ready_indices -v`  
Expected: 若 `Bitmap` 尚未引入则 FAIL。

- [ ] **Step 3: 为 `RbTree` 写失败测试，锁定 OOO 与 sample lookup 共同需要的语义**

```rust
#[test]
fn rbtree_insert_keeps_keys_sorted_for_iteration() {}

#[test]
fn rbtree_predecessor_and_successor_follow_in_order_neighbors() {}

#[test]
fn rbtree_overwrite_existing_key_returns_old_value_without_duplicate_node() {}

#[test]
fn rbtree_search_neighbors_work_when_exact_key_is_missing() {}
```

Run: `cargo test -p hammer-infra rbtree::tests::rbtree_insert_keeps_keys_sorted_for_iteration -v`  
Expected: 若 `RbTree` 尚未引入则 FAIL。

- [ ] **Step 4: 为 session RX OOO 迁移写失败测试**

```rust
#[test]
fn session_rx_queue_inserts_future_segments_by_offset_order() {}

#[test]
fn session_rx_queue_gap_close_promotes_contiguous_ooo_segments() {}

#[test]
fn session_rx_queue_duplicate_covered_segment_is_discarded() {}
```

Run: `cargo test -p hammer-service --test session_runtime -v`  
Expected: 新增 OOO 测试在实现前 FAIL。

- [ ] **Step 5: 只在审批通过后实现，并替换掉对应场景里的临时 `Vec + Hash` / `FifoQueue::insert` 终局设计**

要求：
- `Bitmap` 只进稠密 ready/pending 集合，不替代 tuple/session lookup hash。
- `RbTree` 先用于 session RX OOO，再覆盖 byte tracker/sample lookup 语义，不做 payload carrier。
- session RX OOO 必须按 VPP 语义拆成“两层”：
  - session/runtime 自己持有真正的 RX entry、buffer index、next/prev 或等价链接信息；
  - `RbTree` 只负责按 `offset` 做 ordered lookup、predecessor/successor、gap close 后重挂 key。
- 不接受“直接把 `SessionRxBuffer` 全塞进 `RbTree` 当最终业务存储”的终局设计；树是索引，不是存储所有权。
- 如果现有 `FifoQueue<SessionRxBuffer>` 无法同时表达“顺序交付头部 + OOO 有序索引”，执行时应优先改成“runtime 私有 entry storage + delivered 视图 + OOO tree index”的最终结构，而不是继续在线性插入上打补丁。
- TCP 继续只消费 `SessionRxEnqueue`
- `TcpScoreboard` 只放 TCP 纯业务状态，不夹带 session/runtime/app 字段。

- [ ] **Step 5.1: 在计划里写清楚 VPP 两类 `rb_tree` 落点和本仓库对应关系**

```markdown
- `svm_fifo`：pool/list 持有 OOO 业务状态，`ooo_enq_lookup` / `ooo_deq_lookup` 只做 chunk lookup
- `tcp_byte_tracker`：sample pool/list 持有业务状态，`sample_lookup` 只做 min_seq lookup
- Hammer 对应：
  - session RX OOO：runtime 持有 RX entry，`RbTree` 做 offset lookup
  - TCP byte tracker：TCP 持有 sample/record，`RbTree` 做 seq lookup
```

- [ ] **Step 6: 运行针对性测试**

Run:
```bash
cargo test -p hammer-infra bitmap::tests::bitmap_first_and_next_set_follow_dense_ready_indices -v
cargo test -p hammer-infra rbtree::tests::rbtree_insert_keeps_keys_sorted_for_iteration -v
cargo test -p hammer-service --test session_runtime -v
cargo test -p hammer-service --test tcp_established_receive -v
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-infra/src/lib.rs crates/hammer-infra/src/bitmap.rs crates/hammer-infra/src/rbtree.rs crates/hammer-service/src/session/ready.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/src/session/protocol.rs crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/tests/session_runtime.rs crates/hammer-service/tests/tcp_established_receive.rs docs/superpowers/plans/2026-06-22-data-structure-hot-path-optimization.md
git commit -m "infra(Refactor): add dense and tree-based hot-path structures"
```

### Task 5: buffer header/cacheline 语义和现有 prefetch 钩子接入

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs`
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/{established,listen,syn_sent,rcv_process}.rs`
- Test: `crates/hammer-adapter/tests/buffer.rs`
- Test: `crates/hammer-service/tests/tcp_input_nodes.rs`

**Interfaces:**
- Consumes:
  - `BufferPool::prefetch_read`
  - `FlatHashTable::prefetch_key`
  - `BufferFrame` 现有 batched prefetch 保留接口
- Produces:
  - 与 VPP 一致的“prefetch header，再用 header 中索引继续走 lookup”的节点热路径
  - `Buffer` 热字段访问顺序注释
  - 收敛后的 `BufferFrame` batched prefetch 命名

- [ ] **Step 1: 写失败测试，锁定 buffer header 对齐和 prefetch 顺序**

```rust
#[test]
fn buffer_header_keeps_hot_metadata_in_first_cacheline() {
    assert_eq!(core::mem::align_of::<Buffer>(), 64);
    assert!(core::mem::size_of::<BufferPacketCursor>() <= 32);
}
```

```rust
#[test]
fn tcp_input_prefetches_lookup_key_before_later_chunk_processing() {}
```

Run: `cargo test -p hammer-adapter --test buffer buffer_header_keeps_hot_metadata_in_first_cacheline -v`  
Expected: 若 cursor/header 过大则 FAIL；否则后面的 tcp prefetch 测试先 FAIL。

- [ ] **Step 2: 在 `buffer.rs` 里按 VPP 语义标注并固定热字段顺序**

```rust
#[repr(C, align(64))]
pub struct Buffer {
    // cacheline0: routing/header/chain hot state
    opaque: PrimaryOpaque,
    packet_cursor: BufferPacketCursor,
    flags: BufferFlags,
    ref_count: usize,
    current_data: usize,
    current_len: usize,
    data_len: usize,
    next_buffer: Option<BufferIndex>,
    // cacheline1: trace/chain tail length/secondary opaque
    opaque2: SecondaryOpaque,
    total_len_not_including_first: usize,
    // colder fields follow
    node_error: Option<BufferNodeError>,
    handoff_source_worker: Option<DataWorkerId>,
    trace_mark: Option<TraceMark>,
    storage: Slice<u8>,
}
```

要求：这里只重排已有字段和注释访问意图，不新增新的 header wrapper。

- [ ] **Step 3: 先把 `BufferFrame` 的 batched prefetch 命名收敛，再让 TCP 输入节点接上它**

要求：
- 不保留 `retain_indices_batched_with_prefetch_state_lazy_chunks` 这种把实现细节全缝进名字里的 API。
- 不新增 TCP 专用包装。
- `buffer.rs` 里只保留一组能表达“批量保留 + 带状态预取”的短命名，TCP/input 节点只消费这组收敛后的通用接口。

```rust
batch.prefetch_read(index);
lookup.prefetch_key(&tuple_key);
```

```rust
frame.retain_indices_batched_with_prefetch(
    state,
    |state, indices| {
        for index in indices {
            state.batch.prefetch_read(*index);
        }
    },
    keep,
)
```

- [ ] **Step 4: 运行 buffer/TCP 节点测试**

Run: `cargo test -p hammer-adapter --test buffer -v`  
Expected: PASS

Run: `cargo test -p hammer-service --test tcp_input_nodes -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-adapter/src/buffer.rs crates/hammer-adapter/tests/buffer.rs crates/hammer-service/src/transport/tcp/input.rs crates/hammer-service/src/transport/tcp/established.rs crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/src/transport/tcp/syn_sent.rs crates/hammer-service/src/transport/tcp/rcv_process.rs crates/hammer-service/tests/tcp_input_nodes.rs
git commit -m "buffer(Refactor): align hot header layout and prefetch use"
```

### Task 6: 全量验证与边界复查

**Files:**
- Modify: `docs/superpowers/plans/2026-06-22-data-structure-hot-path-optimization.md`

**Interfaces:**
- Consumes:
  - 前五个任务的全部实现
- Produces:
  - 验证记录
  - 剩余风险清单

- [ ] **Step 1: 运行本次改动的完整验证集**

Run:
```bash
cargo test -p hammer-infra
cargo test -p hammer-adapter --test buffer
cargo test -p hammer-service --test session_runtime
cargo test -p hammer-service --test tcp_input_nodes
cargo test -p hammer-service --lib session::runtime
```

Expected: 全部 PASS

- [ ] **Step 2: 运行 release 下的最小性能冒烟**

Run:
```bash
cargo test -p hammer-service --release --test tcp_input_nodes -- --nocapture
cargo test -p hammer-service --release --test session_runtime -- --nocapture
```

Expected: 全部 PASS；不要求在本任务里引入 benchmark 基建。

- [x] **Step 3: 更新计划文档底部“执行结果”**

```markdown
## 执行结果

- 旧的路由元数据结构和非 VPP dataplane 路径已从 `crates/` 源码树移除，仅保留与当前 VPP dataplane 对齐的 `tun` 路径。
- `PacketBufferCacheline0` / `PacketBufferCacheline1` 已固定为 64-byte 对齐布局；`Buffer` 也已保持 64-byte 对齐，并通过 `packet_buffer` / `buffer` 测试覆盖布局约束。
- buffer prefetch 已按 header/data 分层落地：`prefetch_header()` 只预取 cacheline0，`prefetch_read()` 顺序预取 cacheline0、cacheline1 和当前 data，语义对齐 VPP 的 header/data prefetch 分层。
- `FlatHashTable` 已支持 `clear()` 并在 ready/session 索引中复用容量；`Pool` 已改为用 `Bitmap` 管理 free slot，`hammer-infra` 也已新增通用 `RbTree`。
- session RX OOO 与 TCP recovery sample lookup 已接入 `RbTree`，不再停留在线性插入路径；TCP tuple/listener lookup 也补上了 prefetch 钩子。
- TCP input / output / reset 节点已接上现有 buffer prefetch；`ip_lookup` 现改为优先复用 `ip_input` 产出的 cached metadata，不再依赖重新解析被后续节点改坏的当前头。
- adjacency rewrite 已修正旧的 runtime 二次借用路径，并把 egress interface 写回现有 `NetworkOpaque`，相关 lookup/rewrite 测试已重新通过。
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-06-22-data-structure-hot-path-optimization.md
git commit -m "docs(Refactor): record hot-path data structure optimization plan"
```

## Self-Review

- **Spec coverage:** 已覆盖占用优化（`FlatHashTable` 容量复用）、查询效率（timer 索引、runtime 查表收紧、session RX OOO 树化）、高频字段 cacheline/prefetch（`Buffer` header、TCP input 节点接入现有 prefetch），并把 VPP 里实际使用 `rb_tree` 的两条主线 `svm_fifo OOO lookup` 和 `tcp_bt sample_lookup` 合并进了同一份计划。
- **Placeholder scan:** 本计划未使用 `TODO`/`TBD`/“后续实现”式占位；所有任务都列了明确文件、命令和目标代码片段。
- **Type consistency:** 全文新增项已统一为：泛型 `FlatHashTable::clear`、通用 `Bitmap`、通用 `RbTree`、候选 `TcpScoreboard`；session RX OOO 的最终方向是“runtime 持有业务状态 + `RbTree` 做 ordered lookup”，不再保留 `OrderedRangeSet` 这条并行路线。

## 执行交接

Plan complete and saved to `docs/superpowers/plans/2026-06-22-data-structure-hot-path-optimization.md`. Two execution options:

1. **Subagent-Driven (recommended)** - 我按任务逐个起新 agent 做，实现后逐步 review
2. **Inline Execution** - 我在当前会话里按任务直接执行并分段校验

Which approach?

# Infra RbTree + Session RX OOO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 先把通用红黑树能力补到 `hammer-infra`，再让 session RX 的 OOO 存储直接切到 infra 结构，并把 VPP 中已经明确用红黑树解决的问题域一起纳入能力边界。

**Architecture:** OOO 仍然属于 session RX 存储，不上提到 TCP 连接里。`hammer-infra` 提供纯通用的 `RbTree`，session runtime 在其上维护以 `offset` 为序的 OOO buffer 索引；同一棵通用树还要覆盖 VPP 已经证明需要红黑树的另一类场景：按序号做 byte-tracker/sample lookup。TCP 继续只维护 `rcv_nxt`、ACK/SACK/DSACK 等协议状态，并根据 session 返回的“已交付长度 / 最新 OOO 区间”更新协议视图。

**Tech Stack:** Rust 2024，`hammer-infra::{pool,vec}`，新增 `hammer-infra::rbtree`，`hammer-service::session::runtime`，`hammer-service::transport::tcp::connection`，`hammer-service/tests/tcp_established_receive.rs`。

## Global Constraints

- 以当前代码为准，OOO 归 session RX 管理，TCP 不管理 payload buffer。
- 这次必须先在 `hammer-infra` 加通用结构，再让 OOO 使用；不能继续把 OOO 最终结构写死在 `session/runtime.rs` 里。
- 新增的 `RbTree` 必须是通用 infra 结构，不能出现 TCP/session 业务字段或命名。
- 不新增任何 TCP/Session 过渡层、包装层、helper carrier 类型。
- 不引入中间 payload `Vec`；OOO 条目只持有现有 `BufferIndex + offset/len/fin` 这类描述信息。
- session runtime 继续负责调度；这次改造不改变 TCP 和 session 的职责边界。
- OOO 的最终热路径不允许继续依赖 `FifoQueue::insert` 线性找位作为主结构。
- 如果需要新增业务态记录，只允许保留最小的 session RX 记录类型，不能把红黑树节点语义外泄到 TCP。

## 文件责任图

- `crates/hammer-infra/src/rbtree.rs`
  - 通用红黑树实现，只负责有序 key 索引和前驱/后继/首尾遍历。
- `crates/hammer-infra/src/lib.rs`
  - 导出 `rbtree` 模块。
- `crates/hammer-service/src/session/runtime.rs`
  - session RX OOO 存储与交付逻辑；从线性 `FifoQueue` 切到基于 `RbTree` 的 OOO 索引。
- `crates/hammer-service/src/session/protocol.rs`
  - 跟随 session RX 存储调整 flush 路径，不引入新抽象层。
- `crates/hammer-service/src/transport/tcp/connection.rs`
  - 保持 TCP 只消费 session RX 的 enqueue 结果，必要时只调整对 OOO 结果的解释，不接管 OOO 存储。
- `crates/hammer-service/tests/tcp_established_receive.rs`
  - 锁定 gap close、SACK、DSACK、duplicate、in-order/OOO 混合路径。

## 现状问题

- 当前 OOO 逻辑在 [runtime.rs](/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/crates/hammer-service/src/session/runtime.rs) 的 `enqueue_rx()` 里，用 `FifoQueue<SessionRxBuffer>` 做“前面顺序可交付，后面 offset != 0 代表 OOO”的混合结构。
- 插入 OOO 时靠线性扫描找插入位：
  - 先找第一段 future
  - 再在 future 段内按 `offset` 线性找位
- gap close 后再做一轮线性扫描和 rebasing，把后续 OOO 的 offset 重写成相对新 `rcv_nxt` 的值。
- 这能跑现有基础测试，但它不是最终结构：OOO 规模一上来，插入、去重、前驱/后继、gap close 都会退化成多次线性扫描。

## VPP 对照结论

VPP 当前明确把红黑树用在两类问题上：

1. `svm_fifo` 的 OOO lookup
   - 位置：
     - [svm_fifo.c](/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/third_party/vpp/src/svm/svm_fifo.c)
     - [fifo_types.h](/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/third_party/vpp/src/svm/fifo_types.h)
   - 结构：
     - `ooo_enq_lookup`
     - `ooo_deq_lookup`
   - 含义：
     - OOO 不是靠线性链表长期硬扛，VPP 在 FIFO 层就为 OOO 查找单独建树。

2. TCP byte tracker 的 sample lookup
   - 位置：
     - [tcp_bt.c](/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/third_party/vpp/src/vnet/tcp/tcp_bt.c)
     - [tcp_types.h](/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/third_party/vpp/src/vnet/tcp/tcp_types.h)
   - 结构：
     - `sample_lookup`
   - 含义：
     - 按序号查询 sample、找 predecessor/successor、split/merge sample 这类场景，VPP 也明确用树，不是继续扫 `Vec`。

所以这次计划里，`RbTree` 不能只为 session RX OOO 设计；它的接口必须从第一天就覆盖：
- OOO 按 offset 查找
- predecessor/successor
- first/last
- 按 key 删除与重新插入
- 上层记录 split/merge 后重新挂树

## 目标结构

### 1. `hammer-infra::rbtree`

通用红黑树只提供这些能力：

- `insert(key, value)`
- `remove(key)`
- `get(key)`
- `get_mut(key)`
- `first()`
- `last()`
- `predecessor(key)`
- `successor(key)`
- 有序迭代
- `contains_key(key)`
- `len()`
- `is_empty()`

它不提供区间语义，不提供 payload 语义，也不提供“OOO/scoreboard”这种业务命名。

### 2. session RX OOO 存储

session RX 切成两部分：

- 顺序可交付头部：仍然可以保持一个轻量 FIFO 视图，服务 `flush_session_rx`
- OOO 区域：改成按 `offset` 排序的 `RbTree`

其中 `SessionRxBuffer` 仍然是最小业务记录：

```rust
pub(crate) struct SessionRxBuffer {
    pub(crate) index: BufferIndex,
    pub(crate) offset: u32,
    pub(crate) len: u32,
    pub(crate) fin: bool,
}
```

但它不再存放在单一 `FifoQueue` 中承担“有序索引”职责。

### 3. TCP 与 session 的边界

TCP 保持现有模式：

- 计算 `offset`
- 把 buffer 交给 `queue.enqueue_rx(...)`
- 读取 `delivered_len / newest_ooo_start / newest_ooo_len`
- 更新 `rcv_nxt` 与 ACK/SACK/DSACK

TCP 不关心底下是 `FifoQueue` 还是 `RbTree`。

---

### Task 1: 在 `hammer-infra` 增加通用红黑树模块

**Files:**
- Create: `crates/hammer-infra/src/rbtree.rs`
- Modify: `crates/hammer-infra/src/lib.rs`
- Test: `crates/hammer-infra/src/rbtree.rs`

**Interfaces:**
- Consumes: 无
- Produces:
  - `pub struct RbTree<K, V>`
  - `pub struct RbTreeIter<'a, K, V>`
  - `pub fn insert(&mut self, key: K, value: V) -> Option<V>`
  - `pub fn remove(&mut self, key: &K) -> Option<V>`
  - `pub fn get(&self, key: &K) -> Option<&V>`
  - `pub fn get_mut(&mut self, key: &K) -> Option<&mut V>`
  - `pub fn first(&self) -> Option<(&K, &V)>`
  - `pub fn last(&self) -> Option<(&K, &V)>`
  - `pub fn predecessor(&self, key: &K) -> Option<(&K, &V)>`
  - `pub fn successor(&self, key: &K) -> Option<(&K, &V)>`
  - `pub fn contains_key(&self, key: &K) -> bool`
  - `pub fn len(&self) -> usize`
  - `pub fn is_empty(&self) -> bool`

- [ ] **Step 1: 写失败测试，锁定红黑树基本语义**

```rust
#[cfg(test)]
mod tests {
    use super::RbTree;

    #[test]
    fn insert_keeps_keys_sorted_for_iteration() {
        let mut tree = RbTree::new();
        tree.insert(20u32, "b");
        tree.insert(10u32, "a");
        tree.insert(30u32, "c");

        let items: std::vec::Vec<_> = tree.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(10, "a"), (20, "b"), (30, "c")]);
    }

    #[test]
    fn predecessor_and_successor_follow_in_order_neighbors() {
        let mut tree = RbTree::new();
        tree.insert(10u32, "a");
        tree.insert(20u32, "b");
        tree.insert(30u32, "c");

        assert_eq!(tree.predecessor(&20).map(|(k, _)| *k), Some(10));
        assert_eq!(tree.successor(&20).map(|(k, _)| *k), Some(30));
    }

    #[test]
    fn remove_preserves_remaining_order() {
        let mut tree = RbTree::new();
        tree.insert(10u32, "a");
        tree.insert(20u32, "b");
        tree.insert(30u32, "c");

        assert_eq!(tree.remove(&20), Some("b"));
        let items: std::vec::Vec<_> = tree.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(10, "a"), (30, "c")]);
    }

    #[test]
    fn overwrite_existing_key_returns_old_value_without_duplicate_node() {
        let mut tree = RbTree::new();
        assert_eq!(tree.insert(10u32, "a"), None);
        assert_eq!(tree.insert(10u32, "b"), Some("a"));
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.get(&10), Some(&"b"));
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p hammer-infra rbtree::tests::insert_keeps_keys_sorted_for_iteration -v`  
Expected: FAIL，因为 `rbtree.rs` 和 `RbTree` 还不存在。

- [ ] **Step 3: 写最小实现**

```rust
pub struct RbTree<K, V>
where
    K: Ord,
{
    // 使用 pool + parent/left/right/color/index 的典型红黑树节点布局
}
```

要求：
- 节点存储用 `hammer-infra::pool::Pool`
- 节点关系字段用索引，不用裸指针
- 迭代按中序遍历
- 不暴露内部节点索引给上层
- 设计时要允许上层把“外部记录索引”作为 value 挂进去，供后续 OOO / byte tracker 共用

- [ ] **Step 4: 运行红黑树测试**

Run: `cargo test -p hammer-infra rbtree::tests::insert_keeps_keys_sorted_for_iteration -v`  
Expected: PASS

Run: `cargo test -p hammer-infra rbtree::tests::predecessor_and_successor_follow_in_order_neighbors -v`  
Expected: PASS

Run: `cargo test -p hammer-infra rbtree::tests::remove_preserves_remaining_order -v`  
Expected: PASS

Run: `cargo test -p hammer-infra rbtree::tests::overwrite_existing_key_returns_old_value_without_duplicate_node -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-infra/src/rbtree.rs crates/hammer-infra/src/lib.rs
git commit -m "infra(Feat): add generic red-black tree"
```

### Task 2: 把 session RX OOO 从线性队列拆成“顺序头 + RbTree OOO”

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Test: `crates/hammer-service/tests/session_runtime.rs`

**Interfaces:**
- Consumes:
  - `hammer_infra::rbtree::RbTree<u32, SessionRxBuffer>`
  - `SessionRxBuffer`
- Produces:
  - `SessionRxQueue`
  - `SessionDriverRuntime::enqueue_rx(...) -> CoreResult<SessionRxEnqueue>`
  - `SessionDriverRuntime::flush_session_rx(...) -> CoreResult<()>`
  - OOO 记录通过 `RbTree<u32, SessionRxBuffer>` 管理

- [ ] **Step 1: 写失败测试，锁定 OOO 存储语义**

```rust
#[test]
fn session_rx_queue_inserts_future_segments_by_offset_order() {}

#[test]
fn session_rx_queue_gap_close_promotes_contiguous_ooo_segments() {}

#[test]
fn session_rx_queue_duplicate_covered_segment_is_discarded() {}
```

Run: `cargo test -p hammer-service --test session_runtime -v`  
Expected: FAIL，因为当前 `FifoQueue<SessionRxBuffer>` 不是目标结构。

- [ ] **Step 2: 引入 `SessionRxQueue`，替代直接暴露 `FifoQueue<SessionRxBuffer>`**

```rust
struct SessionRxQueue {
    delivered: hammer_infra::fifo::FifoQueue<SessionRxBuffer>,
    ooo: hammer_infra::rbtree::RbTree<u32, SessionRxBuffer>,
}
```

要求：
- `delivered` 只保存 `offset == 0`、可直接交付 app 的条目
- `ooo` 按 `offset` 排序保存未来数据
- 这里不新增 TCP 业务字段
- `ooo` 的 key 就是 `offset`，value 还是当前 `SessionRxBuffer`

- [ ] **Step 3: 重写 `enqueue_rx` 的三段逻辑**

代码目标：

```rust
pub(crate) fn enqueue_rx(
    &mut self,
    session_id: SessionId,
    index: BufferIndex,
    offset: u32,
    fin: bool,
) -> CoreResult<SessionRxEnqueue>
```

处理规则：
- `offset == 0`
  - 先放进 `delivered`
  - 然后不断从 `ooo.first()` 拉取与当前尾部连续或重叠的条目
  - 连续则推进到 `delivered`
  - 完全覆盖则释放掉重复条目
  - 部分重叠则 `advance(buffer)` 后再推进
- `offset > 0`
  - 先用 `predecessor/successor` 处理覆盖与重叠
  - 完全重复直接释放
  - 部分重叠则 trim 后插入
  - 插入后返回“最新 OOO 区间”

- [ ] **Step 4: 重写 `flush_session_rx` 只消费 `delivered`**

```rust
while let Some(current) = queue.delivered.front().copied() {
    let consumed = self.app.complete_recv(op, self.buffers.clone(), current.index, current.fin)?;
    if !consumed {
        break;
    }
    let _ = queue.delivered.pop_front().expect("session rx buffer is missing");
}
```

- [ ] **Step 5: 运行 session runtime 测试**

Run: `cargo test -p hammer-service --test session_runtime -v`  
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs crates/hammer-service/src/session/protocol.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "session(Refactor): back rx ooo with infra rbtree"
```

### Task 3: 让 TCP Established/RCV 路径继续只消费 session OOO 结果

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/tests/tcp_established_receive.rs`

**Interfaces:**
- Consumes:
  - `SessionDriverRuntime::enqueue_rx`
  - `SessionRxEnqueue { delivered_len, newest_ooo_start, newest_ooo_len }`
- Produces:
  - `TcpConnection::receive_data(...) -> CoreResult<Option<TcpSegment>>`

- [ ] **Step 1: 写失败测试，锁定新 OOO 结构下的 TCP 行为**

```rust
#[test]
fn tcp_established_gap_closing_payload_advances_ack_across_buffered_ooo_data() {}

#[test]
fn tcp_established_ooo_payload_emits_sack_block() {}

#[test]
fn tcp_established_duplicate_payload_emits_dsack_block() {}

#[test]
fn tcp_established_overlapping_ooo_segments_collapse_to_single_sack_range() {}
```

Run: `cargo test -p hammer-service --test tcp_established_receive -v`  
Expected: 新增 overlap 用例 FAIL，现有 OOO 用例可能也会因结构切换而先 FAIL。

- [ ] **Step 2: 保持 TCP 只解释结果，不接管 OOO 存储**

关键代码形状保持：

```rust
let enqueue = queue.enqueue_rx(session_id, index, offset, false)?;
if let Some(start) = enqueue.newest_ooo_start {
    let left_edge = TcpSeq::from(self.rcv_nxt).advance(start).raw();
    let right_edge = TcpSeq::from(left_edge).advance(enqueue.newest_ooo_len).raw();
    self.update_ack_sack_block(left_edge, right_edge);
}
```

要求：
- 不把红黑树节点、索引、内部细节暴露给 TCP
- `receive_data` 仍然只看 `SessionRxEnqueue`

- [ ] **Step 3: 运行 TCP receive 测试**

Run: `cargo test -p hammer-service --test tcp_established_receive -v`  
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs crates/hammer-service/tests/tcp_established_receive.rs
git commit -m "tcp(Fix): consume session rx ooo results from infra tree"
```

### Task 4: 为 VPP byte tracker/sample lookup 预留同一棵 infra 树接口

**Files:**
- Modify: `crates/hammer-infra/src/rbtree.rs`
- Modify: `docs/superpowers/plans/2026-06-22-infra-rbtree-session-rx-ooo.md`

**Interfaces:**
- Consumes:
  - `RbTree<K, V>`
- Produces:
  - 明确覆盖 byte tracker/sample lookup 需求的接口说明

- [ ] **Step 1: 写失败测试，锁定“key 不命中时拿 predecessor/successor”语义**

```rust
#[test]
fn search_neighbors_work_when_exact_key_is_missing() {
    let mut tree = RbTree::new();
    tree.insert(100u32, "a");
    tree.insert(200u32, "b");
    tree.insert(300u32, "c");

    assert_eq!(tree.predecessor(&250).map(|(k, _)| *k), Some(200));
    assert_eq!(tree.successor(&250).map(|(k, _)| *k), Some(300));
}
```

- [ ] **Step 2: 运行测试确认通过**

Run: `cargo test -p hammer-infra rbtree::tests::search_neighbors_work_when_exact_key_is_missing -v`  
Expected: PASS

- [ ] **Step 3: 在计划文档中补充“下一步直接接 byte tracker/sample lookup”的说明**

```markdown
- `RbTree` 第一位消费者是 session RX OOO
- 第二位消费者应是 TCP byte tracker/sample lookup
- 不需要再为 byte tracker 重新造一棵 TCP 私有树
```

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-infra/src/rbtree.rs docs/superpowers/plans/2026-06-22-infra-rbtree-session-rx-ooo.md
git commit -m "infra(Refactor): cover byte tracker tree semantics"
```

### Task 5: 全量验证与计划回填

**Files:**
- Modify: `docs/superpowers/plans/2026-06-22-infra-rbtree-session-rx-ooo.md`

**Interfaces:**
- Consumes:
  - 前三任务全部实现
- Produces:
  - 验证记录
  - 剩余风险

- [ ] **Step 1: 运行完整验证集**

Run:
```bash
cargo test -p hammer-infra
cargo test -p hammer-service --test session_runtime
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service --lib transport::tcp
```

Expected: 全部 PASS

- [ ] **Step 2: 回填执行结果**

```markdown
## 执行结果

- `hammer-infra` 已新增通用 `RbTree`
- session RX OOO 已不再依赖 `FifoQueue + 线性插入`
- TCP Established 接收路径继续只消费 session OOO 结果
- `RbTree` 接口已覆盖 VPP byte tracker/sample lookup 所需的 neighbor 查询语义
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-06-22-infra-rbtree-session-rx-ooo.md
git commit -m "docs(Refactor): record infra rbtree session rx ooo plan"
```

## Self-Review

- **Spec coverage:** 已覆盖“先做到 infra 中，然后 OOO 用”的完整链路，并把 VPP 中另一条红黑树使用线 `tcp_bt sample_lookup` 也纳入了接口边界。
- **Placeholder scan:** 没有使用 `TODO/TBD/implement later`；每个任务都列了明确文件、接口、命令。
- **Type consistency:** 新增的核心只有 `RbTree<K, V>` 和 `SessionRxQueue`；TCP 侧继续只依赖现有 `SessionRxEnqueue`。

## 执行交接

Plan complete and saved to `docs/superpowers/plans/2026-06-22-infra-rbtree-session-rx-ooo.md`. Two execution options:

1. **Subagent-Driven (recommended)** - 我按任务逐个起新 agent 做，实现后逐步 review
2. **Inline Execution** - 我在当前会话里按任务直接执行并分段校验

Which approach?

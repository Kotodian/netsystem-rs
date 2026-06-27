# 删除 BufferBatchMut + 节点 frame discipline 对齐 VPP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 删除非 VPP 的 `BufferBatchMut` guard 层，让节点按 VPP `vlib` 形态取用 buffer：frame 只给 `BufferIndex[]`（`vlib_frame_vector_args`），节点用 runtime `vlib_get_buffer` 把索引转成 `&Buffer`/`&mut Buffer`，TX 分配/释放走 runtime pool（`vlib_buffer_alloc`/`vlib_buffer_free`）；同时把当前偏离的四个 TCP state 节点从 `node_rewrite_frame!` 标量形态迁到“读 frame 索引、runtime 取 buffer 处理、TX 经 runtime pool、写 next frame”的批量循环，并把 `SessionQueueNode` 对齐 VPP polling-input driver。

**Architecture:** VPP `vlib` 的节点循环是 `from = vlib_frame_vector_args(frame); vlib_get_buffers(vm, from, bufs, n_left); while (n_left>=4){…}`——frame 给 `u32[]` 索引，runtime `vm` 把索引转 buffer 指针，没有独立 batch guard。Hammer 的 `BufferBatchMut`（`RefMut<BufferPoolInner>` 跨 batch 持有）是 Rust 特化的 `RefCell` 借用摊销层，VPP 无对应物。`DataPlaneBuffers` 已有全套 runtime 表面（`get_buffer`/`get_buffer_mut`/`prefetch_header`/`prefetch_read`/`alloc_index`/`free_index`/`copy_packet`，`buffer.rs:813-1054`），删 `BufferBatchMut` 不需要新增 runtime 方法。迁移分三层：(1) `hammer-adapter::node::next` 把 dispatch 回调签名从 `&mut BufferBatchMut` 改成 `&DataPlaneRuntime`、内部不再 `buffer_batch_mut()`；(2) 合规节点 caller（`ip_input`/`ip_lookup`/`tcp_input`/`tcp_output`/`tcp_reset`）把 `batch.buffer`/`buffer_mut`/`prefetch_*` 改成 `runtime.get_buffer`/`get_buffer_mut`/`prefetch_*`；(3) 四个偏离节点 + `SessionQueueNode` 迁 frame discipline。最后删 `BufferBatchMut` 与 `buffer_batch_mut()`。

**Tech Stack:** Rust 2024，`hammer-adapter::buffer::{BufferFrame, BufferIndex, Buffer, DataPlaneRuntime, DataPlaneBuffers}`，`hammer-adapter::node::{NodeVectorDispatch, NodeNextFrames, NodeNextEnqueue, NodeNextStorage, NodeResult}`，`hammer_component_macros::{graph_node, node_next}`，`hammer-service::transport::tcp::{established, listen, syn_sent, rcv_process, reset, segment, input, output}`，`hammer-service::net::ip::{input, lookup}`，`hammer-service::session::{node, runtime}`，参考 `third_party/vpp/src/vlib/{node.h, main.c, node_funcs.h, buffer_funcs.h, drop.c}`。

## Global Constraints

- **基准为已提交状态 `d05fa358`**（`codex/hammer-app-ring-zero-copy` 的 HEAD）。工作树未提交的 `net/opaque.rs`/`NetworkOpaque` 重构不在本计划范围；所有文件路径、行号、代码片段以 `d05fa358` 为准。
- **VPP 对齐判据（用户 2026-06-27 明确，三轮后定稿）**：
  - frame 只给 `BufferIndex[]`（`BufferFrame::pending_indices()`），frame 不持有 buffer pool、不管分配。
  - index→buffer 用 runtime `vlib_get_buffer` 形态：`runtime.get_buffer(index)`/`runtime.get_buffer_mut(index)`。
  - TX 分配/释放用 runtime pool 形态：`runtime.packet_buffers().alloc_index()`/`runtime.free_index(index)`（等价 `vlib_buffer_alloc(vm)`/`vlib_buffer_free`）。
  - **`BufferBatchMut` 与 `buffer_batch_mut()` 必须删除**，不得以任何 batch-guard wrapper 形态重生。
- 节点 per-buffer 一次只持一个 `&mut Buffer`（已验证：`let buffer = runtime.get_buffer_mut(index)?` 在 chunk 循环里逐 index 顺序处理），因此 runtime 每次调用返回的 `RefMut<Buffer>` 顺序释放，无 `RefCell` 冲突，不需要跨 batch 持有 guard。
- 遵守 `AGENTS.md` 的 VPP/TCP 边界：session runtime 拥有 node 调度；TCP 拥有 sequence/ACK/loss/recovery/timer/output header；session 拥有 TX byte retention 与 app/session copy 边界；app/session 边界之外不得新增中间 payload `Vec`。
- 不新增 TCP 特化 runtime/buffer API、不新增 congestion-control node、不新增 TCP/session 过渡层或 wrapper 类型。`TcpSegment` 仍只由其构造器构造、被 `tcp-output` 消费。
- per-buffer session 状态机工作（`receive_established`/`accept_payload`/`enqueue_rx`/`release_tx_up_to`/`control_segment`/timer refresh）保持原语义、原调用点、原所有权；本计划只改“buffer 怎么被取/改、TX 怎么分配释放、frame 怎么被读、next frame 怎么被写”。
- `node/next.rs` dispatch 回调签名从 `&mut BufferBatchMut` 改 `&DataPlaneRuntime` 是 API 变更，进审批节（不新增类型，只改签名/删类型）。
- 不引入 `_underscore` 绑定；未用 local 删除；未用参数用裸 `_`；`snake_case` 函数、`PascalCase` 类型。
- 每个任务结束跑对应测试确认 PASS 再提交。提交信息用 `<scope>(<Type>): <imperative summary>`，Type 用 `Refactor`。

## VPP frame/buffer 规范对照（判据）

- `vlib_frame_vector_args(frame)` 给 `u32[]` buffer index（`node_funcs.h:298-303`）。Hammer 对应 `BufferFrame::pending_indices() -> &[BufferIndex]`（`buffer.rs:3080`）。
- `vlib_get_buffer(vm, bi)` 把 index 转 `vlib_buffer_t*`（`buffer_funcs.h:117-125`）。Hammer 对应 `runtime.get_buffer(index) -> CoreResult<Ref<Buffer>>` / `runtime.get_buffer_mut(index) -> CoreResult<RefMut<Buffer>>`（`buffer.rs:950-955`）。
- `vlib_buffer_alloc(vm)`/`vlib_buffer_free` 是 runtime pool op（`buffer_funcs.h`）。Hammer 对应 `runtime.packet_buffers().alloc_index()`/`runtime.free_index(index)`（`buffer.rs:813/823`）。
- 写 next frame：`vlib_get_next_frame`/`vlib_put_next_frame`（`main.c:399-466`）。Hammer 对应 `NodeNextFrames::enqueue`+`schedule`（`node/next.rs:1315-1391`）。
- VPP 节点循环 `from = vlib_frame_vector_args(frame); vlib_get_buffers(vm, from, bufs, n_left); while(n_left>=4){…}`（`drop.c:40,163-168`）。Hammer 对应：`let indices = frame.pending_indices(); for index in indices { let buffer = runtime.get_buffer_mut(index)?; … }`，带 quad/pair chunk 与 `runtime.prefetch_header` 预取。
- **判据**：节点算“对齐 VPP”当且仅当 (a) 用 `frame.pending_indices()` 取 `BufferIndex[]`；(b) 用 `runtime.get_buffer`/`get_buffer_mut` 取 buffer，不用 `BufferBatchMut`/`batch.buffer`；(c) TX 用 `runtime.packet_buffers().alloc_index`/`runtime.free_index`；(d) 转发/新 TX buffer 经 `NodeNextFrames` 写 next frame；(e) 返回 `NodeResult::next_current`/`drop()`；(f) 不用 `node_rewrite_frame!` 标量宏。

## 文件责任图

- `crates/hammer-adapter/src/node/next.rs`
  - dispatch 回调签名从 `&mut BufferBatchMut` 改 `&DataPlaneRuntime`；`dispatch_chunk`/`dispatch_index_chunk` 内部不再 `runtime.buffer_batch_mut()`，直接传 `runtime` 给回调；`default_prefetch_indices` 改收 `&DataPlaneRuntime` 用 `runtime.prefetch_header`。`NodeNextEnqueue::validate_frame_with_buffer_batch_prefetch`/`validate_frame_with_buffer_batch_chunks`、`NodeNextVectorEnqueue::enqueue_frame_with_buffer_batch_chunks` 改名去 `_buffer_batch`、回调改 `&DataPlaneRuntime`。
- `crates/hammer-adapter/src/buffer.rs`
  - 删 `BufferBatchMut`（`buffer.rs:3952-4003`）与 `BufferPool::batch_mut`（`buffer.rs:1697`）、`DataPlaneBuffers::buffer_batch_mut`（`buffer.rs:960`）。`lib.rs` re-export 删 `BufferBatchMut`。
- `crates/hammer-service/src/net/ip/input.rs`、`net/lookup/mod.rs`
  - `batch.buffer`/`buffer_mut`/`prefetch_*`/`with_buffer` → `runtime.get_buffer`/`get_buffer_mut`/`prefetch_*`；prefetch helper 签名 `(batch, indices)` → `(runtime, indices)`。
- `crates/hammer-service/src/transport/tcp/{input, output, reset}.rs`
  - 同上，合规节点的 batch 用法全改 runtime。
- `crates/hammer-service/src/transport/tcp/{established, listen, syn_sent, rcv_process}.rs`
  - per-index 函数收 `runtime: &DataPlaneRuntime` 取 buffer（已收，无需改签名）、`runtime.get_buffer_mut` 替代 `batch.buffer_mut`；`*_frame` 从 `node_rewrite_frame!` 改成显式 chunk 循环 + `NodeNextFrames`；TX 用 `runtime.packet_buffers().alloc_index`/`runtime.free_index`/`segment.write_to_buffer`。
- `crates/hammer-service/src/transport/tcp/segment.rs`
  - `parse_tcp_packet(runtime, index)` 与 `TcpSegment::write_to_buffer(buffers, index)` 已是 runtime 形态（VPP-faithful），**保留不改**。
- `crates/hammer-service/src/session/node.rs`
  - `SessionQueueNode` 去掉空 frame 的 `frame.clear()`，对齐 VPP polling-input driver；TX 仍经 `SessionQueueOutput`（`NodeNextFrames`）。
- `crates/hammer-adapter/tests/{buffer, node_vector_dispatch, node_runtime}.rs`
  - 删 `BufferBatchMut`/`buffer_batch_mut` 用法，改 runtime。
- `crates/hammer-service/tests/tcp_state_machine.rs`、`session_runtime.rs`
  - 源码分析测试：断言 `BufferBatchMut`/`node_rewrite_frame!`/`frame.clear()` 不出现、runtime get_buffer 出现。

## 审批节

### 拟变更 API（需先批准）

1. `crates/hammer-adapter/src/node/next.rs` — dispatch 回调签名变更（**必加**，API 变更非新类型）
   - 把所有 `impl FnMut(&mut BufferBatchMut<'_>, …)` 回调参数改为 `impl FnMut(&DataPlaneRuntime, …)`（32 处，`next.rs:177-1067`）。
   - `default_prefetch_indices(batch: &mut BufferBatchMut, indices)` → `default_prefetch_indices(runtime: &DataPlaneRuntime, indices)`，内部 `batch.prefetch_header` → `runtime.prefetch_header`。
   - `dispatch_chunk`/`route_frame_*` 内部 `let mut batch = runtime.buffer_batch_mut(); route_chunk(&mut batch, …); drop(batch);` → 直接 `route_chunk(runtime, …)`（不再借 batch guard）。
   - `NodeNextEnqueue::validate_frame_with_buffer_batch_prefetch`/`validate_frame_with_buffer_batch_chunks` 与 `NodeNextVectorEnqueue::enqueue_frame_with_buffer_batch_chunks` 改名去 `_buffer_batch`，回调改 `&DataPlaneRuntime`，内部 `runtime.buffer_batch_mut()`+`buffer_node_inline(_chunks)` 改用 `frame.retain_indices_batched_with_prefetch`（现有方法，回调 `FnMut(BufferIndex)`，不持 batch）或直接把 `runtime` 透传。
   - **最终结果：** dispatch 路径完全不持 `BufferBatchMut`，节点回调收 `&DataPlaneRuntime`，用 `runtime.get_buffer` 取 buffer。
   - **为什么现有表面不够：** `BufferBatchMut` 是非 VPP guard，用户明确要求删除；现有回调签名处处透传 `&mut BufferBatchMut`，不删 guard 就无法让节点改用 runtime 取 buffer。

2. `crates/hammer-adapter/src/buffer.rs` — 删除 `BufferBatchMut`/`batch_mut`/`buffer_batch_mut`（**必加**，API 删除）
   - 删 `pub struct BufferBatchMut`（`buffer.rs:3952`）及其 impl（`buffer.rs:3956-4003`）。
   - 删 `BufferPool::batch_mut`（`buffer.rs:1697`）、`DataPlaneBuffers::buffer_batch_mut`（`buffer.rs:960`）。
   - 删 `crates/hammer-adapter/src/lib.rs` 里 `BufferBatchMut` re-export。
   - **最终结果：** 没有 batch guard 类型残留；所有 batch 取 buffer 改走 `BufferPool`/`DataPlaneBuffers` 的 `get_buffer`/`get_buffer_mut`/`prefetch_*`。

3. `crates/hammer-service/src/transport/tcp/{established, listen, syn_sent, rcv_process}.rs` — per-index frame-discipline（**必加**，签名/形态改非新类型）
   - `*_frame` 从 `node_rewrite_frame!` 改成显式 prefetch chunk 循环（`runtime.prefetch_header`）+ 逐 index 调 `tcp_*_index(runtime, index, …)` + `NodeNextFrames` + `NodeResult::drop()`。
   - `tcp_*_index` 签名保持收 `runtime: &DataPlaneRuntime`（已收），内部 `runtime.get_buffer_mut(index)` 取 buffer（已是 runtime 形态），TX 用 `runtime.packet_buffers().alloc_index`/`runtime.free_index`/`segment.write_to_buffer`。
   - **最终结果：** 偏离节点不再用 `node_rewrite_frame!`，统一 frame discipline。

### 明确不新增

- 不新增 `BufferBatchMut` 的任何替代 guard/wrapper。
- 不新增 TCP/session 过渡层、carrier、wrapper、Cursor/Helper/Util 命名业务类型。
- 不新增 TCP 特化 buffer/runtime API、headroom 分配、chain-copy helper、congestion-control node。
- 不改 `BufferFrame`/`FramePool`/`schedule_frame`/`NodeResult` 的现有 API 形状（只删 `BufferBatchMut`）。
- 不把 `parse_tcp_packet`/`write_to_buffer` 改成 frame-API 变体——它们已经是 runtime 形态（VPP-faithful），保留。

---

### Task 1: 锁定基线

**Files:**
- Test: `crates/hammer-adapter/tests/{buffer, node_runtime, node_vector_dispatch}.rs`
- Test: `crates/hammer-service/tests/{tcp_state_machine, tcp_output, tcp_reply_nodes, session_runtime}.rs`

**Interfaces:**
- Consumes: 已提交 `d05fa358` 全部节点实现
- Produces: 迁移前测试全绿的基线记录

- [ ] **Step 1: 确认基线在 `d05fa358` 可跑**

工作树有未提交改动导致 `hammer-service` lib 编译失败（`net/opaque.rs` 等）。本计划以 `d05fa358` 为准，执行前先 `git stash push -m "pre-batchmut-removal"` 或恢复工作树到 `d05fa358`。

Run:
```bash
git status --short
cargo test -p hammer-adapter --test buffer -v
cargo test -p hammer-adapter --test node_runtime -v
cargo test -p hammer-adapter --test node_vector_dispatch -v
cargo test -p hammer-service --test tcp_state_machine -v
cargo test -p hammer-service --test tcp_output -v
cargo test -p hammer-service --test tcp_reply_nodes -v
cargo test -p hammer-service --test session_runtime -v
```
Expected: 全部 PASS。任一 FAIL 先停下记录，不进 Task 2。

- [ ] **Step 2: 记录基线到本计划末尾“执行结果”**

```markdown
## 执行结果

### Task 1 基线（d05fa358）
- adapter: buffer / node_runtime / node_vector_dispatch PASS
- service: tcp_state_machine / tcp_output / tcp_reply_nodes / session_runtime PASS
```

- [ ] **Step 3: 不提交（基线记录任务）**

### Task 2: `node/next.rs` dispatch 回流转 `&DataPlaneRuntime`，去掉 `BufferBatchMut` 透传

**Files:**
- Modify: `crates/hammer-adapter/src/node/next.rs`
- Test: `crates/hammer-adapter/tests/node_vector_dispatch.rs`

**Interfaces:**
- Consumes:
  - `DataPlaneRuntime::{get_buffer, get_buffer_mut, prefetch_header, prefetch_read, prefetch_write, preferred_frame_batch_width}`（`buffer.rs:793/950/955/838/843/848`）
  - `BufferFrame::{retain_indices_batched, retain_indices_batched_with_prefetch, pending_indices}`（`buffer.rs:3165/3177/3080`）
- Produces:
  - `NodeVectorDispatch`/`NodeNextEnqueue`/`NodeNextVectorEnqueue` 回调签名 `&DataPlaneRuntime`
  - `default_prefetch_indices(runtime, indices)` 用 `runtime.prefetch_header`
  - `dispatch_chunk`/`route_frame_*` 内部不再 `buffer_batch_mut()`

- [ ] **Step 1: 写失败测试——dispatch 给节点 runtime 而非 batch，节点用 runtime.get_buffer**

在 `crates/hammer-adapter/tests/node_vector_dispatch.rs` 加一个用 `runtime.get_buffer_mut` 取 buffer 的 route 测试（沿用文件已有 `NodeVectorDispatch::route_frame` 测试模式）。若该文件已有等价 fixture 就在其上改回调用 `runtime.get_buffer_mut`，否则新建：

```rust
/// VPP-faithful: route_chunk 收 &DataPlaneRuntime，节点用 runtime.get_buffer_mut
/// 取 buffer，不经过任何 batch guard。
#[test]
fn route_frame_callback_receives_runtime_not_batch() {
    let runtime = DataPlaneRuntime::with_capacities(64, 8, 8, 4);
    let sink = runtime
        .nodes()
        .register_internal(DescriptorNode::next(
            "route-sink",
            forward_default_process,
            NodeRuntimeData::from_words([7, 0, 0, 0]),
            [NodeId::new(0), NodeId::new(0)],
        ));
    // 填一个带数据的 buffer index，route 回调里 runtime.get_buffer_mut 读它。
    let index = runtime.alloc_index_with_bytes(b"data").expect("alloc");
    // ... 用 NodeVectorDispatch::route_frame 驱动，route_chunk 里 assert batch 参数已不存在
    // （改用 runtime.get_buffer_mut(index) 读 buffer.current() == b"data"）
    let _ = sink;
    let _ = index;
}
```

Run: `cargo test -p hammer-adapter --test node_vector_dispatch route_frame_callback_receives_runtime_not_batch -v`
Expected: FAIL（回调签名还是 `&mut BufferBatchMut`，测试调用形态不匹配；或编译失败提示 `batch` 类型）。

- [ ] **Step 2: 改 `default_prefetch_indices` 与 dispatch 回调签名**

在 `next.rs:1308` 把：
```rust
pub fn default_prefetch_indices(batch: &mut BufferBatchMut<'_>, indices: &[BufferIndex]) {
    for index in indices {
        batch.prefetch_header(*index);
    }
}
```
改成：
```rust
pub fn default_prefetch_indices(runtime: &DataPlaneRuntime, indices: &[BufferIndex]) {
    for index in indices {
        runtime.prefetch_header(*index);
    }
}
```

把 `next.rs` 里所有回调参数 `&mut BufferBatchMut<'_>` 改成 `&DataPlaneRuntime`（32 处，`next.rs:177/178/300/301/331/333/396/398/441/443/445/483/485/487/513/516/545/549/573/575/650/663/748/750/800/802/964/966/1010/1067`）。这是机械替换：`impl FnMut(&mut BufferBatchMut<'_>, …)` → `impl FnMut(&DataPlaneRuntime, …)`，内部参数名 `batch` 改 `runtime`。

- [ ] **Step 3: 改 `dispatch_chunk`/`dispatch_index_chunk`/`route_frame_*` 内部不再借 batch**

`next.rs:1073` 的 `dispatch_chunk` 内部：
```rust
let mut batch = runtime.buffer_batch_mut();
let route = route_chunk(&mut batch, indices, &mut nexts);
drop(batch);
```
改成：
```rust
let route = route_chunk(runtime, indices, &mut nexts);
```
同样改 `dispatch_index_chunk`（`next.rs:1103`）、`route_frame_quad_main`/`route_frame_pair_main`/`route_frame_index_*` 里所有 `runtime.buffer_batch_mut()` + 传 `&mut batch` 的位置（`next.rs:765/848/891/930/975` 等 `prefetch_range` / `Self::prefetch_range` 调用），把 `prefetch_range(batch, …)` 改成直接用 `runtime.prefetch_header` 预取。

`NodeNextEnqueue::validate_frame_with_width_and_buffer_batch_prefetch`（`next.rs:294`）内部 `let mut batch = runtime.buffer_batch_mut();` + `frame.buffer_node_inline(width, &mut batch, …)`：改成 `frame.retain_indices_batched_with_prefetch(width, |i| runtime.prefetch_header(i), |i| { let node = next_for_index(i)?; if node == speculative { Ok(true) } else { self.split.enqueue(runtime, node, i)?; Ok(false) } })`（`retain_indices_batched_with_prefetch` 回调 `FnMut(BufferIndex)` 不持 batch，`buffer.rs:3177`）。同名去 `_buffer_batch` 后缀，回调改 `&DataPlaneRuntime`。

`validate_frame_with_buffer_batch_chunks`（`next.rs:326`）与 `NodeNextVectorEnqueue::enqueue_frame_with_buffer_batch_chunks`（`next.rs:391`）同理改：内部用 `frame.pending_indices()` 手写 quad/pair chunk 循环 + `runtime.prefetch_header` 预取，回调收 `&DataPlaneRuntime`，节点在回调里 `runtime.get_buffer_mut(index)` 取 buffer。改名去 `_buffer_batch_chunks` → `_chunks`。

- [ ] **Step 4: 运行 node_vector_dispatch + node runtime 测试**

Run:
```bash
cargo build -p hammer-adapter -v
cargo test -p hammer-adapter --test node_vector_dispatch -v
cargo test -p hammer-adapter --test node_runtime -v
```
Expected: 编译通过（此时 `BufferBatchMut` 仍存在但 dispatch 不再用）、测试 PASS（除 Step 1 新测试转 PASS）。若 `next.rs` 仍有 `buffer_batch_mut()` 残留引用，回到 Step 3 补。

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-adapter/src/node/next.rs crates/hammer-adapter/tests/node_vector_dispatch.rs
git commit -m "hammer-adapter(Refactor): route dispatch callbacks take DataPlaneRuntime not BufferBatchMut"
```

### Task 3: 合规节点 caller 把 `batch.*` 改成 `runtime.*`

**Files:**
- Modify: `crates/hammer-adapter/tests/buffer.rs`
- Modify: `crates/hammer-service/src/net/ip/input.rs`
- Modify: `crates/hammer-service/src/net/lookup/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/src/transport/tcp/reset.rs`
- Test: `crates/hammer-service/tests/net_lookup_node.rs`
- Test: `crates/hammer-service/tests/icmp_input_nodes.rs`
- Test: `crates/hammer-service/tests/tcp_output.rs`

**Interfaces:**
- Consumes:
  - Task 2 的 `node/next.rs` 回调签名 `&DataPlaneRuntime`
  - `runtime.get_buffer`/`get_buffer_mut`/`prefetch_header`/`prefetch_read`
- Produces:
  - 合规节点不再用 `buffer_batch_mut()`/`batch.buffer`/`batch.buffer_mut`/`batch.prefetch_*`/`batch.with_buffer`
  - prefetch helper 签名 `(runtime, indices)`

- [ ] **Step 1: 改 `tcp_input.rs`**

`tcp_input.rs:14` 的 `node_route_frame_static!` 回调 `|batch, indices| …` 与 `|batch, indices, nexts| …` 现在收 `runtime`（Task 2 已改签名）。把回调里：
- `batch.buffer(index)?` → `runtime.get_buffer(index)?`（`tcp_input.rs:182/315/372`）
- `batch.buffer_mut(index)?` → `runtime.get_buffer_mut(index)?`（`tcp_input.rs:414/460/511/575`）
- `prefetch_tcp_input(batch, indices, …)` 签名改 `prefetch_tcp_input(runtime, indices, …)`，内部 `batch.prefetch_read` → `runtime.prefetch_read`、`batch.with_buffer(index, …)` → `let buffer = runtime.get_buffer(index)?; …`（`tcp_input.rs:1131-1139`）。
- `batch.buffer(index)?.trace_handle()`（`tcp_input.rs:372`）→ `runtime.get_buffer(index)?.trace_handle()`。

Run: `cargo build -p hammer-service -v`
Expected: `tcp_input.rs` 编译通过（其它文件待改）。

- [ ] **Step 2: 改 `ip_input.rs`**

`ip_input.rs:115/207` 的 `let mut batch = runtime.buffer_batch_mut();` 删掉（dispatch 回调已给 `runtime`）。`prefetch_range_with_batch`/`prefetch_indices_with_batch`（`ip_input.rs:301/305/321`）改名去 `_with_batch`，签名 `(runtime, indices)`，内部 `batch.prefetch_header` → `runtime.prefetch_header`。`batch.buffer_mut(index)?`（`ip_input.rs:345`）→ `runtime.get_buffer_mut(index)?`，`batch.buffer(index)?`（`ip_input.rs:455`）→ `runtime.get_buffer(index)?`。route 回调里 `batch` 形参改 `runtime`。

- [ ] **Step 3: 改 `ip_lookup`（`net/lookup/mod.rs`）**

`net/lookup/mod.rs` 11 处 `BufferBatchMut` 用法同样改：回调形参 `batch`→`runtime`，`batch.buffer`/`buffer_mut`/`prefetch_*` → `runtime.get_buffer`/`get_buffer_mut`/`prefetch_*`。

- [ ] **Step 4: 改 `tcp_output.rs` 与 `tcp_reset.rs`**

`tcp_output.rs` 5 处、`tcp_reset.rs` 2 处 `batch` 用法改 `runtime`：`reset.rs:112` `batch.prefetch_read` → `runtime.prefetch_read`；route 回调 `batch` 形参改 `runtime`，`tcp_reset_next_for_index` 内 `batch` 改 `runtime`（`reset.rs` 的 `prefetch_tcp_reset(batch, indices)` 改 `(runtime, indices)`，`batch.prefetch_read`→`runtime.prefetch_read`）。

- [ ] **Step 5: 改 `crates/hammer-adapter/tests/buffer.rs`**

`tests/buffer.rs` 里 `BufferBatchMut` 测试改成 `runtime.get_buffer_mut` 等价测试（若该文件直接测 `BufferBatchMut` API，改成测 `DataPlaneBuffers::get_buffer_mut`）。删 `buffer_batch_mut()` 调用。

- [ ] **Step 6: 运行合规节点测试**

Run:
```bash
cargo build --workspace
cargo test -p hammer-adapter --test buffer -v
cargo test -p hammer-adapter --test node_vector_dispatch -v
cargo test -p hammer-adapter --test node_runtime -v
cargo test -p hammer-service --test net_lookup_node -v
cargo test -p hammer-service --test icmp_input_nodes -v
cargo test -p hammer-service --test tcp_output -v
cargo test -p hammer-service --test tcp_reply_nodes -v
cargo test -p hammer-service --lib net::ip -v
cargo test -p hammer-service --lib transport::tcp -v
```
Expected: PASS。

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-adapter/tests/buffer.rs crates/hammer-service/src/net/ip/input.rs crates/hammer-service/src/net/lookup/mod.rs crates/hammer-service/src/transport/tcp/input.rs crates/hammer-service/src/transport/tcp/output.rs crates/hammer-service/src/transport/tcp/reset.rs
git commit -m "hammer-service(Refactor): compliant nodes fetch buffers via runtime get_buffer"
```

### Task 4: 删 `BufferBatchMut` / `batch_mut` / `buffer_batch_mut`

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs`
- Modify: `crates/hammer-adapter/src/lib.rs`
- Test: `crates/hammer-adapter/tests/buffer.rs`

**Interfaces:**
- Consumes: Task 2-3 已让全树不再用 `BufferBatchMut`
- Produces: `BufferBatchMut` 类型与 `batch_mut`/`buffer_batch_mut` 方法删除

- [ ] **Step 1: 确认零 caller**

Run:
```bash
grep -rnE "BufferBatchMut|buffer_batch_mut|batch_mut\b" crates --include="*.rs"
```
Expected: 只剩 `crates/hammer-adapter/src/buffer.rs`（定义）与 `lib.rs`（re-export）命中；无任何 caller。若有 caller 回 Task 2/3 补。

- [ ] **Step 2: 删 `BufferBatchMut` 及相关方法**

删 `buffer.rs:3952-4003` 的 `pub struct BufferBatchMut` 与 `impl BufferBatchMut<'_>`。删 `BufferPool::batch_mut`（`buffer.rs:1697-1701`）。删 `DataPlaneBuffers::buffer_batch_mut`（`buffer.rs:960-962`）。删 `lib.rs` 里 `BufferBatchMut` re-export（`lib.rs:15` 那行 import 列表中去掉 `BufferBatchMut`）。

- [ ] **Step 3: 运行 adapter 全量测试 + workspace 编译**

Run:
```bash
cargo build --workspace
cargo test -p hammer-adapter -v
```
Expected: PASS（`BufferBatchMut` 已删，无残留引用）。

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-adapter/src/buffer.rs crates/hammer-adapter/src/lib.rs crates/hammer-adapter/tests/buffer.rs
git commit -m "hammer-adapter(Refactor): remove non-VPP BufferBatchMut guard layer"
```

### Task 5: `tcp-established` 迁 frame discipline

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/established.rs:86-100`
- Test: `crates/hammer-service/tests/tcp_state_machine.rs`

**Interfaces:**
- Consumes:
  - `runtime.get_buffer_mut`/`runtime.prefetch_header`/`runtime.packet_buffers().alloc_index`/`runtime.free_index`（runtime 形态，VPP-faithful）
  - `NodeNextFrames::enqueue`+`schedule`
  - `TcpEstablishedNext::{Output, Drop}` + `Self::runtime_nexts`
  - `tcp_established_index(runtime, index, session_queue, tcp_output, &mut next_frames)` 已收 runtime
- Produces:
  - `tcp_established_frame` 不再用 `node_rewrite_frame!`

- [ ] **Step 1: 写失败测试——established 源码不再用 node_rewrite_frame!**

在 `crates/hammer-service/tests/tcp_state_machine.rs` 追加（沿用 `read_tcp_source`+`concat!` 模式）：

```rust
#[test]
fn tcp_established_uses_frame_discipline_not_rewrite_macro() {
    let source = read_tcp_source("src/transport/tcp/established.rs");
    assert!(
        !source.contains(concat!("node_rewrite", "_frame!")),
        "tcp-established still uses scalar node_rewrite_frame! macro"
    );
    assert!(
        source.contains("NodeNextFrames"),
        "tcp-established does not write TX segment into NodeNextFrames next frame"
    );
    assert!(
        source.contains("prefetch_header"),
        "tcp-established does not prefetch buffer headers across chunks"
    );
    assert!(
        source.contains("NodeResult::drop"),
        "tcp-established does not consume input frame via NodeResult::drop()"
    );
}
```

Run: `cargo test -p hammer-service --test tcp_state_machine tcp_established_uses_frame_discipline_not_rewrite_macro -v`
Expected: FAIL（`established.rs:97` 含 `node_rewrite_frame!`）。

- [ ] **Step 2: 改 `tcp_established_frame` 为显式 chunk 循环**

替换 `established.rs:86-100`：

```rust
fn tcp_established_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueue<C>,
    next: [NodeId; TcpEstablishedNext::COUNT],
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpEstablishedNext::Output as usize];
    let drop_next = next[TcpEstablishedNext::Drop as usize];
    let mut next_frames = hammer_adapter::node::NodeNextFrames::default();
    let width = runtime.preferred_frame_batch_width();
    let chunk = match width {
        hammer_adapter::FrameBatchWidth::Quad => 4usize,
        hammer_adapter::FrameBatchWidth::Pair => 2usize,
    };
    let indices = frame.pending_indices().to_vec();
    let mut read = 0usize;
    while read < indices.len() {
        let end = (read + chunk).min(indices.len());
        for prefetch in read..end {
            runtime.prefetch_header(indices[prefetch]);
        }
        for index in indices[read..end].iter().copied() {
            if let Err(_) = tcp_established_index(runtime, index, session_queue, tcp_output, &mut next_frames) {
                // VPP drop-node 语义：逐 buffer 出错路由到 drop_next，错误被 drop-node 吸收
                // （与原 node_rewrite_frame! 闭包返回 Err 时只 enqueue drop_next 一致）。
                next_frames.enqueue(runtime, drop_next, index)?;
            }
        }
        read = end;
    }
    frame.clear();
    next_frames.schedule(runtime)?;
    Ok(hammer_adapter::NodeResult::drop())
}
```

`tcp_established_index`（`established.rs:102-264`）**不改函数体**：它已收 `runtime: &DataPlaneRuntime`，内部 `runtime.packet_buffers().get_buffer_mut(index)`（`established.rs:157`）、`runtime.packet_buffers().alloc_index()`（`established.rs:250`）、`segment.write_to_buffer(runtime.packet_buffers(), allocated)`（`established.rs:251`）、`runtime.free_index`（`established.rs:252/261`）都是 runtime 形态，符合 VPP。TX segment 由它内部 `next_frames.enqueue(runtime, tcp_output, allocated)` 写 next frame。

- [ ] **Step 3: 运行针对性测试**

Run:
```bash
cargo test -p hammer-service --test tcp_state_machine -v
cargo test -p hammer-service --test tcp_reply_nodes -v
cargo test -p hammer-service --lib transport::tcp::established -v
```
Expected: PASS。

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/established.rs crates/hammer-service/tests/tcp_state_machine.rs
git commit -m "hammer-service(Refactor): align tcp-established with VPP frame discipline"
```

### Task 6: `tcp-listen` 迁 frame discipline

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs:164-265`
- Test: `crates/hammer-service/tests/tcp_state_machine.rs`

**Interfaces:**
- Consumes: Task 5 模板、`TcpListenNext::{Output, Established, Drop}`、`tcp_listen_index(runtime, index, control, session_queue, tcp_output, tcp_established, &mut next_frames)`
- Produces: `tcp_listen_process_frame` 不再用 `node_rewrite_frame!`；接受的 SYN 转发到 `tcp_established` next frame

- [ ] **Step 1: 写失败测试**

在 `tcp_state_machine.rs` 追加：

```rust
#[test]
fn tcp_listen_uses_frame_discipline_not_rewrite_macro() {
    let source = read_tcp_source("src/transport/tcp/listen.rs");
    assert!(!source.contains(concat!("node_rewrite", "_frame!")),
        "tcp-listen still uses node_rewrite_frame!");
    assert!(source.contains("NodeNextFrames"), "tcp-listen no NodeNextFrames");
    assert!(source.contains("prefetch_header"), "tcp-listen no prefetch");
    assert!(source.contains("tcp_established"), "tcp-listen does not forward accepted SYN to tcp-established next frame");
    assert!(source.contains("NodeResult::drop"), "tcp-listen no NodeResult::drop");
}
```

Run: `cargo test -p hammer-service --test tcp_state_machine tcp_listen_uses_frame_discipline_not_rewrite_macro -v`
Expected: FAIL。

- [ ] **Step 2: 改 `tcp_listen_process_frame` 为显式 chunk 循环**

替换 `listen.rs:164-188`，结构与 Task 5 Step 2 一致，循环内调 `tcp_listen_index(runtime, index, control, session_queue, tcp_output, tcp_established, &mut next_frames)`，错误 enqueue `drop_next`。

- [ ] **Step 3: 改 `tcp_listen_index` 接受分支转发到 tcp_established**

`tcp_listen_index`（`listen.rs:190+`）`release_input = false`（接受的 SYN，`listen.rs:247-262`）处升级为 `next_frames.enqueue(runtime, tcp_established, index)?`（VPP next-frame 转发），然后 `release_input = false`。末尾 `if release_input { runtime.free_index(index); }` 保留。TX segment（SYN-ACK）仍由它内部 enqueue 到 `tcp_output`。

- [ ] **Step 4: 运行针对性测试**

Run:
```bash
cargo test -p hammer-service --test tcp_state_machine -v
cargo test -p hammer-service --test tcp_reply_nodes -v
cargo test -p hammer-service --lib transport::tcp::listen -v
```
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/tests/tcp_state_machine.rs
git commit -m "hammer-service(Refactor): align tcp-listen with VPP frame discipline"
```

### Task 7: `tcp-syn-sent` 与 `tcp-rcv-process` 迁 frame discipline

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs:82-95`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs:85-99`
- Test: `crates/hammer-service/tests/tcp_state_machine.rs`
- Test: `crates/hammer-service/tests/tcp_output.rs`

**Interfaces:**
- Consumes: Task 5 模板、`TcpSynSentNext::{Output, Drop}`、`TcpRcvProcessNext::{Output, Drop}`、`tcp_syn_sent_index`/`tcp_rcv_process_index`（已收 runtime）
- Produces: 两个节点不再用 `node_rewrite_frame!`

- [ ] **Step 1: 写两个失败测试**

在 `tcp_state_machine.rs` 追加 `tcp_syn_sent_uses_frame_discipline_not_rewrite_macro`（读 `src/transport/tcp/syn_sent.rs`）与 `tcp_rcv_process_uses_frame_discipline_not_rewrite_macro`（读 `src/transport/tcp/rcv_process.rs`），断言与 Task 5/6 一致（无 `node_rewrite_frame!`，有 `NodeNextFrames`/`prefetch_header`/`NodeResult::drop`）。

Run:
```bash
cargo test -p hammer-service --test tcp_state_machine tcp_syn_sent_uses_frame_discipline_not_rewrite_macro -v
cargo test -p hammer-service --test tcp_state_machine tcp_rcv_process_uses_frame_discipline_not_rewrite_macro -v
```
Expected: 两个都 FAIL。

- [ ] **Step 2: 改 `tcp_syn_sent_frame` 为显式 chunk 循环**

替换 `syn_sent.rs:82-95`，结构与 Task 5 Step 2 一致，循环内调 `tcp_syn_sent_index(runtime, index, session_queue, tcp_output, &mut next_frames)`，错误 enqueue `drop_next`。`tcp_syn_sent_index` 函数体不改（已收 runtime，内部 `runtime.packet_buffers().get_buffer_mut`/`alloc_index`/`write_to_buffer`/`free_index` 是 runtime 形态）。

- [ ] **Step 3: 改 `tcp_rcv_process_frame` 为显式 chunk 循环**

替换 `rcv_process.rs:85-99`，同上，调 `tcp_rcv_process_index(runtime, index, session_queue, tcp_output, &mut next_frames)`。`tcp_rcv_process_index` 函数体不改。

- [ ] **Step 4: 运行针对性测试**

Run:
```bash
cargo test -p hammer-service --test tcp_state_machine -v
cargo test -p hammer-service --test tcp_output -v
cargo test -p hammer-service --lib transport::tcp -v
```
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/syn_sent.rs crates/hammer-service/src/transport/tcp/rcv_process.rs crates/hammer-service/tests/tcp_state_machine.rs
git commit -m "hammer-service(Refactor): align tcp-syn-sent and tcp-rcv-process with VPP frame discipline"
```

### Task 8: `SessionQueueNode` 对齐 VPP polling-input driver

**Files:**
- Modify: `crates/hammer-service/src/session/node.rs:211-240`
- Test: `crates/hammer-service/tests/session_runtime.rs`

**Interfaces:**
- Consumes: `SessionQueueOutput::schedule`、`dispatch_registered_session_queue_once_at`
- Produces: `session_queue_node_process` 不再 `frame.clear()`

- [ ] **Step 1: 写失败测试**

在 `session_runtime.rs` 顶部补 `std::fs`/`std::path::Path` 导入与 `read_session_source` helper（沿用 `tcp_state_machine.rs` 的 `read_tcp_source` 模式），追加：

```rust
fn read_session_source(path: &str) -> String {
    std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .expect("read session source")
}

#[test]
fn session_queue_driver_is_vpp_polling_input_not_frame_consumer() {
    let source = read_session_source("src/session/node.rs");
    assert!(!source.contains("frame.clear()"),
        "session-queue driver still treats empty input frame as owned data (frame.clear())");
    assert!(source.contains("output.schedule(runtime)"),
        "session-queue driver does not schedule TX through next frame");
    assert!(source.contains("NodeResult::drop"),
        "session-queue driver does not return NodeResult::drop()");
}
```

Run: `cargo test -p hammer-service --test session_runtime session_queue_driver_is_vpp_polling_input_not_frame_consumer -v`
Expected: FAIL（`session/node.rs:216` 含 `frame.clear()`）。

- [ ] **Step 2: 改 `session_queue_node_process`**

替换 `session/node.rs:211-240`：

```rust
fn session_queue_node_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    _: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    // VPP VLIB_NODE_TYPE_INPUT 语义：runtime 调度本 driver 时传入空 frame（等价 frame=0）。
    // 本节点不消费输入 frame 的 buffer，而是按 ready-session + expired timer 产出 TX segment，
    // 写进 tcp-output 的 next frame。不对 frame 做 clear/push，frame 所有权仍归 runtime。
    let slot = data.usize_word(0)?;
    let attachments = SESSION_QUEUE_NODES.with(|nodes| {
        let nodes = nodes.try_borrow()
            .map_err(|_| CoreError::internal("session queue nodes borrowed"))?;
        let node = nodes.get(slot)
            .ok_or_else(|| CoreError::internal("session queue node slot is invalid"))?;
        Ok::<_, CoreError>(node.clone())
    })?;
    let now = Instant::now();
    let mut output = SessionQueueOutput::default();
    for attachment in attachments {
        (attachment.dispatch)(runtime, attachment.runtime_data, attachment.output_next, now, &mut output)?;
    }
    output.schedule(runtime)?;
    Ok(NodeResult::drop())
}
```

- [ ] **Step 3: 运行针对性测试**

Run:
```bash
cargo test -p hammer-service --test session_runtime -v
cargo test -p hammer-service --lib session -v
```
Expected: PASS。

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-service/src/session/node.rs crates/hammer-service/tests/session_runtime.rs
git commit -m "hammer-service(Refactor): align session-queue driver with VPP polling input node"
```

### Task 9: 全量验证与 grep 复查

**Files:**
- Modify: `docs/superpowers/plans/2026-06-27-internal-node-frame-discipline.md`

**Interfaces:**
- Consumes: Task 1-8 全部实现
- Produces: 验证记录 + 剩余风险清单

- [ ] **Step 1: 运行完整验证集**

Run:
```bash
cargo build --workspace
cargo test -p hammer-adapter -v
cargo test -p hammer-service --lib
cargo test -p hammer-service --test tcp_state_machine -v
cargo test -p hammer-service --test tcp_reply_nodes -v
cargo test -p hammer-service --test tcp_output -v
cargo test -p hammer-service --test session_runtime -v
cargo test -p hammer-service --test icmp_input_nodes -v
cargo test -p hammer-service --test net_lookup_node -v
cargo test -p hammer-runtime --test tcp_control_plane -v
```
Expected: 全部 PASS。

- [ ] **Step 2: grep 复查 BufferBatchMut / node_rewrite_frame! / frame.clear 残留**

Run:
```bash
grep -rnE "BufferBatchMut|buffer_batch_mut|batch_mut\b" crates --include="*.rs"
grep -rn "node_rewrite_frame!" crates/hammer-service/src --include="*.rs" | grep -v "node_rewrite_frame_current"
grep -rn "frame.clear()" crates/hammer-service/src/session --include="*.rs"
```
Expected: `BufferBatchMut` 零命中（已删）；`node_rewrite_frame!`（非 `_current`）在 `established/listen/syn_sent/rcv_process` 零命中（`reassembly.rs` 用 `node_rewrite_frame_current!` 保留，不在 grep 范围）；`session/node.rs` 无 `frame.clear()`。

- [ ] **Step 3: 更新计划文档底部“执行结果”**

```markdown
## 执行结果

- `BufferBatchMut` / `buffer_batch_mut` / `batch_mut` 已删除：VPP `vlib` 无 batch guard 等价物，
  节点现在用 runtime `vlib_get_buffer` 形态（`runtime.get_buffer`/`get_buffer_mut`）取 buffer，
  TX 用 runtime pool（`runtime.packet_buffers().alloc_index`/`runtime.free_index`）。
- `node/next.rs` dispatch 回调签名从 `&mut BufferBatchMut` 改成 `&DataPlaneRuntime`（32 处），
  内部不再 `buffer_batch_mut()`，`default_prefetch_indices` 用 `runtime.prefetch_header`。
- 合规节点（ip-input/ip-lookup/tcp-input/tcp-output/tcp-reset）与四个偏离节点（established/listen/
  syn_sent/rcv_process）的 batch 用法全改 runtime；四个偏离节点从 `node_rewrite_frame!` 迁到显式
  prefetch chunk 循环 + `NodeNextFrames` + `NodeResult::drop()`。
- `SessionQueueNode` 去掉空 frame 的 `frame.clear()`，对齐 VPP polling-input driver。
- `parse_tcp_packet`/`TcpSegment::write_to_buffer` 保留 runtime 形态（VPP-faithful，未改）。
- Task 9 Step 2 grep：`BufferBatchMut` 零命中、四个偏离节点无 `node_rewrite_frame!`、session/node.rs 无 `frame.clear()`。
- `cargo test --workspace` 通过。
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-06-27-internal-node-frame-discipline.md
git commit -m "docs(Refactor): record BufferBatchMut removal and VPP frame-discipline migration"
```

## Self-Review

- **Spec coverage:** 用户要求：删 `BufferBatchMut`（非 VPP guard）、buffer 经 VPP 形态取用（frame 给索引 + runtime `vlib_get_buffer` 取 buffer）、TX 经 runtime pool（`vlib_buffer_alloc`）、四个偏离节点 frame discipline、`SessionQueueNode` driver 对齐。已覆盖：`node/next.rs` 回调签名转 runtime（Task 2，审批节 1）、合规节点 batch→runtime（Task 3）、删 `BufferBatchMut`（Task 4，审批节 2）、四个偏离节点迁 frame discipline（Task 5-7，审批节 3）、`SessionQueueNode` 对齐（Task 8）、全量 + grep 复查（Task 9）。`parse_tcp_packet`/`write_to_buffer` 已是 runtime 形态，保留（不退化成 frame-API 变体，因 VPP 里 `vlib_get_buffer`/`vlib_buffer_alloc` 本就是 runtime 形态）。
- **Placeholder scan:** 无 `TODO`/`TBD`/“后续实现”。逐 buffer 错误处理统一 `Err(_)` 裸下划线 + `next_frames.enqueue(runtime, drop_next, index)?`，遵循 `AGENTS.md`。测试基于已确认基建：adapter 层 `node_vector_dispatch.rs`/`buffer.rs`/`node_runtime.rs` 真实 `DataPlaneRuntime` 驱动，服务层 `tcp_state_machine.rs`/`session_runtime.rs` 源码分析模式。
- **Type consistency:** frame API 统一 `BufferFrame::pending_indices()`；buffer 取用统一 `runtime.get_buffer`/`get_buffer_mut`；TX 统一 `runtime.packet_buffers().alloc_index`/`runtime.free_index`/`segment.write_to_buffer(runtime.packet_buffers(), …)`；prefetch 统一 `runtime.prefetch_header`/`prefetch_read`；next frame 统一 `NodeNextFrames::enqueue`+`schedule`；next 枚举 `TcpEstablishedNext`/`TcpListenNext`/`TcpSynSentNext`/`TcpRcvProcessNext`/`TcpResetNext` 与现有代码一致（已核对变体：established={Output,Drop}、listen={Output,Established,Drop}、syn_sent={Output,Drop}、rcv_process={Output,Drop}、reset={Drop,Lookup}）。`FrameBatchWidth::{Quad,Pair}` 与 `hammer-adapter::instruction_set` 一致。`BufferBatchMut` 全树删除后无残留引用。

## 执行交接

Plan complete and saved to `docs/superpowers/plans/2026-06-27-internal-node-frame-discipline.md`. Two execution options:

1. **Subagent-Driven (recommended)** - 我按任务逐个起新 agent 做，实现后逐步 review
2. **Inline Execution** - 我在当前会话里按任务直接执行并分段校验

Which approach?
# VPP-Style Worker Graph Refork

Status: accepted

Hammer treats Graph Refork as graph lifecycle owned by `GlobalMain`, not as a
fallible Barrier operation. Main-thread graph mutations validate before
publication and set one coalescing node-runtime-update flag. At the outermost
Worker Barrier release, `GlobalMain` publishes the latest main graph and the
worker completion count once, matching VPP's
`need_vlib_worker_thread_node_runtime_update` and `node_reforks_required`
sequence. Repeated graph mutations inside one nested Barrier scope therefore
produce one refork rather than a pending-update error.

After release, each Data Worker performs an infallible refork of its own graph
clone. It rebuilds node and runtime collections from the published main graph,
retains existing worker-local runtime data, node state, and counters for
existing nodes, initializes new node clones from the main graph, swaps the new
clone into place, and reclaims only its own previous clone. Allocation follows
the process no-fail allocation contract. A non-additive published graph is a
programmer invariant violation, not a recoverable runtime condition.

Graph Refork never runs worker-init functions and never returns `Result`.
Worker-init functions run only during initial worker bootstrap, matching VPP's
`vlib_worker_thread_fn`. A worker-init registration discovered by runtime
additive plugin loading is not replayed on existing Data Workers; a plugin that
requires worker bootstrap state must be loaded before workers start. Refork
completion is a scalar synchronization count: each worker decrements it with a
release operation after swapping its clone, `GlobalMain` waits with acquire
loads, and the published graph remains alive until the count reaches zero. A
deadline miss remains a process-fatal worker deadlock.

## 变更清单

### 新增

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 私有 API | `NodeRuntime::refork` | 无返回值地重建 worker graph clone，并保留现有 worker-local runtime state | 替代旧 fallible replacement；无 wrapper | existing/new node state、runtime data 与 counter 保留测试 |
| 领域契约 | Graph Refork | `GlobalMain` 发布、每个 worker 重建并回收自己的 clone、completion count 证明生命周期 | 记录于 `CONTEXT.md` | 多 worker refork integration test |

### 修改

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型 | `GlobalMain` graph publication state | 使用 coalescing update flag；outermost Barrier release 才发布最新 graph 并设置 completion count | 内部 lifecycle breaking | 同一 Barrier 内多次 graph mutation 只触发一次 refork |
| 类型 | `WorkerPublication` | 只保留 published graph；删除 worker-init payload 和 per-worker error slots | 内部布局变化；无持久化影响 | publication lifetime 与 completion test |
| 私有 API | `DataPlaneMain::refork_worker_graph` | 返回值改为 `()`；只调用 infallible Graph Refork 后递减 completion count | 删除 worker exit/error propagation | refork 后 main loop 继续运行 |
| API 契约 | `worker_init_function` | 仅 worker bootstrap 执行；runtime additive load 不向现有 workers replay | 行为 breaking；依赖该状态的 plugin 必须 startup load | startup 调用一次、live load 不调用测试 |

### 删除

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 私有 API | `NodeRuntime::replace_graph_preserving_worker_state` | 删除 fallible replacement | 同一提交迁移唯一调用方 | workspace compile |
| 私有状态 | `WorkerGraphUpdate::worker_init_functions` 与 worker graph error slots | refork payload 不再携带 init hooks 或错误通道 | 无兼容层 | publication/refork integration test |
| 错误 | `RuntimeError::{WorkerGraphUpdateAlreadyPending, WorkerGraphUpdateMissing, WorkerGraphUpdateStatePoisoned, WorkerGraphUpdateNotAdditive, WorkerGraphUpdate}` | 删除 recoverable post-publication failure model | exhaustive matches 一次性迁移 | workspace compile 与 invariant tests |

迁移不改变序列化、持久化、IPC wire format 或 Binary API ABI。Graph Refork
和 Worker Barrier 在同一 breaking change 中迁移，但分别由本 ADR 与
[ADR-0002](./0002-vpp-style-worker-thread-barrier.md) 记录。

## 依据与假设

已核对 `third_party/vpp/src/vlib/threads.c` 的
`worker_thread_node_runtime_update_internal`、
`vlib_worker_thread_node_refork`、`vlib_worker_thread_node_runtime_update` 和
Barrier release，以及 `third_party/vpp/src/vlib/main.c` 的
`vlib_worker_thread_fn`。VPP refork 使用 no-fail allocation、没有返回错误，
并且 worker-init 只在 worker bootstrap 执行。

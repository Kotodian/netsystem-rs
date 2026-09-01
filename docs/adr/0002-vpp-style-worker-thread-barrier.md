# VPP-Style Worker Thread Barrier

Status: accepted

Hammer keeps Worker Barrier handshake state in private, process-wide Worker
Thread state, not in `GlobalMain` or any `DataPlaneMain`. Only one Data Worker
generation exists for the process lifetime; starting another generation
violates a programmer invariant. `GlobalMain` continues to own worker
lifecycle, graph publication, and refork completion. A main `DataPlaneMain`
supplies only the thread-zero execution context used to enter synchronization,
while worker loops only acknowledge the process-wide state. This matches VPP,
where `vlib_worker_threads` owns the handshake state, `vlib_main_t *` supplies
the caller context, and `vlib_global_main_t` retains only release callbacks and
the node-runtime-update flag.

Normal synchronization is scope-bound and always releases. Final process
shutdown is the sole exception: runtime lifecycle code synchronizes once,
never releases the barrier, runs main-loop exit functions while Data Workers
remain stopped, and then terminates the process without joining those workers.
Hammer does not provide a second restartable or graceful worker-shutdown mode.

The only supported call surface is an inline block macro:

```rust
worker_thread_barrier_sync!(main, {
    mutate_worker_visible_state()?;
    Ok(())
})?;
```

The implementation lives under the private
`hammer_runtime::sync::worker_thread_barrier` module. Its operations use the
VPP-aligned verbs `sync`, `check`, and `release`, and its process-global data is
an implementation-local `State`. The crate-root macro is the only supported
public name. Any guard symbol made public solely for cross-crate macro
expansion is `#[doc(hidden)]` and has no public constructor, release method, or
state query.

The macro does not create a closure. A private, non-`Send` RAII guard releases
the barrier when the block completes, returns early, propagates an error, or
unwinds. Nested synchronization retains VPP's recursion semantics. Supplying a
worker `DataPlaneMain` violates a programmer invariant and asserts, matching
VPP's thread-zero assertion.

The Binary API dispatcher acquires the barrier only around a successfully
resolved handler that is not marked `mp_safe`. MP-safe handlers and requests
that never reach a handler do not acquire it. Handlers neither acquire nor
inspect the barrier; nested dispatcher calls use the barrier's recursion
semantics. The outermost private release performs any deferred graph-refork
completion, so the dispatcher has no pending-state branch or manual refork
finish step.

Session and Application authorities do not acquire or inspect the barrier.
Main-thread operations that mutate shared Application, listener, or Session
structure assert that their caller already holds it. Binary API dispatch
provides that scope for control handlers, and any other control ingress owns
one outer synchronization scope for its complete transaction. Worker-owned
Session operations are delivered to the owning Data Worker, while connect work
is delivered to the transport control thread; neither path acquires the global
barrier.

An owner that can determine whether its operation will publish, expand, or
invalidate worker-visible storage acquires the barrier internally for exactly
that operation. Graph, interface, feature-arc, IP/FIB, and adjacency publication
follow this rule. No-op operations avoid synchronization, while calls made
inside an existing scope use recursion. These owners do not store an optional
barrier or accept one through builder-style injection.

The synchronization macro takes the main thread's `&mut DataPlaneMain`
explicitly, corresponding to VPP's `vlib_main_t *vm`. Owners that acquire the
barrier propagate this context as an ordinary borrow. The argument proves the
calling thread and supplies its execution context; it does not contain the
barrier. No acquisition path discovers the barrier through
`GlobalMain::with_current`.

Code that requires a caller-owned synchronization scope uses a hidden,
repository-internal, zero-argument assertion. It reads private Worker Thread
state, returning no value. Before Data Workers start, the installed main thread
satisfies the assertion without creating zero-worker Barrier state. After they
start, the assertion requires both the recorded main thread identity and a
non-zero recursion level. A Data Worker never satisfies it. The implementation
does not add thread-local state. Hammer exposes no held-state query, and neither
Binary API handler ABI nor Session/Application method signatures gain a
barrier-context parameter. The existing Result-returning
`ensure_main_thread_with_barrier` API and its error variant are removed.

Startup plugin loading, configuration, initialization, and main graph
construction complete before Worker Thread state is installed and therefore do
not synchronize. Hammer does not create a zero-worker barrier placeholder;
Barrier state exists only for the process's real Data Worker generation.

Runtime additive plugin loading also does not acquire one barrier around the
loader transaction. Dynamic library loading, configuration parsing, and
ordinary initialization proceed while workers run. Each plugin hook or owning
subsystem establishes synchronization only at the exact worker-visible
publication it performs; graph publication follows the owner-internal rule
above, while Session/Application transactions establish their scope at their
control ingress. Graph publication and the refork coordinated by the outermost
release follow [ADR-0003](./0003-vpp-style-worker-graph-refork.md).

## Synchronization Contract

The main thread publishes a sync request with a release store. Each Data Worker
observes it with an acquire load, finishes its prior graph work, and acknowledges
with a release operation; the main thread waits with acquire loads until every
worker has acknowledged. The main thread then mutates barrier-owned state.
Release publishes those writes with a release store, and workers observe them
with acquire loads before resuming.

Barrier state lasts for the process lifetime and therefore has no concurrent
reclamation path. The outermost release coordinates Graph Refork through the
owner state defined in ADR-0003; the Barrier primitive does not own graph data
or add a second publication protocol around it.

## 变更清单

### 新增

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| API | `hammer_runtime::worker_thread_barrier_sync!(main, { ... })` | 唯一受支持的同步入口；block 原地执行，RAII 覆盖正常返回、`?`、`return`、panic 和递归 | 新 API；同一提交迁移全部调用方 | 作用域退出、递归、错误传播、panic 与 worker-context invariant 测试 |
| 私有类型/API | `sync::worker_thread_barrier::{State, sync, check, release}` | 进程级 handshake state、主线程同步和 worker acknowledgement；宏所需 guard 仅 `#[doc(hidden)]` | 不构成受支持 API；不得由业务代码构造或查询 | 启动双 Barrier、acknowledgement、deadline 与单 generation 测试 |
| 私有 API | zero-argument held assertion | 无 workers 时验证已安装主线程；有 workers 时验证 main thread 与 recursion level | 仅仓库 owner 使用；Binary API ABI 不变 | startup、held、unheld、Data Worker 调用测试 |

### 修改

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型 | `GlobalMain` | 删除 Barrier 存储；继续拥有 worker lifecycle、graph publication 与 refork completion；final shutdown 永久持有 Barrier 且不 join workers | 行为 breaking；`close` 成为一次性进程终止阶段 | startup、publication 与 subprocess shutdown 测试 |
| 类型 | `DataPlaneMain` | 删除 Barrier 字段与安装参数；主实例仅作为显式 sync context，worker loop 通过私有 `check` acknowledgement | 构造/clone 内部迁移；无 public Barrier accessor | main/worker context 与 hot-loop acknowledgement 测试 |
| API | Binary API `dispatch` | 仅成功解析的非 `mp_safe` handler 进入宏；无 pending 查询或手动 refork finish | handler function-pointer ABI 与 handler 签名不变 | MP-safe、non-MP-safe、missing/invalid dispatch 测试 |
| API | Session/Application 及 TLS/QUIC/HTTP/TCP/UDP control mutation | 删除内部 acquisition，改用 hidden held assertion；owner-worker 与 transport-control routing 不变 | 公共业务签名不增加 Barrier 参数 | caller-held invariant 与 owner-worker routing 测试 |
| API | `InterfaceControlPlane::{register_interface, register_interface_with_mtu, set_mtu, set_protocol_mtu, add_address, remove_address}` | 增加显式 `&mut DataPlaneMain` sync context；只在实际 publish 时同步 | breaking，一次性迁移全部调用方 | no-op 不同步与 snapshot publication 测试 |
| API | `FeatureArcControl::{register_feature, add_start_node, remove_start_node, attach_start_at, set_default_end_node, enable_feature, enable_feature_with_config, disable_feature, set_end_node_for_interface, clear_end_node_for_interface}` | 增加显式 `&mut DataPlaneMain` sync context；owner 内部 publish | breaking，一次性迁移全部调用方 | feature publication 与 nested Barrier 测试 |
| API | `IpLookupControlPlane::publish` | 增加显式 `&mut DataPlaneMain` sync context并在 owner 内同步 | breaking，一次性迁移全部调用方 | lookup snapshot publication 测试 |
| API | `GlobalMain::load_plugins` | startup 不同步；runtime loader 不包围整段 Barrier | acquisition behavior breaking；publication 交由 owner | startup/live additive-load integration test |

### 删除

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型/API | public `WorkerBarrier` 及 `barrier` module surface | 删除 type、crate-root re-export、clone handle、`sync`、`worker_count`、`is_pending` 及 manual release surface | breaking；无 alias、deprecated wrapper 或兼容层 | workspace consumers 编译 |
| API | `GlobalMain::worker_barrier`、`DataPlaneMain::worker_barrier` | 删除 clone-return accessors | breaking；调用方迁移到 owner rule 或 block macro | workspace consumers 编译 |
| API | `ensure_main_thread_with_barrier` 与 `RuntimeError::ControlRequiresWorkerBarrier` | 删除 Result 型 held 检查和对应 recoverable error | breaking；仓库 owner 使用 hidden assertion | invariant tests 与 exhaustive error matches 编译 |
| API | `InterfaceControlPlane::with_barrier`、`FeatureArcControl::with_barrier`、`IpLookupControlPlane::with_barrier` | 删除 Barrier 注入与 `Option<WorkerBarrier>` 字段 | breaking；调用时传 main context | workspace consumers 编译 |
| 私有 API | Session/Application/plugin `with_control_barrier` helpers 与 pending branches | 删除重复 closure 模板和 `GlobalMain::with_current` Barrier lookup | 无兼容层 | control-path integration tests |
| API | `GlobalMain::finish_deferred_worker_graph_update` | refork completion 并入 outermost private release | breaking；dispatcher 不再手动调用 | nested publication/refork test |

迁移在一个 breaking change 中完成，不提供 feature flag、双轨 rollout 或弃用窗口。没有序列化、持久化、IPC wire format 或 Binary API handler ABI 迁移。除明确列出的 control-plane publish API 外，Session/Application 和协议 handler 签名保持不变。

## 依据与假设

已核对 vendored VPP 的 `vlib/threads.h`、`vlib/threads.c`、`vlib/main.c`、
`vlib/node.c`、`vlibapi/api_shared.c`、`vnet/interface/runtime.c` 以及
Session/Application 调用路径。设计假设 Hammer 与 VPP 一样，每个进程只
创建一次 Data Worker generation，并在进程终止时永久持有最终 Barrier。

Evidence: `third_party/vpp/src/vlib/threads.h` defines the handshake state and
sync/release interface; `third_party/vpp/src/vlib/threads.c` enforces the main-
thread and recursion contract; `third_party/vpp/src/vlib/main.h` keeps Worker
Barrier state out of `vlib_global_main_t`.

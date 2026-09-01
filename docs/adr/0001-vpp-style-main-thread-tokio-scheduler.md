# VPP-Style Main Thread With Tokio Scheduling

Status: accepted

Hammer adopts VPP's split between the process-global `GlobalMain` and one
per-thread `DataPlaneMain`, while using the existing `ControlThread` as the
main operating-system thread's single-thread Tokio scheduler. `GlobalMain` is
the process-wide container and lifecycle authority; `DataPlaneMain[0]` is the
main-thread execution context and `DataPlaneMain[1..]` are worker execution
contexts. This replaces the current detached `ProcessMain`/`LocalSet` path and
keeps the main scheduler on the same owner path as the main `DataPlaneMain`.

## Decision

### Ownership and execution

- `GlobalMain` corresponds to VPP `vlib_global_main_t` and contains the
  indexed per-thread `DataPlaneMain` values. Index zero is the main thread;
  subsequent indexes are Data Workers.
- `ControlThread` remains the only main-thread scheduler type. It is not a
  second main object and is not an OS thread. Its Tokio runtime and task
  driving state are used only by the main operating-system thread.
- `ProcessMain` is removed. Process task state, process restore state, main
  timer state, main RPC dispatch, and File readiness are driven from the main
  `DataPlaneMain` through `ControlThread`.
- The main scheduler executes Process Nodes, Process Restore, main timers,
  main RPC/Binary API dispatch, File readiness, and lifecycle checkpoints. It
  does not execute worker packet frames, handoff queues, or worker packet
  graph work.
- Data Worker `DataPlaneMain` values execute packet graph dispatch, frames,
  buffers, handoff, worker-local File readiness, and worker-local transport
  timers on their owning worker thread.

### Tokio and VPP loop alignment

The main scheduler follows the main branch of VPP's
`vlib_main_or_worker_loop`: drain main RPC work, process main File readiness,
run ready Process Nodes, advance the main process timer wheel, convert timer
expiry into Process Restore records, run restored Process Nodes, and perform
the lifecycle/exit checkpoint. Packet graph stages are present only in worker
loops. A Process Node is a Tokio task, not an operating-system thread.

`ControlThread` owns the scheduler execution, but it does not provide an
external command handle or an external timer API. The daemon's lifecycle path
constructs and runs this scheduler directly on the existing main thread.

### Thread ownership

Per-thread values use the existing `hammer_infra::thread_owned::ThreadOwned`.
`ThreadOwned` is explicitly non-`Send`, has no public `with_mut` or `migrate`,
and does not expose raw pointers. Owner-loop code obtains ordinary Rust
borrows and performs concrete domain operations on its own `DataPlaneMain`.
Cross-thread callers submit domain messages through existing worker-control or
Binary API paths; they never transfer `&mut DataPlaneMain`.

The owner thread is also responsible for clearing and dropping its
`ThreadOwned` value before the global container is torn down. This is part of
the worker join/lifecycle contract, not a generic migration operation.

### RPC and Binary API

Generic main RPC execution is separated from Binary API framing and method
lookup. The generic RPC execution path is private to the main scheduler and
does not create a public queue API. Binary API is the only external RPC
ingress: its Process Node runs on the same main Tokio scheduler, decodes
requests, dispatches registered methods, and emits replies.
Worker code does not receive a new public main-RPC handle.

`SocketApiRegistrationPool`, `SocketToken`, and `ClientSlot` are removed. The
Binary API Process Node stores active connections in
`hammer_infra::Pool<BinaryApiConnection>`. `BinaryApiConnection` is a private
concrete connection record, not a pool wrapper or public capability type. File
private data is the Pool index. No socket-level generation field or custom
slot-generation source remains. Reuse is allowed only after FileMain has
cancelled the poll registration and completed the owner-thread removal
ordering, so a pending readiness notification cannot be applied to a newly
reused connection value.

### Timers

The external `ControlTimerHandle`/`ControlTimerRegistration` API and the
Tokio-spawned control timer registry are removed. Main Process timers are
owned by the main `DataPlaneMain` timer wheel and restore Process Nodes when
they expire. Timer handles in that main process path are pool indexes and have
no generation semantics.

This decision does not turn worker-local TCP/QUIC timer ownership into an
external Control API. Existing worker-local timer users remain owner-local;
removing the entire worker timer primitive would require a separate transport
migration and is outside this ADR.

### Global access and lifecycle

`GlobalMain::install_current`, `GlobalMain::with_current`,
`GlobalMain::uninstall_current`, and the `spawn` module's thread-local
`DataPlaneMain` raw-pointer access are removed. Main-thread code receives the
existing `GlobalMain` reference through the lifecycle path. Worker code keeps
its direct owner-local `DataPlaneMain` reference. Plugin loading, main-loop
enter/exit hooks, Binary API dispatch, and shutdown all use that explicit
owner path.

There is no compatibility facade for the removed control handles, process
container, socket registration pool, or raw-pointer accessors. All in-tree
callers and plugin registration images migrate in the same change.

## 变更清单

### 新增

#### 类型

| 类型 | 位置 | 字段 | 方法/不变量 | 兼容性/验证 |
| --- | --- | --- | --- | --- |
| `BinaryApiConnection` | `hammer-service::binary_api`，私有 | `file_index: Option<u32>`、`read_buf: Vec<u8>`、`output: Vec<u8>` | `new`、owner-local file binding/clear、read/flush buffer access；由 `Pool<BinaryApiConnection>` 直接索引，不带 generation | 仅 Binary API Process Node 内部使用；accept/read/flush/close 和 index reuse 测试 |

不得新增 `MainThread`、`ProcessMain`、`ControlLoop` 或同等主线程聚合类型。

#### API

无新的公开 queue、timer、control handle 或 pointer accessor API。

### 修改

#### 类型

| 类型 | 位置 | 变更 | 迁移/验证 |
| --- | --- | --- | --- |
| `GlobalMain` | `hammer-runtime` | 从单个 `main` 改为 per-thread `DataPlaneMain` 容器；持有主线程 `ControlThread` 生命周期；继续持有 plugin、registry 和 worker 生命周期 | 所有 `data_plane_main()`、生命周期和插件调用点迁移；检查主索引为 0 |
| `ControlThread` | `hammer-runtime` | 从独立 command/timer 服务改为主线程 Tokio 调度器；进程、restore、main timer、main RPC、File readiness 在同一主路径驱动 | daemon 只在主 OS thread 创建和运行；不再返回外部 handle |
| `DataPlaneMain` | `hammer-runtime` | index 0 承担主 Process/restore/timer/RPC/File 状态；worker index 保持 packet graph 和 worker-local 状态 | 主循环固定顺序测试；worker 不能运行主 Process Node |
| `ProcessContext` / `ProcessEntry` | `hammer-runtime` | 移除对 `DataPlaneMain` clone 的依赖；Process Node 只能通过 owner-local 主路径访问主状态 | 所有 `process_node` 注册镜像一起重编译；禁止跨线程借用 |
| `ThreadOwned<T>` | `hammer-infra` | 保持 owner-thread 语义；删除 `Send` 实现、`with_mut` 和 `migrate`；明确 owner-thread drop | 编译期检查非 `Send`；错误线程访问和 owner 回收测试 |
| `TimerHandle` / `TimerEntry` | `hammer-infra` timer path | 删除 generation 字段、generation 校验和辅助 generation 数组；句柄只表示 pool index | timer start/stop/update/expire 行为测试 |

#### API

| API | 位置 | 变更 | 兼容性/迁移 |
| --- | --- | --- | --- |
| `ControlThread::new` / `run` | `hammer-runtime` | 改为主线程 scheduler 生命周期；不再产生 `ControlThreadHandle` | daemon 和所有测试直接持有 `ControlThread` |
| `GlobalMain` lifecycle methods | `hammer-runtime` | `main_loop_enter`、主 loop、close、Process start/restore、Binary API dispatch 统一走主 `DataPlaneMain` | 旧 `run_processes_until`/独立 `ProcessMain` 调用点删除 |
| `ProcessContext` main access | `hammer-runtime` | 禁止返回 clone；主状态只能通过 owner-local concrete operation 使用 | IP reassembly 等 Process Node 调用点迁移 |
| `ThreadOwned` access | `hammer-infra` | 删除泛型 `with_mut`/`migrate`，不允许 raw pointer accessor | plugin worker wrappers 改为 owner-loop concrete operation |
| Binary API client lookup | `hammer-service` | Process Node 直接使用现有 `Pool<BinaryApiConnection>` 和 `u32` index；删除 `ClientSlot`、`SocketToken` generation 编解码 | socket event lifecycle must preserve index-reuse ordering |
| Control timer APIs | `hammer-runtime` | 删除 `ControlThreadHandle::schedule_*`、`ControlTimerHandle`、cancel API 和外部 timer entry | 无兼容层；调用方迁移至 Process timer/Binary API |
| Global/thread-local accessors | `hammer-runtime` | 删除 `GlobalMain::with_current` 和 `spawn::{set,with}_data_plane_main*` | 显式传递 owner reference；不保留 thread-local facade |

### 删除

#### 类型

- `ProcessMain`
- `ControlThreadHandle`
- `ControlTimerHandle`
- `ControlTimerId`
- `ControlTimerRegistration`
- `TimerRegistry` 及其 control-timer task records
- `SocketApiRegistrationPool`
- `SocketToken`
- `ClientSlot`

#### API

- `ThreadOwned::with_mut`
- `ThreadOwned::migrate`
- `GlobalMain::install_current`
- `GlobalMain::with_current`
- `GlobalMain::uninstall_current`
- `spawn::set_data_plane_main`
- `spawn::with_data_plane_main`
- `spawn::with_data_plane_main_mut`
- `ControlThreadHandle::{call,call_blocking,call_async,call_with_timeout}`
- `ControlThreadHandle::{schedule_once,schedule_interval}`
- `ControlTimerHandle::{cancel,cancel_timeout}`

## Consequences

- The main thread has one scheduler and one main `DataPlaneMain` ownership
  path, matching VPP's main/worker distinction.
- Process tasks no longer observe a cloned or detached main runtime.
- Binary API remains a Process Node on the main scheduler.
- Removing generations makes Pool-index reuse an ordering contract; FileMain
  cancellation and pending-event handling must be verified on every poller.
- Existing plugin registration images and all callers of the removed control,
  thread-local, and broad mutable-access APIs must migrate together. No
  cross-version compatibility is promised.

## 依据与验证

- VPP `vlib_global_main_t` stores per-thread `vlib_main_t` entries in
  `third_party/vpp/src/vlib/main.h`.
- VPP's main branch processes main RPC, File readiness, process timers, and
  Process Restore in `third_party/vpp/src/vlib/main.c`.
- VPP main-thread RPC queue and drain are in
  `third_party/vpp/src/vlib/threads.c`.
- VPP socket registration uses a Pool index in
  `third_party/vpp/src/vlibmemory/socket_api.c`; Hammer additionally requires
  an explicit pending-readiness ordering because its File callback signals a
  Process Node asynchronously.

Final implementation verification must include `cargo fmt --all -- --check`,
`git diff --check`, `cargo check --workspace --tests`, focused lifecycle,
ThreadOwned, Binary API index-reuse, Process Restore, and timer tests, followed
by the repository's final pre-commit test gate.

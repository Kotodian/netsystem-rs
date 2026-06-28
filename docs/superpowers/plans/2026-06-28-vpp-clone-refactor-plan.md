# VPP Clone Refactor Plan: Delete FFI, Restructure Runtime, Cross-Platform

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform hammer from an iOS NetworkExtension VPN engine into a cross-platform VPP clone. Delete the `hammer-ffi` UniFFI layer and all iOS packaging artifacts. Restructure `hammer-runtime` to mirror VPP's `vlib` engine (`vlib_main_t` / `vlib_global_main_t` / node-graph fork / barrier / hybrid main loop). Introduce a top-level `hammer` binary (the `vpp` equivalent) and a `hammerctl` IPC client (the `vppctl` equivalent). Add a `hammer-ipc` crate for the control-plane transport (Unix socket + framed messages).

**Architecture:** VPP `vppinfra → vlib → vnet → vpp` maps to `hammer-infra → hammer-runtime(+hammer-adapter) → hammer-service → hammer (bin)`. The init sequence becomes a topologically-sorted `linkme`-registered function framework (replacing VPP's constructor-list + topo-sort). The main thread keeps a tokio-based `ControlThread` for control-plane events, timers, and IPC accept; dataplane workers run a vlib-style fixed-schedule main loop on dedicated OS threads. A barrier (`wait_at_barrier` / `workers_at_barrier` atomics) synchronizes the two when the main thread needs to mutate shared graph state. All `packet_graph::CONTROL_INITS` registrations migrate to `#[init_function]` annotations.

**Tech Stack:** Rust 2024, `linkme` (init/node registration), `petgraph` (topo sort), `clap` (CLI), `bincode`+`serde` (IPC payload), `libc` (eventfd/kqueue/mmap/pthread), `tokio` (ControlThread only). Linux primary, macOS secondary.

## Decisions (confirmed)

| Item | Choice |
|---|---|
| Config | Keep TOML startup config; add `hammerctl` CLI |
| Main loop | Hybrid: ControlThread (tokio) for control plane; vlib-style dataplane main loop on OS threads; barrier synchronization |
| Init framework | Extend linkme with topological sort (petgraph) — `#[init_function]`, `#[config_function]`, `#[main_loop_enter_function]`, `#[main_loop_exit_function]`, `#[worker_init_function]`, `#[early_config_function]` |
| Binary entry | `hammer` daemon + IPC socket; `hammerctl` subcommands via IPC |
| iOS artifacts | Delete ALL (hammer-ffi, hammer-uniffi-bindgen, scripts/build-xcframework.sh, dist/ios/, ios-demo/, Makefile iOS targets, Cargo.toml uniffi dep + size-opt release profile) |
| Topo sort | petgraph |
| IPC framing | `[u32 BE length][bincode payload]` |
| CLI framework | clap |
| Socket path | Fixed `/run/hammer.sock` (overridable via `--sock`) |
| Worker wakeup | Linux eventfd / macOS kqueue EVFILT_USER |

## Global Constraints

- Dependency direction (post-refactor):
  `hammer (bin) → {hammer-runtime, hammer-ipc, hammer-service, hammer-control, hammer-core}`
  `hammerctl (bin) → hammer-ipc`
  `hammer-ipc → {hammer-core, hammer-runtime}`
  `hammer-runtime → {hammer-adapter, hammer-core, hammer-infra, hammer-component-macros}`
  `hammer-service → {hammer-runtime, hammer-adapter, hammer-core, hammer-infra, hammer-component-macros, hammer-control}`
  `hammer-app → {hammer-runtime, hammer-adapter, hammer-core, hammer-infra}`
  `hammer-control → {hammer-adapter, hammer-core}`
  `hammer-adapter → {hammer-core, hammer-infra}`
  `hammer-core → hammer-infra`
  `hammer-infra → (external)`
- No dlopen plugin system (VPP `vlib_plugin_early_init`). All subsystems compile into the `hammer` binary; linkme handles registration at link time. Future plugin system is out of scope.
- The app/session boundary stays VPP `svm_fifo` + `svm_msg_q` semantics (already implemented in `hammer-infra`/`hammer-runtime`). Do not reintroduce `AppRing`/SQE/CQE/submission/completion surfaces for dataplane app/session exchange.
- TCP owns sequence/ACK/loss/recovery/timers; session owns TX byte retention; `TcpSegment` is the output intent constructed only via its constructor and consumed by `tcp-output`. Do not break these invariants during the refactor.
- Barrier memory ordering must mirror VPP `threads.c:296 barrier_check` exactly: `wait_at_barrier` release-store by main, acquire-load by workers; `workers_at_barrier` fetch_add by workers (release), acquire-load by main. Use `Processing::SeqCst` where VPP uses `__sync_synchronize`.
- Topo-sort dependency name typos are runtime-only errors. Add unit tests `assert_all_deps_resolved()` and `assert_no_cycle()` in `hammer-core`.
- No `_underscore` bindings (per AGENTS.md); unused locals deleted. `snake_case` functions, `PascalCase` types, `SCREAMING_SNAKE_CASE` constants. Commit message scope: `<crate>(<Type>): <imperative summary>`.
- No commits unless requested. Run `cargo test -p <crate>` while iterating; `cargo test --workspace` after each phase. Run `cargo fmt --all && cargo clippy --workspace --all-targets` before declaring a phase done.

## Type Design (approved)

```rust
// ── hammer-component-macros (new attribute macros) ──

#[init_function(name = "tcp_init", runs_after = ["buffer_main_init"], runs_before = ["session_init"])]
//   expands to: linkme::distributed_slice!(INIT_FUNCTIONS) push InitFunction { name, runs_before, runs_after, func }
#[config_function(name = "tcp", early = false)]
#[early_config_function(name = "unix")]
#[main_loop_enter_function]
#[main_loop_exit_function]
#[worker_init_function(name = "tcp_worker_init", runs_after = ["generic_worker_init"])]

// ── hammer-core ──

pub struct InitFunction {
    pub name: &'static str,
    pub runs_before: &'static [&'static str],
    pub runs_after:  &'static [&'static str],
    pub func: fn(&mut EngineMain) -> Result<()>,
}

pub struct ConfigFunction {
    pub name: &'static str,
    pub early: bool,
    pub func: fn(&mut EngineMain, input: &toml::Value) -> Result<()>,
}

// linkme-collected slices:
#[linkme::distributed_slice] pub static INIT_FUNCTIONS: [InitFunction];
#[linkme::distributed_slice] pub static CONFIG_FUNCTIONS: [ConfigFunction];
#[linkme::distributed_slice] pub static EARLY_CONFIG_FUNCTIONS: [ConfigFunction];
#[linkme::distributed_slice] pub static MAIN_LOOP_ENTER_FUNCTIONS: [InitFunction];
#[linkme::distributed_slice] pub static MAIN_LOOP_EXIT_FUNCTIONS: [InitFunction];
#[linkme::distributed_slice] pub static WORKER_INIT_FUNCTIONS: [InitFunction];

pub fn vlib_call_all_init_functions(vm: &mut EngineMain) -> Result<()>;
pub fn vlib_call_all_config_functions(vm: &mut EngineMain, early: bool, input: &toml::Value) -> Result<()>;

// Topo sort impl: petgraph::stable_graph DiGraph<&str>;
//   edges from `runs_after` dep -> dependent, and dependent -> `runs_before`.
//   Kahn's algorithm; cycle => panic listing cycle node names.

// lifecycle.rs: keep Lifecycle trait for runtime start/close of services
//   (NetworkManager/CertificateStore/etc). The init framework is for
//   graph/construction; Lifecycle is for runtime state machine.
// LIFECYCLE_ORDER constant removed — let topo sort drive init order.

// ── hammer-runtime (new engine types) ──

#[repr(align(64))]
pub struct EngineMain {                  // one per thread; vlib_main_t
    pub thread_index: u32,
    pub numa_node: u32,
    pub time: Time,
    pub main_loop_count: AtomicU32,
    pub node_main: NodeMain,             // per-thread node graph runtime (forked)
    pub buffer_main: Arc<BufferMain>,    // shared per-NUMA pool
    pub pending_frames: Vec<PendingFrame>,
    pub next_frames: Vec<NextFrame>,
    pub handoff_queue_pending_bmp: u64,
    pub wait_at_barrier: Arc<AtomicU32>,     // shared; workers point to same Arc
    pub workers_at_barrier: Arc<AtomicU32>,  // shared
    pub epoll_fd: i32,                       // worker-local epoll/kqueue
    pub wakeup_fd: WakeupFd,                 // Linux eventfd / macOS kqueue EVFILT_USER
    pub timing_wheel: TimerWheel1t2w2048sl,
    pub rpc_pending: RpcQueue,
    pub main_loop_exit_now: AtomicBool,
    pub main_loop_exit_status: Mutex<i32>,
}

pub struct GlobalMain {                 // vlib_global_main_t, singleton
    pub engine_mains: Vec<Arc<EngineMain>>,   // [0] = main thread
    pub name: String,
    pub exec_path: String,
    pub argv: Vec<String>,
    pub startup_config: String,
    pub elog: ElogMain,
}

impl GlobalMain {
    pub fn new() -> Self;
    pub fn fork_worker(&mut self, idx: u32) -> Arc<EngineMain>;  // clone node graph
}

// ── hammer-adapter (WakeupFd abstraction) ──

pub trait WakeupFd: Send + Sync {
    fn wake(&self);
    fn consume(&self);
    fn raw_fd(&self) -> i32;
}
// cfg target_os = "linux": eventfd(EFD_NONBLOCK | EFD_CLOEXEC)
// cfg target_os = "macos": kqueue + EVFILT_USER (wake via kevent NOTE_TRIGGER)

// ── hammer-ipc (new) ──

pub struct IpcServer;                  // bind + accept on ControlThread epoll
pub struct IpcClient;                  // hammerctl uses this
pub struct IpcRequest;                 // enum (see below)
pub struct IpcReply;                   // enum

// Frame: [u32 BE length][bincode payload]
// Accept: pool of Registration { stream, unprocessed: Vec<u8> }
// Read does framing only; handler runs on control thread
//   (mp-safe direct; non-mp-safe via barrier)

#[derive(Serialize, Deserialize)]
pub enum IpcRequest {
    Pause, Wake, ResetNetwork, Shutdown,
    Metrics { format: MetricsFormat },
    ConfigReload { toml: String },
    Status, ListListeners, ListSessions,
}

#[derive(Serialize, Deserialize)]
pub enum IpcReply {
    Ok, Error(String),
    Metrics(Box<[u8]>),                 // bincode-encoded snapshot
    Status(RuntimeStatus),
    Listeners(Vec<ListenerInfo>),
    Sessions(Vec<SessionInfo>),
}

// ── hammer (bin) ──
//   clap: -c/--config <file>, --daemon, -i/--interactive, --sock <path>,
//         --log-level <level>, -v/--version
//   Flow: parse → read TOML → early config fns → pin main core →
//         GlobalMain::new + vlib_main_init → vlib_call_all_init_functions →
//         vlib_call_all_config_functions(early=false) →
//         sort WORKER_INIT_FUNCTIONS →
//         vlib_call_all_main_loop_enter_functions (start_workers spawns OS
//              threads running engine_main_loop) →
//         run ControlThread (tokio) on main thread →
//         SIGINT/SIGTERM → vlib_call_all_main_loop_exit_functions →
//         trigger barrier → join workers → exit

// ── hammerctl (bin) ──
//   clap subcommands: pause | wake | reset-network | shutdown | metrics |
//                     status | listeners | sessions | "config reload" <path>
//   Each: IpcClient::connect(sock_path).request(IpcRequest::X) → format reply
```

## Phase A — Cleanup & Scaffolding

- [ ] A1. Delete `crates/hammer-ffi/` directory.
- [ ] A2. Delete `crates/hammer-uniffi-bindgen/` directory.
- [ ] A3. Delete `scripts/build-xcframework.sh` (and `scripts/` if empty).
- [ ] A4. Delete `dist/ios/` directory.
- [ ] A5. Delete `ios-demo/` directory.
- [ ] A6. Edit `Makefile`: remove `ios-lib`, `clean-ios-lib`, `xcframework` targets and any iOS-only variables. Keep `clean`/`test`/`fmt`/`clippy` targets; stub `build`/`run`/`ctl` targets deferred to Phase E.
- [ ] A7. Edit root `Cargo.toml`:
  - Remove `crates/hammer-ffi` and `crates/hammer-uniffi-bindgen` from `members`.
  - Remove `uniffi = "0.31"` from `[workspace.dependencies]`.
  - Add `petgraph`, `clap`, `bincode` to `[workspace.dependencies]` (versions: petgraph `0.6`, clap `4`, bincode `1`).
  - Replace `[profile.release]` with: `lto = "thin"`, `codegen-units = 16`, `opt-level = 3` (drop `panic = "abort"`, `strip = "symbols"`, `opt-level = "z"`, `codegen-units = 1`, the iOS comment).
  - Keep `[profile.release-perf]` (drop iOS comment, keep `debug = "line-tables-only"`).
  - Keep `boringtun` patch (wireguard feature uses it).
- [ ] A8. Create `crates/hammer-ipc/` with `Cargo.toml` (lib, deps: `hammer-core`, `serde`, `bincode`, `libc`) and `src/lib.rs` (empty module declarations).
- [ ] A9. Create `crates/hammer/` with `Cargo.toml` (bin, deps: `hammer-runtime`, `hammer-ipc`, `hammer-service`, `hammer-control`, `hammer-core`, `clap`, `tokio`, `toml`, `tracing`) and `src/main.rs` (stub `fn main() { println!("hammer: stub"); }`).
- [ ] A10. Create `crates/hammerctl/` with `Cargo.toml` (bin, deps: `hammer-ipc`, `clap`, `bincode`) and `src/main.rs` (stub).
- [ ] A11. Add `crates/hammer`, `crates/hammerctl`, `crates/hammer-ipc` to `Cargo.toml` `members`.
- [ ] A12. Run `cargo build --workspace` — must pass with the three stub crates compiling.
- [ ] A13. Run `cargo fmt --all`.

## Phase B — Init Framework + GlobalMain/EngineMain Skeleton

- [ ] B1. `hammer-component-macros`: add proc-macro `#[init_function]` that expands to a `linkme::distributed_slice!(INIT_FUNCTIONS)` item with `InitFunction { name, runs_before, runs_after, func }`. Parse attributes: `name`, `runs_before` (list of string literals), `runs_after` (list).
- [ ] B2. Add `#[config_function(name, early)]` → `CONFIG_FUNCTIONS` or `EARLY_CONFIG_FUNCTIONS`. Add `#[early_config_function(name)]` (sugar for `early=true`).
- [ ] B3. Add `#[main_loop_enter_function]` → `MAIN_LOOP_ENTER_FUNCTIONS`; `#[main_loop_exit_function]` → `MAIN_LOOP_EXIT_FUNCTIONS`; `#[worker_init_function(name, runs_after)]` → `WORKER_INIT_FUNCTIONS` (with topo deps).
- [ ] B4. `hammer-core/src/init.rs`: define `InitFunction`, `ConfigFunction` structs. Declare the six `linkme::distributed_slice` statics. Implement `vlib_call_all_init_functions(vm)` and `vlib_call_all_config_functions(vm, early, input)` using petgraph topo sort. Cycle detection → panic with cycle node names. Missing-dep detection → include in error. Maintain `init_functions_called: HashSet<&'static str>` for dedup.
- [ ] B5. `hammer-core/src/init.rs` tests: `assert_all_deps_resolved` (every `runs_before`/`runs_after` name resolves to a registered fn), `assert_no_cycle`, `assert_order` (topo order respects declared deps), `assert_config_dispatch` (early vs non-early partitioning).
- [ ] B6. `hammer-runtime`: define `EngineMain` struct (cache-line aligned, fields per Type Design). Define `GlobalMain` with `engine_mains: Vec<Arc<EngineMain>>`. `GlobalMain::new()` creates `engine_mains[0]` (main-thread instance).
- [ ] B7. Absorb fields from `hammer-adapter::DataPlaneRuntime` into `EngineMain` (buffer pool, node runtime, handoff, current node, worker). Keep `DataPlaneRuntime` as a thin type alias or remove if all call sites can migrate. Update `hammer-adapter` re-exports.
- [ ] B8. Define `EngineMain::fork_worker(idx)` — clone node graph state (dup `next_frames`, reset `pending_frames`, deep-copy `nodes` clearing owner/stats, dup `nodes_by_type`, per-thread frame freelists). Return `Arc<EngineMain>`. Stubs OK for node internals that don't exist yet (B phase only needs the skeleton).
- [ ] B9. `hammer-service/src/packet_graph.rs`: convert each `init_control_plane` fn registered via `linkme::distributed_slice!(CONTROL_INITS)` to an `#[init_function(name="...")]`-annotated fn. Delete `CONTROL_INITS`. Update `RuntimeService::start_inner` to call `vlib_call_all_init_functions(vm)` instead of iterating `CONTROL_INITS` (keep old `lifecycles: Vec` path for runtime start/close during migration).
- [ ] B10. Run `cargo test -p hammer-core` (init framework tests), then `cargo test --workspace`. All existing tests must still pass (`tcp_control_plane.rs`, etc.), possibly with the migration dual-path.
- [ ] B11. `cargo fmt --all && cargo clippy --workspace --all-targets`.

## Phase C — Atomic Barrier + WakeupFd + engine_main_loop + Worker Fork

**Design constraints (per user approval):**
- VPP-style atomic barrier `(wait_at_barrier / workers_at_barrier Arc<AtomicU32>)` — delete `DataPlaneBarrierState`/`Handle`/`Guard`/`Control` from spawn.rs entirely
- `WakeupFd` factory returns `impl WakeupFd` (no `Box<dyn>`, no enum dispatch) — one concrete `Wakeup` per target via cfg
- RAII `BarrierGuard` with plain `Drop` release — no `Held`/`Released` type-state ceremony
- `fork_worker` = per-thread `DataPlaneRuntime::with_buffer_capacity(...)` + `init_graph()` — fresh construction, NOT Rc-clone-and-send (DataPlaneRuntime is !Send)
- `engine_main_loop` in vlib fixed-schedule step order (main.c:1442-1693); tokio reactor driven as step #4 — existing transport/session futures keep working
- No `WorkerIdx` newtype, no `WorkerSpawnError` enum — `u32`, `HammerError::internal`
- `#[must_use]` on `BarrierGuard`
- `thiserror` for `WakeupFdError` only; runtime errors reuse `HammerError::internal`
- No `unwrap`/`expect` outside bootstrap paths; no `_underscore` bindings
- `&T` borrows throughout `barrier_check`/`engine_main_loop`; no clones in the loop
- `!Send` `DataPlaneRuntime` stays thread-local via existing `DATA_PLANE_RUNTIME` pattern

**Memory ordering (VPP threads.c:296 barrier_check):**
- `wait_at_barrier` release-store by main, acquire-load by workers
- `workers_at_barrier` `fetch_add(1, Release)` by workers, `load(Acquire)` by main
- Both use `SeqCst` where VPP uses `__sync_synchronize` (full barrier)

- [ ] C1. New files:
  - `crates/hammer-adapter/src/wakeup.rs`: `WakeupFd` trait (`wake`, `consume`, `raw_fd`), `cfg(target_os = "linux") LinuxEventfdWakeup` (eventfd EFD_NONBLOCK|EFD_CLOEXEC), `cfg(target_os = "macos") MacosKqueueWakeup` (kqueue + EVFILT_USER ident, wake via kevent NOTE_TRIGGER, EV_CLEAR for consume), platform-generic `WakeupFd::new() -> io::Result<impl WakeupFd>` factory. `thiserror`-derived `WakeupFdError`. Unit test `wakeup_self_roundtrip`: create wakeup, wake, consume, assert no spurious wake.
  - Add `pub mod wakeup;` to `hammer-adapter/src/lib.rs`.
- [ ] C2. New files:
  - `crates/hammer-runtime/src/barrier.rs`: `BarrierGuard(Arc<AtomicU32>, Arc<AtomicU32>)` with `#[must_use]`. Static fn `barrier_sync(wait: &AtomicU32, workers: &AtomicU32, n_workers: u32) -> BarrierGuard`. Drop: `barrier_release(wait, workers)`. Module fns: `fn barrier_check(wait: &AtomicU32, workers: &AtomicU32)` — if `wait.load(Acquire) > 0`, `workers.fetch_add(1, Release)`, spin `while wait.load(Acquire) > 0 { spin_loop_hint() }`. Doc comment cites VPP threads.c:296.
  - Delete from `crates/hammer-runtime/src/spawn.rs`: `DataPlaneBarrierState` struct, `BarrierStateHandle`, `BarrierGuard`, `BarrierControl`, and the barrier initialization block in `SpawnContext` (lines ~70-99 and ~670-793). Remove `barrier` field from `SpawnContext`.
  - Unit tests: `barrier_sync_barrier_check_roundtrip` (AtomicBool flag protected by barrier, single mock worker thread), `barrier_concurrent_workers` (4 threads, all must reach barrier before any proceeds).
  - Add `pub mod barrier;` to `hammer-runtime/src/lib.rs`.
- [ ] C3. New file `crates/hammer-runtime/src/start_workers.rs`:
  - `#[init_function(name = "start_workers")] fn start_workers(vm: &EngineMain) -> Result<()>`.
  - Allocate `Arc<AtomicU32>` pair for barrier.
  - For each worker index 1..n: call `DataPlaneRuntime::with_buffer_capacity(...)`, `let engine = DataPlaneRuntime::new_engine(barrier.clone())`, store arcs in shared vec. Spawn OS thread via `std::thread::Builder` with name `hammer-worker-{idx}`, entry `worker_main(idx, engine, wait_arc, workers_arc)`.
  - `fn worker_main(idx: u32, engine: Engine, wait: Arc<AtomicU32>, workers: Arc<AtomicU32>)`: install DataPlaneRuntime into thread-local, call `init_graph(worker=idx, ...)`, call `engine_main_loop(&engine)`.
  - Add `pub mod start_workers;` to `hammer-runtime/src/lib.rs`.
  - Target is vlib-style fork: fresh DataPlaneRuntime, NOT clone-and-send of the main-thread's Rc<RefCell> graph state.
- [ ] C4. New file `crates/hammer-runtime/src/main_loop.rs`:
  - `fn engine_main_loop(engine: &Engine) -> i32`: vlib fixed-schedule per iteration:
    (1) `barrier::barrier_check(&engine.wait_at_barrier, &engine.workers_at_barrier)`
    (2) flush pending RPC + drain handoff queues (existing `drain_handoffs()`)
    (3) main-loop callbacks (currently no-op; hook for future)
    (4) tokio reactor tick: `DATA_PLANE_RUNTIME.with(|r| r.tokio_handle.borrow().as_ref().map(|h| h.try_current()))` — drive existing transport/session futures
    (5) dispatch polling-state nodes per `nodes_by_type` (existing dispatching)
    (6) dispatch interrupt nodes (bitset scan via existing `interrupt_node_mask`)
    (7) dispatch timer nodes (timing wheel pop)
    (8) drain `pending_frames`, advance timers, `main_loop_count += 1`
    (9) if `main_loop_exit_now` → return exit status
  - Keep existing `tun_read_callback` / `tun_write_callback` or integrate into step (4) file_poll if epoll/kqueue is present.
  - Reference: VPP `main.c:1442-1693` step order.
- [ ] C5. Modify `crates/hammer-runtime/src/control_thread.rs`:
  - Add `fn control_call_with_barrier<R>(&self, n_workers: u32, f: impl FnOnce() -> R) -> R`.
  - Body: `let _guard = barrier::barrier_sync(&wait, &workers, n_workers); f()`. Guard drop releases barrier.
  - Keep existing `control_call` for mp-safe reads (no barrier needed).
  - Wire the ControlThread's barrier arcs to those created in `start_workers`.
- [ ] C6. Modify `crates/hammer-service/src/packet_graph.rs`:
  - Add `#[worker_init_function(name = "install_worker_graph")] fn install_worker_graph(vm: &EngineMain)`.
  - Move body of `service.rs:390-414` closure `install_on_workers` into this function.
  - Delete `install_on_workers` closure from `crates/hammer-service/src/service.rs`.
  - This must compile so `start_workers` can call it.
- [ ] C7. Unit tests:
  - `barrier_single_worker`: spawn 1 worker thread running a loop that calls `barrier_check` each iteration. Control thread calls `barrier_sync` -> `barrier_release`. Verify worker flag toggled.
  - `barrier_multi_worker`: 4 workers, all must reach barrier before any continues.
  - `engine_main_loop_exits_on_flag`: spawn worker with `main_loop_exit_now=true`, verify loop returns quickly.
  - `wakeup_self_roundtrip` (for C1, if not already added in C1).
- [ ] C8. Integration test `crates/hammer-runtime/tests/worker_spawn.rs`:
  - Start N workers via `start_workers`, verify they enter engine_main_loop. Use `control_call_with_barrier` to mutate a shared AtomicU32, verify all workers observe the new value after barrier release.
  - Alternatively, validate via spying on `main_loop_count`.
- [ ] C9. `cargo test --workspace` — all existing tests (tcp_control_plane, session lifecycles, transport) must still pass. Resolve any breakage.
- [ ] C10. `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`. This is the pre-merge gate.

## Phase D — IPC + Binary + Lifecycle Re-attach

- [ ] D1. `hammer-ipc/src/frame.rs`: implement `read_frame(stream) -> Result<Vec<u8>>` and `write_frame(stream, payload: &[u8])`. Frame = 4-byte BE u32 length prefix + payload. Handle partial reads/writes with a `Vec<u8>` accumulation buffer per stream.
- [ ] D2. `hammer-ipc/src/protocol.rs`: define `IpcRequest` / `IpcReply` enums (per Type Design), `MetricsFormat` enum (`Table`, `Json`, `Prometheus`), `RuntimeStatus`, `ListenerInfo`, `SessionInfo` structs. Derive `Serialize, Deserialize` (bincode 1). Version the protocol with a `PROTOCOL_VERSION: u32` constant sent on connect.
- [ ] D3. `hammer-ipc/src/server.rs`: `IpcServer::bind(path)`, `accept_on(control_thread)`. Accept loop registers streams into ControlThread's epoll/kqueue; `read_ready` performs framing and enqueues `(reg_index, IpcRequest)` onto a control-thread channel; `dispatch` runs handlers. Non-mp-safe handlers call `control_thread.barrier_sync()` first.
- [ ] D4. `hammer-ipc/src/client.rs`: `IpcClient::connect(path)`, `.request(IpcRequest) -> Result<IpcReply>` (write frame, read frame, deserialize). Add `connect_with_version` handshake.
- [ ] D5. Implement handlers in `hammer-runtime` (or `hammer-service`) for each `IpcRequest`: `Pause`/`Wake` → `PauseManager`; `ResetNetwork` → `NetworkManager::reset_network`; `Metrics` → `MetricsRegistry` snapshot encoded via bincode; `ConfigReload` → re-parse TOML and re-run `vlib_call_all_config_functions`; `Status` → `RuntimeStatus { running, n_workers, n_sessions, uptime }`; `ListListeners`/`ListSessions` → from the registry; `Shutdown` → set `main_loop_exit_now` + status.
- [ ] D6. Wire `IpcServer` into `ControlThread`: add an `ipc: Option<IpcServer>` field; `start_inner` binds to `--sock` path (default `/run/hammer.sock`) and registers the listener fd into the control-thread tokio runtime. Handle `bind` permission errors (EACCES on `/run/` → fallback to `$XDG_RUNTIME_DIR/hammer.sock` with warning).
- [ ] D7. `crates/hammer/src/main.rs`: clap parser (`-c/--config <file>`, `--daemon`, `-i/--interactive`, `--sock <path>`, `--log-level <level>`, `--version`). Read TOML from file or stdin. Early config fns first (signals, daemonize via `libc::daemon`, pidfile under `/run/hammer.pid`). Pin main thread via `pthread_setaffinity_np` per `cpu { main-core }` block. Construct `GlobalMain`, run `vlib_call_all_init_functions`, `vlib_call_all_config_functions(early=false, toml)`, sort + run `WORKER_INIT_FUNCTIONS` on forked workers (via `vlib_call_all_main_loop_enter_functions` which calls `start_workers`). Start ControlThread (tokio runtime on main thread). Install `tokio::signal::ctrl_c()` + `SIGTERM` handlers → call `vlib_call_all_main_loop_exit_functions` → trigger barrier → join workers → flush trace/metrics → exit.
- [ ] D8. `crates/hammerctl/src/main.rs`: clap subcommands (`pause`, `wake`, `reset-network`, `shutdown`, `metrics [--format json|table]`, `status`, `listeners`, `sessions`, `config reload <path>`). Each connects to `/run/hammer.sock` (or `--sock <path>`), sends `IpcRequest`, formats `IpcReply` to stdout.
- [ ] D9. Refactor `RuntimeService::start`: remove the FFI-callable entry. `start()` is now invoked from the `hammer` binary's init chain (via an `#[init_function(name="runtime_service_init")]` that constructs `RuntimeService` and stashes it in `EngineMain`). Keep `RuntimeService::close` for the exit chain. Delete `hammer-ffi`-specific public surface (already gone with the crate deletion).
- [ ] D10. Add integration test `hammer_e2e.rs`: build `hammer` binary, write a minimal `startup.toml`, spawn `./hammer -c startup.toml &` with `--sock /tmp/hammer-test.sock`, call `hammerctl --sock /tmp/hammer-test.sock status`, assert reply. Clean up.
- [ ] D11. `cargo test --workspace`. `cargo fmt --all && cargo clippy --workspace --all-targets`.

## Phase E — Cleanup & Documentation

- [ ] E1. Audit `ControlThread` and remove fields now superseded by `EngineMain` (per-thread time, counters, node runtime refs if duplicated). Document the ControlThread↔EngineMain ownership split in a module doc comment: ControlThread owns control-plane events/timers/IPC/barrier-main-side; EngineMain owns dataplane per-thread node graph/main loop/barrier-worker-side.
- [ ] E2. Audit `hammer-service`/`hammer-runtime` for any remaining `apple-platform`/`linux-platform` feature gates that were only for the iOS demo. Keep cross-platform-relevant gates; remove iOS-only ones.
- [ ] E3. Rewrite `Makefile`: targets `build` (`cargo build --workspace`), `build-release` (`cargo build --workspace --release`), `run` (`cargo run -p hammer -- -c startup.toml`), `ctl` (`cargo run -p hammerctl --`), `clean` (`cargo clean`), `test` (`cargo test --workspace`), `clippy` (`cargo clippy --workspace --all-targets`), `fmt` (`cargo fmt --all`), `fmt-check` (`cargo fmt --all -- --check`). Remove any iOS references.
- [ ] E4. Rewrite `README.md`: drop iOS section. Add: project goal (cross-platform VPP clone), VPP→hammer crate mapping table (`vppinfra→hammer-infra`, `vlib→hammer-runtime+hammer-adapter`, `vnet→hammer-service`, `vpp→hammer`, `vppctl→hammerctl`, `vlibmemory+vlibapi→hammer-ipc`, `vcl→hammer-app`, `svm→hammer-infra`), quickstart (`cargo build --workspace`, write `startup.toml`, `./hammer -c startup.toml &`, `hammerctl status`), architecture overview (init framework, hybrid main loop, barrier).
- [ ] E5. Update `AGENTS.md`: remove iOS NetworkExtension references. Generalize the "VPP Refactor Principles" from iOS+VPP to cross-platform VPP clone. Document the init-function framework (linkme + topo sort + `#[init_function]`), the `EngineMain`/`GlobalMain` model, the barrier contract, the hybrid main loop split, `hammer`/`hammerctl`/`hammer-ipc` responsibilities. Keep the TCP/session/buffer carve-out rules (those are unchanged). Note "no dlopen plugin system; linkme only".
- [ ] E6. Run `cargo fmt --all && cargo clippy --workspace --all-targets && cargo test --workspace`. Resolve any findings.
- [ ] E7. Verify no `hammer-ffi`/`hammer-uniffi-bindgen`/`uniffi`/`ios-demo`/`dist/ios`/`build-xcframework`/`ios-lib` references remain anywhere: `rg "hammer-ffi|uniffi|ios-demo|build-xcframework|ios-lib" --glob '!third_party/**'` should return zero hits.

## Risk Register

- **Barrier ordering pitfalls**: must copy VPP `threads.c:296` fence orderings field-by-field. Mitigation: add a `// verified: matches VPP threads.c:296 barrier_check` comment on each fence with the C source excerpt in the doc comment; review against `third_party/vpp/src/vlib/threads.c` before merging Phase C.
- **Topo-sort dependency name typos**: only caught at runtime. Mitigation: `assert_all_deps_resolved` runs in `cargo test`; also call it during `hammer` startup before `vlib_call_all_init_functions` (fail fast).
- **Replacing worker injection breaks `tcp_control_plane.rs`**: the old `install_on_workers` + per-worker `init_graph` is replaced by `fork_worker`. Mitigation: run `cargo test -p hammer-runtime` after C6/C7; if broken, restore temporarily and file a follow-up.
- **macOS kqueue `EVFILT_USER` semantics**: not as ubiquitous as eventfd; needs careful testing. Mitigation: C1 unit test `wakeup_macos_roundtrip` + CI on macOS runner if available; fall back to `pipe(2)` if kqueue proves flaky (documented in `WakeupFd` doc).
- **`DataPlaneRuntime` absorption**: collapsing `DataPlaneRuntime` into `EngineMain` may touch many node-dispatch call sites. Mitigation: B7 keeps a type alias forward-compat; full migration to `EngineMain`-named fields gia gradual in B/C.
- **IPC `/run/hammer.sock` permissions**: binding to `/run/` needs root. Mitigation: D6 falls back to `$XDG_RUNTIME_DIR/hammer.sock` on EACCES with a warning; document root requirement in README.
- **No dlopen plugins**: future plugin requirements need a separate design. Out of scope for this refactor; documented as a limitation in README.

## Open Items (deferred)

- Binary API message ID allocation (VPP `vl_msg_api_get_msg_ids`) — not needed; we use Rust enum `IpcRequest` dispatch instead of per-plugin ID ranges.
- `vppctl`-style telnet CLI — deferred; `hammerctl` over Unix socket is the entry.
- Plugin crate system (dlopen) — future work.
- Stats export (VPP `gmon`/`svmdb`/Prometheus) — minimal via `metrics --format prometheus`; full `svmdb` equivalent is future work.
- Multi-process app via `vcl_sapi_app_worker_add` — `hammer-app` already has the attach client; worker-add for multi-process apps is future work.
# VPP-Style Per-Thread State and Worker Dispatch

Status: accepted

Date: 2026-09-11

Hammer deletes `ThreadOwned<T>`, the runtime-wide `DataRemoteLocalQueue`, and
the unused generic runtime metrics registry. It also moves the protocol-neutral
`Network` and `SocksAddr` values from runtime to service. Each module keeps
per-thread values in its existing `Vec`, boxed slice, or `Pool`, constructs
every entry before worker launch, and directly borrows the entry selected by
the executing runtime `thread_index`. Cross-thread work uses the owning
subsystem's existing event path.

No common per-thread container or generic access surface is added. There is no
replacement trait, token, callback accessor, compatibility facade, or renamed
`ThreadOwned`. `DataPlaneMain` continues to expose its existing
`thread_index()` fact; it neither owns nor mediates another module's per-thread
state.

This ADR supersedes only the `ThreadOwned<T>` and generic
main-to-Data-Worker control-path decisions in ADR-0001, ADR-0005, and ADR-0009.
Their remaining Tokio Process, runtime authority, barrier, IP, and plugin
decisions remain in force. This change set implements the accepted replacement;
its compile-only validation status is recorded in the completion review.

## Scope

In scope:

- Session, TCP, UDP, TLS, HTTP, QUIC, IP throttle, and Buffer cache state that
  used `ThreadOwned<T>` in the pre-change baseline;
- owner-local `with_worker` and `with_worker_mut` closure accessors;
- `DataRemoteLocalQueue`, `schedule_on_worker`, worker-loop polling, worker
  control configuration, and their errors;
- Session, TCP, UDP, QUIC, and IP callers that schedule runtime worker
  closures;
- `hammer_runtime::metrics`, its public re-exports, tests, and its otherwise
  unused `metrics` and `metrics-derive` dependencies.
- `hammer_runtime::network::{Network, SocksAddr}` and their root re-exports.

Out of scope:

- `DataPlaneHandoff`, which transfers packet Buffer ownership between graph
  workers;
- app/session SVM FIFOs and message queues;
- worker-to-main RPC, thread-zero Tokio Process scheduling, and worker barrier
  publication;
- VPP-style owner statistics, Stats Segment publication, Node error counters,
  and protocol-specific facts such as TCP congestion measurements;
- compatibility aliases or deprecated shims for removed APIs.

## Evidence Ledger

| ID | Source class | Path and symbol | Verified behavior | Design consequence |
| --- | --- | --- | --- | --- |
| H1 | Hammer baseline | `crates/hammer-infra/src/thread_owned.rs`: `ThreadOwned<T>` | A slot starts empty, installs `T` after its OS thread starts, records `ThreadId`, and wraps `Option<T>` in `RefCell` | Delete a lifecycle the design no longer accepts |
| H2 | Hammer baseline | `crates/hammer-runtime/src/spawn.rs`: `DataRemoteLocalQueue`, `schedule_on_worker`, `poll_remote_local_tasks` | Runtime queues arbitrary `Box<dyn FnOnce(&mut DataPlaneMain)>` values and polls them in every worker loop | Delete generic main-to-worker execution |
| H3 | Hammer baseline | `crates/hammer-service/src/session/runtime.rs`: `SessionMain::workers`, `with_worker_mut`, `schedule_worker_task` | Session stores `ThreadOwned<SessionWorker>`, lends it through a closure, and blocks on an `mpsc` reply for cross-thread work | Preconstruct the existing vector, return its selected borrow, and use Session events for cross-thread work |
| H4 | Hammer baseline | `crates/hammer-plugins/transport/tcp/src/lib.rs`, `transport/udp/src/worker.rs`, `app-session/quic/src/listener.rs` | TCP, UDP, and QUIC use closure accessors and runtime worker closures | Direct worker borrows replace closure access; connect work enters through Session dispatch |
| H5 | Hammer baseline | `crates/hammer-plugins/app-session/tls/src/lib.rs`, `app-session/http/src/listener.rs` | TLS and HTTP store installable worker slots although their work already executes on a selected Session worker | Preconstruct their existing worker collections and directly borrow the current slot |
| H6 | Hammer baseline | `crates/hammer-core/src/buffer/main.rs`, `buffer/pool.rs`: `BufferPool::workers`, `borrow_worker_caches` | Each Buffer Pool owns a per-thread cache array; each cache is lazily installed through `ThreadOwned` | Keep Pool ownership and preconstruct each cache; do not move it to `DataPlaneMain` |
| H7 | Hammer baseline | `crates/hammer-plugins/net/ip/src/lookup.rs`, `reassembly.rs` | ICMP throttle uses a vector of `ThreadOwned`; reassembly expiry schedules one runtime closure per worker | Use the existing throttle vector and let the Process walk locked reassembly state |
| H8 | Hammer baseline | `crates/hammer-runtime/src/metrics.rs`, `lib.rs`, `Cargo.toml` | A generic `Mutex<HashMap<MetricKey, Arc<AtomicU64>>>` registry, scopes, counters, gauges, snapshots, and `metrics::Recorder` bridge are exported; scoped repository search found no caller outside that module and its re-export | Delete the whole unused registry and its two dependencies; preserve owner statistics |
| H9 | Hammer baseline | `crates/hammer-runtime/src/network.rs`, `lib.rs`; `crates/hammer-service/src/net` | Runtime defines and re-exports `Network` and `SocksAddr`; scoped repository search found no in-tree caller, while service already owns protocol-neutral network infrastructure and depends on runtime | Move both values to `hammer_service::net`; runtime needs no reverse dependency or compatibility re-export |
| V1 | VPP | `third_party/vpp/src/vlib/main.h`: `vlib_global_main_t::vlib_mains`; `third_party/vpp/src/vlib/threads.c`: `start_workers` | VPP creates per-thread mains and assigns stable indexes before launching workers | Thread index and collection length are startup facts |
| V2 | VPP | `third_party/vpp/src/vnet/session/session.h`: `session_main_t::wrk`, `session_main_get_worker`; `session_node.c`: `session_queue_node_fn` | Session owns a worker vector and directly selects `wrk[thread_index]` during Session queue execution | Session callers directly borrow the selected Session worker |
| V3 | VPP | `third_party/vpp/src/vnet/tcp/tcp.h`: `tcp_get_worker`; `third_party/vpp/src/vnet/udp/udp.h`: `udp_worker_get`; `third_party/vpp/src/plugins/quic/quic.h`: `quic_wrk_ctx_get` | Protocol mains directly index their worker vectors | Hammer protocol owners keep concrete vectors and direct owner accessors |
| V4 | VPP | `third_party/vpp/src/vnet/session/session.c`: `session_send_evt_to_thread`, `session_send_rpc_evt_to_thread`; `session_node.c`: `session_event_dispatch_ctrl` | Main-to-worker Session work is queued on the target Session worker's `vpp_event_queue` and executed by the Session node | Reuse Hammer's Session message queue and Session queue Node |
| V5 | VPP | `third_party/vpp/src/vlib/threads.c`: `vlib_rpc_call_main_thread`, `vlib_rpc_call_main_thread_process` | Generic vlib RPC is worker-to-main; VPP has no symmetric generic main-to-worker `vlib_main_t` closure queue | `DataRemoteLocalQueue` has no VPP runtime counterpart |
| V6 | VPP | `third_party/vpp/src/vnet/ip/reass/ip4_full_reass.c`: `ip4_full_reass_walk_expired` | The main Process walks `per_thread_data`, locks each entry, and expires it directly | IP expiry does not schedule worker closures |
| V7 | VPP | `third_party/vpp/src/vlib/buffer.h`: `vlib_buffer_pool_t::threads`; `buffer.c`: `vlib_buffer_num_workers_change`; `buffer_funcs.h`: `vlib_buffer_alloc_from_pool` | Each Buffer Pool owns its cache vector, sized for all threads and selected by `vm->thread_index` | Keep Buffer caches in `BufferPool`; remove only lazy install semantics |

A scoped search under `third_party/vpp/src/vlib` and
`third_party/vpp/src/vnet` found no runtime queue that sends an arbitrary
main-to-worker callback receiving the target worker's `vlib_main_t *`.
Session-owned events and generic worker-to-main RPC are distinct mechanisms.

## Semantic Diff

| Dimension | Baseline Hammer | Accepted Hammer design | VPP evidence | Impact |
| --- | --- | --- | --- | --- |
| Per-thread storage | Existing Vec/slice entries contain empty `ThreadOwned<T>` slots | Existing owner Vec, slice, or Pool contains fully constructed values | V1-V3, V7 | Remove `Option`, install, clear, and OS `ThreadId` state |
| Current-worker access | `with_worker`/`with_worker_mut` execute caller closures | `worker(thread_index)` returns the existing direct borrow type | V2-V3 | Delete closure APIs; call methods on the borrowed worker |
| Runtime relationship | Draft designs made `DataPlaneMain` a generic access authority | `DataPlaneMain` only supplies its existing numeric `thread_index()` to its caller | V1-V3 | No generic method, trait, token, or owner registration |
| Worker startup | Worker-init creates and installs module worker state | Module init creates all entries; worker-init configures the current existing entry | V1-V3 | No cross-thread installation lifecycle remains |
| Cross-thread Session work | Runtime executes arbitrary closures, sometimes followed by blocking `recv` | Existing Session queue carries concrete Session control events | V4 | Queue acceptance and completion remain distinct |
| TCP/UDP/QUIC connect | Transport code schedules itself on a runtime worker | Session `Connect`/`ConnectStream` dispatch invokes the selected transport | V2-V4 | Transport does not choose runtime scheduling |
| IP expiry | Main Process schedules one closure per worker | Main Process walks locked per-thread reassembly entries | V6 | Runtime queue is removed; packet handoff is unchanged |
| Buffer cache | Pool cache entries lazily install on first access | Pool cache entries are constructed with `BufferPool` and borrowed by thread index | V7 | Preserve VPP Pool ownership; no new `DataPlaneMain` field |
| Generic metrics | Runtime exports a second global metric store and recorder | Owners retain their own VPP-style stats; generic runtime metrics module is absent | H8 and repository Stats Owner contract | Remove dead API, lock, allocations, and dependencies |
| Network values | Runtime owns dial/listen network family and SOCKS destination values | Service `net` owns both values with unchanged fields and behavior | Repository crate ownership | Remove protocol-neutral domain values from the runtime engine |

## Decisions

### D1: Reuse Owner Collections

No common per-thread container is added. Each owner keeps the collection that
already matches its domain:

- fixed Session, TCP, UDP, HTTP, and QUIC worker entries remain in their
  existing `Vec` or boxed slice;
- TLS keeps one existing per-thread connection `Pool` entry per worker;
- IP keeps its existing throttle vector and reassembly per-thread vector;
- each `BufferPool` keeps its existing per-thread cache array.

Every entry is a complete value before worker launch. Where a process-global
Main needs interior mutability, the existing standard borrow cell is used
directly in the owner collection. Existing cache-line slot structs stay only
where alignment is meaningful; they no longer contain `ThreadOwned<T>`. No
replacement wrapper is introduced.

### D2: Return the Current Worker Borrow Directly

Owner accessors return the selected existing borrow type and take no callback.
Representative call shapes are:

```rust
// SAFETY: this Node executes on the DataPlaneMain's owning runtime thread.
let mut sessions = unsafe { session_main().worker(runtime.thread_index()) }?;
sessions.poll_session_events()?;

let mut tcp = tcp_main().worker(runtime.thread_index())?;
tcp.expire_timers(now)?;
```

For process-global collections backed by a borrow cell, `worker` returns its
standard mutable guard; an owner reached through exclusive `&mut self` returns
`&mut T` directly. The accessor validates range and stored worker identity. It
does not install, clear, invoke a closure, return a generic capability, or
accept a target worker for cross-thread mutation.

The only valid mutable call-site index is the executing worker's
`runtime.thread_index()`. A `SessionHandle.thread_index` or `DataWorkerId` that
names another worker is routing data for an event or handoff, not permission to
borrow that worker. Owner accessors retain the narrowest existing visibility;
they are not promoted into a runtime API.

Because a process-global Main is `Sync` while its standard `RefCell` entries
are not, a cross-crate direct-index accessor is `unsafe`: the caller must be the
unique runtime execution thread for that index for the returned borrow's full
lifetime. `DataPlaneMain` encapsulates this requirement for Buffer operations;
Session Node and transport call sites state it locally. This keeps the unsafe
condition explicit without adding a capability type or access wrapper.

### D3: Construct State Before Launch

Module init receives the finalized worker count, constructs all worker values,
and publishes its Main only after construction succeeds. Worker-init callbacks
may attach File readiness, Node runtime data, timers, or protocol registration
to the existing current entry. They do not create, install, replace, take, or
clear the entry.

Shutdown stops scheduling, joins workers, and then drops owner collections in
dependency order. There is no owner-thread clear pass. A resource whose `Drop`
must run on its OS thread stays in that thread's lifecycle owner; it is not put
in a process-global worker vector.

### D4: Delete Generic Main-to-Worker Execution

Delete `DataRemoteLocalQueue`, its global table and state, all queue methods,
`schedule_on_worker`, startup attachment and close logic, and
`poll_remote_local_tasks`. Remove `[worker.control]` because its only consumer
is the deleted queue. `DataPlaneHandoff` remains limited to packet ownership
transfer.

### D5: Keep Each Operation on Its Existing Owner Path

`Connect`, `ConnectStream`, and `AcceptedReply` use each target
`SessionWorker::session_evt_q` and are dispatched by `SessionQueueNode`.
`SessionEvt::control_data` carries the accepted-reply Application identity and
result without adding a new generic event type. The fixed Session event codec
therefore grows from 16 to 24 bytes, and the attach protocol version advances
from 4 to 5 so daemon and SDK rebuild atomically.

Per-Application MQ readiness is not a Session lifecycle event. Attach creates
one `ApplicationWorkerMq` per Data Worker and registers each queue's File with
that worker's polling index. The File callback appends the Application identity
to the current Session worker's pending queue and marks the existing
App-Session input Node pending. Snapshot draining preserves `pending` while the
queue remains nonempty, including error paths, so a producer cannot lose a
wakeup while adding work during the drain.

Application detach runs on the Main Thread inside `WorkerBarrier`, unregisters
the MQ Files before releasing their pointer-owning boxes, and directly walks
all Session worker slots while workers are stopped. No attach/detach Session
event is added. Enqueue acceptance is not operation completion, and main-thread
code does not wait on `std::sync::mpsc::recv` or spin on a generic runtime
queue.

### D6: Keep VPP-Specific Synchronization Where VPP Has It

The thread-zero IP reassembly Process iterates the existing per-thread
`SpinLock<IpReassemblyWorker>` collection, locks one entry, expires its paced
range, releases retained Buffer chains, unlocks, and advances. This lock is
specific to VPP's reassembly Process walk and does not justify locks around
ordinary Session, transport, protocol, throttle, or Buffer worker state.

### D7: Delete Generic Runtime Metrics

Delete `hammer-runtime/src/metrics.rs`, its `lib.rs` module and re-exports, its
module-local tests, and the workspace/runtime `metrics` and `metrics-derive`
dependencies when no remaining crate uses them. Do not replace the registry.

This does not delete statistics owned by `NodeMain`, Session, Buffer, TCP, or
another subsystem. Those facts remain in owner storage and are exposed through
the Stats Segment or owner-defined diagnostics. `CongestionMetrics` is a TCP
connection-domain snapshot, not part of `hammer_runtime::metrics`, and remains.

### D8: Move Network Values to Service

Move `Network` and `SocksAddr` unchanged from
`hammer_runtime::network` to `hammer_service::net`. Preserve `Network` variants,
serde names, `Default`, `as_str`, and `Display`; preserve `SocksAddr` fields,
constructors, `destination_host`, and `Display`.

Delete runtime's `network` module and root re-export. Do not leave a type alias
or compatibility re-export. In-tree code has no current consumer; downstream
Rust callers change imports to `hammer_service::net::{Network, SocksAddr}`.
Serialized `Network` values remain compatible because their representation does
not change.

## Caller Migration

| Owner/caller | Direct current-worker access | Cross-thread path |
| --- | --- | --- |
| Session Node and callbacks | unsafe direct borrow by `runtime.thread_index()` with a local exclusivity proof | enqueue an existing Session control event to the target `session_evt_q` |
| TCP nodes/timers | `tcp_main().worker(runtime.thread_index())?` plus the current Session worker borrow | Session `Connect` dispatch invokes TCP on that worker |
| UDP nodes/cleanup | `udp_main().worker(runtime.thread_index())?` | Session event dispatch; no `schedule_on_worker` |
| TLS callbacks | current thread indexes the existing TLS worker collection and borrows its connection Pool | callback already runs on the selected Session worker |
| HTTP callbacks | `http_main().worker(runtime.thread_index())?` | callback already runs on the selected Session worker |
| QUIC callbacks/timers | `quic_main().worker(runtime.thread_index())?` | Session `Connect`/`ConnectStream` events invoke QUIC |
| IP ICMP throttle | current thread indexes `icmp_throttle` directly | none; throttle state is not remotely borrowed |
| IP reassembly Process | packet nodes use their current entry; main Process walks locked entries | direct locked Process walk |
| Buffer allocation/free | `DataPlaneMain` encapsulates unsafe direct cache borrowing for its immutable thread index | Buffer handoff transfers packet ownership, not cache access |

## 变更清单

### 新增

| 类别 | 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- | --- |
| 类型 | `Network`, `SocksAddr` | `hammer_service::net` | 从 runtime 原样移动；字段、variants、derive 和 serialization contract 不变 | 新 canonical import path；无 runtime compatibility re-export | all-target compile; serde/display not run |
| 类型 | `ApplicationWorkerMq` | `hammer-service::session::application` | one stable boxed entry owns Application id, worker id, queue, File registration, Node id, and pending state | replaces the Session-worker-local MQ entry | all-target compile; behavior not run |
| API | `Network::{as_str,fmt}`, `SocksAddr::{ip,domain,destination_host,fmt}` | `hammer_service::net` | callable signatures and behavior move unchanged | downstream imports migrate to service | all-target compile; behavior not run |
| API | `ApplicationMain::{application_mq,connection,listener}` | `hammer-service::session::application` | return direct references to existing owner state | crate-internal callers migrate; no closure facade | all-target compile |
| API | `DataPlaneMain::trace_control` | `hammer-runtime::data_plane` | expose the existing optional trace control handle for worker-main construction | additive Rust API | all-target compile |

### 修改

| 类别 | 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- | --- |
| 类型 | `SessionMain::workers`, `SessionWorkerSlot` | `hammer-service::session` | replace installable slots with a boxed slice of complete `SessionWorker` borrow cells plus one existing event queue per entry | crate-internal break; no persistence | all-target compile; behavior not run |
| 类型 | `TcpMain::workers`, `UdpMain::workers`, HTTP/QUIC worker collections | owning plugins | existing boxed slices contain complete worker values before launch; meaningful cache-line slots remain | plugins rebuild atomically | all-target compile; behavior not run |
| 类型 | `TlsWorkers::workers` | TLS plugin | each entry directly contains a preconstructed connection `Pool` borrow cell | install errors disappear | all-target compile; behavior not run |
| 类型 | `Ip4Main::icmp_throttle`, `Ip6Main::icmp_throttle` | IP plugin | existing vectors directly contain preconstructed throttle borrow cells | no wire migration | all-target compile; behavior not run |
| 类型 | `BufferPool::workers` | `hammer-core::buffer` | Pool-owned cache array directly contains complete borrow cells constructed by `BufferMain::new` | remains in core, not runtime | all-target compile; behavior not run |
| 类型 | `ApplicationMqResources` | `hammer-service::session::application` | queue array becomes stable boxed worker entries; File deletion precedes entry release and pending state survives nonempty/error drains | daemon/SDK rebuild; no persistence | all-target compile; failure paths reviewed |
| 类型/wire | `SessionEvt` and Session event codec | `hammer-core::session`, `hammer-runtime::app` | add `control_data: u64`; fixed codec grows 16 to 24 bytes; no new `SessionEvtType` variant | attach protocol 4 to 5; atomic daemon/SDK upgrade | all-target compile; codec behavior not run |
| 类型 | `SessionConnectEndpoint` | `hammer-runtime::session` | `connection: u32` becomes `Option<u32>` and `app: Option<u32>` is added for owner-defined chained Session connects | all constructors and transports rebuild | all-target compile; behavior not run |
| 类型/error | `QuicListenerError` | QUIC plugin | add a private lower-transport-connect cleanup variant that preserves both the primary UDP connect error and a later QUIC context cleanup error | no public error ABI or persistence change | all-target compile; failure path reviewed |
| 类型 | `IpReassemblyMain` | IP plugin | expiry changes from worker closure dispatch to thread-zero Process locked walk | packet handoff unchanged | all-target compile; behavior not run |
| 类型/配置 | worker runtime config | `hammer-runtime::config` | remove `[worker.control]`; retain handoff and Session queue config | old key is rejected by typed config | all-target compile; parsing not run |
| API | owner `worker(thread_index)` accessors | Session/TCP/UDP/TLS/HTTP/QUIC | return direct standard guards; cross-crate `SessionMain::worker` is unsafe with a current-thread exclusivity contract | all callers migrate; no wrapper API | all-target compile and call-site audit |
| API | worker-init functions | affected owners | configure the preconstructed current entry instead of installing worker state | init order remains | all-target compile; startup not run |
| API | transport connect callbacks | `hammer-service::transport` | `TransportConnect` and `TransportConnectStream` receive `&mut SessionWorker`; Session event dispatch invokes them on the selected worker | transport plugins rebuild | all-target compile; behavior not run |
| API | `hammer_plugin_udp::connect` | UDP plugin | expose the existing concrete UDP connect callback so the dependent QUIC plugin can invoke its owner directly on the current Session worker; no generic dispatch helper is added | additive public visibility; callers must pass the endpoint's owning `SessionWorker` | all-target compile; QUIC caller audit |
| API | Session Application lifecycle | `SessionMain`, `ApplicationMain` | connect/reply use Session events; MQ File install binds each worker directly; detach walks worker slots under barrier | no generic runtime queue | all-target compile; behavior not run |
| API | `BufferMain::borrow_worker_caches` | `hammer-core::buffer` | remove lazy install and make direct cache borrowing unsafe with a unique-runtime-thread contract | core/runtime callers migrate | all-target compile and call-site audit |

### 删除

| 类别 | 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- | --- |
| 类型 | `ThreadOwned<T>`, `ThreadOwnedError` | `hammer-infra::thread_owned` | delete empty/install/OS-thread/clear abstraction and export | consumers migrate atomically; no replacement type | workspace compile and symbol audit |
| 类型 | `DataRemoteLocalQueue`, state and error types | `hammer-runtime::spawn` | delete arbitrary main-to-worker closure queue | no alias or facade | runtime compile and symbol audit |
| 类型 | `MetricKind`, `MetricLabel`, `MetricSample`, `MetricsRegistry`, `MetricsScope`, `MetricCounter`, `MetricGauge`, `RegistryRecorder`, private metric key/recorder adapters | `hammer-runtime::metrics` | delete the complete generic registry model | public breaking removal; no replacement | public API and dependency audit |
| 类型 | `WorkerControl` | runtime config | delete generic worker queue config | breaking TOML cleanup | all-target compile and symbol audit |
| 类型 | `AppRxMqEntry` | `hammer-service::session::runtime` | delete the Session-worker-local MQ/File/pending record | replaced by Application-owned `ApplicationWorkerMq` | all-target compile and symbol audit |
| API | every `with_worker`/`with_worker_mut` closure accessor | Session/TCP/UDP/TLS/HTTP/QUIC | delete closure parameters and nested closure composition | callers hold direct current-worker borrows | all-target compile and symbol audit |
| API | `ApplicationMain::{with_application_mq,with_connection,with_listener}` and `SessionMain::{with_listener,with_listeners_mut}` | `hammer-service::session` | delete caller-supplied state-access closures | use direct owner references and the barrier-scoped listener Pool borrow | all-target compile and symbol audit |
| API | `ThreadOwned::{new,install,borrow_mut,clear}` | `hammer-infra` | delete with the type | no compatibility API | symbol audit |
| API | `schedule_on_worker`, queue install/attach/push/drain/close, `poll_remote_local_tasks` | `hammer-runtime` | delete generic worker execution and loop stage | callers use Session event or Process walk | worker loop integration |
| API | `schedule_worker_task` and blocking worker `mpsc` replies | Session owner and transport callers | delete synchronous cross-thread closure wrapper | completion becomes owner event state | all-target compile and symbol audit |
| API | `ApplicationMain::reclaim_connection` | `hammer-service::session::application` | delete the obsolete reclaim operation; connected entries are reaped by the Application owner | internal callers removed | all-target compile and symbol audit |
| API | all `hammer_runtime::metrics` constructors, registration, update, snapshot, recorder methods, and public re-exports | `hammer-runtime` | delete complete API surface | downstream users must use owner stats; no deprecation | workspace compile and public API audit |
| 类型 | `hammer_runtime::network::{Network,SocksAddr}` | `hammer-runtime` | delete old definitions and module path after move | breaking Rust import; use `hammer_service::net` | no runtime definition/re-export remains |
| API | runtime-root `Network`/`SocksAddr` re-exports and their old method paths | `hammer_runtime::{Network,SocksAddr}` | delete old public path without alias | downstream import migration required | downstream-style compile fixture |
| API/error | generic worker-control `RuntimeError` variants and ThreadOwned-derived owner errors | runtime and affected owners | delete queue unavailable/closed/full/canceled and installed/wrong-thread/access variants | concrete owner queue/lifecycle errors remain | all-target compile and symbol audit |
| error variant | `SessionQueueError::ApplicationMqAlreadyRegistered` | `hammer-service::session` | delete with the Session-worker-local MQ installation path | `ApplicationMain` owns one MQ resource set per Application | all-target compile and symbol audit |
| dependency | `metrics`, `metrics-derive` | workspace and `hammer-runtime/Cargo.toml` | remove when scoped search after code deletion finds no consumer | lockfile updates normally | `cargo metadata` and workspace compile |

There is no persisted-data migration. Removing `[worker.control]` is a startup
configuration break. The Session event wire-size change requires attach
protocol version 5 and an atomic daemon/SDK upgrade. Rust ABI consumers must be
rebuilt; no compatibility layer is accepted.

## Rollout Order

1. Preconstruct Buffer Pool caches and each owner worker collection using its
   existing Vec/slice/Pool representation.
2. Change owner `worker(thread_index)` methods to return direct borrows and
   migrate every closure accessor call site.
3. Change worker-init callbacks from state installation to configuration of the
   existing current entry.
4. Bind Application MQ Files directly to their workers; route connect and
   accepted-reply work through the existing Session event path; detach under
   `WorkerBarrier` without adding lifecycle events.
5. Replace IP reassembly worker scheduling with the VPP Process walk.
6. Delete `DataRemoteLocalQueue`, worker-loop polling, worker-control config,
   generic runtime errors, and `ThreadOwned`.
7. Delete `hammer_runtime::metrics`, its exports, and unused dependencies.
8. Move `Network` and `SocksAddr` to `hammer_service::net`, migrate imports,
   then delete the runtime module and re-exports.
9. Rebuild daemon, SDK, and plugins atomically and remove the obsolete config
   key.

Rollback is commit-level before deployment. Reintroducing an install slot,
generic worker closure, generic per-thread type, closure accessor, or generic
metrics registry is not an accepted fallback.

## Verification Matrix

| Command/audit | Required result | Decisions | Limitation |
| --- | --- | --- | --- |
| `cargo fmt --all -- --check` | all changed Rust and documentation-adjacent source is formatted | D1-D8 | no behavior coverage |
| `git diff --check` | no whitespace errors | D1-D8 | no behavior coverage |
| `cargo check --locked --workspace --all-targets` | every workspace crate, target, example, benchmark, and test target type-checks against the changed APIs and wire structs | D1-D8 | does not execute startup, queues, barriers, codecs, or transports |
| scoped `rg` symbol/dependency audit | removed containers, worker closure queue, closure accessors, runtime metrics, and runtime network exports are absent | D1, D2, D4, D7, D8 | textual absence only; not behavior proof |

Per explicit delivery constraints, no test command is added or run. Worker
isolation, Application MQ wakeup/failure behavior, Session event routing,
transport connect completion, reassembly expiry, Buffer cache behavior, and
Network serde/display behavior therefore remain behaviorally unverified in
this change set.

## 未决问题

无。Vendored VPP and the inspected Hammer baseline settle storage ownership, direct access,
dispatch, Buffer cache placement, metrics deletion, Network value placement,
and caller migration.
Implementation may choose plain `Vec<T>`, a Vec of existing standard borrow
cells, a boxed slice, or the existing `Pool<T>` according to each owner's
current borrow requirements; that local representation does not justify a
common type or API.

## 依据与假设

- `已验证（当前项目）`: H1-H9 describe the pre-change baseline established
  from the repository diff; the changed owners and named callers were inspected
  on the current worktree.
- `已验证（vendored VPP）`: V1-V7 were inspected under `third_party/vpp/`.
- `设计决定`: direct owner borrows and reuse of the existing Session event path
  are Rust adaptations of VPP's vector lookup and Session worker queue.
- `推断`: retained cache-line slot structs continue to carry meaningful layout;
  no runtime layout measurement was added or executed in this compile-only
  change.
- `需要更多历史记录验证`: none for this decision. Older ADR text remains
  historical context only where ADR-0010 explicitly supersedes it.

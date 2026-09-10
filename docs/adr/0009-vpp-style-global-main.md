# VPP-Style Global, Thread, Unix, and Per-Thread Runtime Authorities

Status: proposed

Date: 2026-09-09

This proposed ADR records the runtime owner design and the user's subsequent
corrections. The interview is closed; remaining technical checks become
executable issue tasks, not further grill questions. This documentation and
issue-planning turn does not implement runtime code. The scope covers
`GlobalMain` / `ThreadMain` / `WorkerThread` / `DataPlaneMain`, Unix startup,
registration handoff, and main-thread Process scheduling.

### 当前设计优先级

后续任务以目标 Rust 设计及用户最新修正为准。下文 Q/F 记录保留历史来源，
不再作为互相冲突的备选方案。最新约束包括：删除 `Worker` 聚合配置类型但
保留 TOML 键；使用 `register_image`；IPC listener 留在 daemon IPC owner，
不放进 GlobalMain 或 UnixMain；DataPlaneMain 使用一个 `FileMode` 字段；
ThreadMain 的 CPU/NUMA 集合保留且复用 `Bitmap`。

调用进度的裸指针集合设计已撤回。`init_functions_called` 和每个 main 的
`worker_init_functions_called` 改用现有 `Bitmap`，bit 表示注册期确定的
稳定回调索引。这个索引到 callback identity 的映射是待实现并验证的注册
职责，当前 Hammer 尚无该映射；不得声称 `topological_order` 返回的临时
列表位置就是稳定 identity。注册任务必须证明重复 image、跨 list 的相同
callback、early/ordinary config 和后续插件加载不会漏调或重复调用，再
实施该表示。不新增公共 identity 包装类型，不把地址 cast 成整数当 bit
索引，也不把名字去重冒充 VPP 的 callback identity。

Within this design, the confirmed owner changes replace the conflicting
GlobalMain ownership descriptions in ADR-0001, ADR-0002, and ADR-0003: a
GlobalMain-owned main container, scheduler, plugin owner, worker lifecycle, or
refork completion is no longer the target. Their independent scheduling,
barrier, and refork contracts still apply unless this ADR explicitly identifies
a remaining policy decision. Those older documents have not been rewritten by
this documentation-only update.

## Context

The current Hammer `GlobalMain` combines several authorities: one owned
`DataPlaneMain`, `ControlThread`, `RuntimeRegistry`, worker publication and
coordination state, plugin loading, IPC listener state, lifecycle orchestration,
and several independent callback-called sets. The current implementation is
visible in `crates/hammer-runtime/src/global_main.rs` and its `global_main/`
submodules.

Vendored VPP separates the process-wide `vlib_global_main_t` from each
thread-local `vlib_main_t`. `vlib_global_main_t` contains the per-thread main
lookup vector, process metadata, node registrations, seven distinct hook
registration lists, and `init_functions_called`; it does not become the
worker's packet execution object. VPP hook callbacks receive `vlib_main_t *`.
VPP additionally has process-global thread administration in
`vlib_thread_main_t` and per-OS-thread descriptions in `vlib_worker_thread_t`.
In current Hammer, `worker_thread.rs` only implements platform setup on the
configuration type `Worker`; neither `ThreadMain` nor `WorkerThread` exists.

VPP also keeps Unix host-process state in an independent `unix_main_t` and
enters the main thread through `vlib_unix_main(argc, argv, startup_config)`.
That state includes the Unix-facing control socket, startup/runtime paths,
signal/error state, and terminal/logging policy. Current Hammer spreads the
corresponding concerns across `hammer/src/main.rs`, GlobalMain's control/file
fields, and the global FileMain; it has no UnixMain authority.

The primary references are:

- `third_party/vpp/src/vlib/main.h:267` for `vlib_global_main_t`;
- `third_party/vpp/src/vlib/main.h:72` for `vlib_main_t`;
- `third_party/vpp/src/vlib/init.h:19` and `:33` for hook callback inputs;
- `third_party/vpp/src/vlib/global_funcs.h:14` for global-main access;
- `third_party/vpp/src/vlib/init.c:298-455` for distinct registration-list
  dispatch, including config's shared `init_functions_called` progress;
- `third_party/vpp/src/vlib/threads.h:46` and `:191` for WorkerThread and
  ThreadMain respectively;
- `third_party/vpp/src/vlib/threads.c:598-765` for worker main publication
  into `vlib_mains`;
- `third_party/vpp/src/vlib/unix/unix.h:15` and
  `third_party/vpp/src/vlib/unix/main.c:677` for UnixMain state and
  `vlib_unix_main` startup;
- `third_party/vpp/src/vlib/unix/plugin.c:13` for independent plugin-main
  ownership.

## Confirmed Decisions

1. Alignment means semantic and ownership equivalence with VPP, not mechanical
   C ABI layout equivalence.
2. `GlobalMain` is one process-global authority. The design does not add a
   daemon-owned ordinary instance, a `OnceLock<GlobalMain>` wrapper, a fixture
   constructor, or a test-only construction path.
3. `DataPlaneMain` remains owned by its executing thread. GlobalMain does not
   add a reference table, pointer field, address-installation method, or owner
   forwarding surface merely to reproduce VPP's C pointer lookup. Indexed main
   access is an owner-local runtime operation and reuses `DataPlaneMain` values
   directly.
4. Startup constructs worker mains before launching OS threads and transfers
   each value once to its executing owner for the process lifetime. Ordinary
   owner-thread access remains an explicit borrow. Cross-worker publication or
   mutation still follows the Worker Barrier; GlobalMain does not provide a
   main lookup table or pointer relation.
5. `PluginMain` is an independent authority. Plugin image, metadata, load
   order, and loaded-code lifetime are not folded into the GlobalMain authority.
6. Init, config, main-loop-enter, and main-loop-exit hooks receive the
   `DataPlaneMain` belonging to the execution thread, matching VPP callbacks'
   `vlib_main_t *` input.
7. The IPC listener is removed from this authority. The remaining behavior is
   aligned against VPP according to actual VPP use, not by retaining current
   Hammer convenience surfaces.
8. `ControlThread` is removed from GlobalMain. `RuntimeRegistry` is also
   removed from GlobalMain and DataPlaneMain. Their precise replacement owner
   remains an interview item.
9. GlobalMain retains the seven VPP-equivalent registration inventories:
   init, main-loop-enter, main-loop-exit, worker-init, worker-count-change,
   API-init, and config. These are seven fields of GlobalMain, not seven new
   inventory-owner objects. It also retains `init_functions_called`.
10. Worker-thread state, Worker Barrier state, graph-refork request, and
    graph-refork completion accounting belong to the worker-thread authority.
    GlobalMain does not retain graph-refork state.
11. The following current GlobalMain orchestration methods are removed from
    GlobalMain and moved to their owning runtime or daemon authority:
    `main_loop_enter`, `configure_early`, `load_plugins`, `init_control`,
    `start_process_nodes`, `process_handle`, `run_processes_until`,
    `shutdown_process_nodes`, and `close`.
12. The registration declarations are process-global variables. `PluginMain`
    is responsible for obtaining their direct references from each plugin image
    and supplying them to the registration owner; it must not create a copied
    plugin-owned registration universe.
13. The final ownership of control scheduling and registry state follows VPP's
    actual ownership model. No Hammer-specific owner is selected merely to
    preserve the current `GlobalMain` aggregate.
14. Public GlobalMain access follows VPP's global-main access semantics and
    does not preserve convenience getters that cross ownership boundaries.
15. Registration dispatch follows VPP's actual per-list lifecycle and progress
    semantics. It is not implemented as one merged list or as independent
    Hammer-only called sets.
16. The process-thread `DataPlaneMain` occupies index zero, matching VPP's
    first main. Its owner supplies access; GlobalMain does not store a second
    ownership or pointer relation for it.
17. `RuntimeRegistry` has no VPP global-main counterpart. Its final treatment
    must follow actual VPP owner boundaries rather than preserve a generic
    GlobalMain/DataPlaneMain registry field.
18. PluginMain is responsible for handing GlobalMain direct references to the
    process-global registration declarations. GlobalMain consumes the
    registrations; PluginMain does not create a copied second registration
    universe.
19. GlobalMain's public access follows the VPP global-main access model. The
    current convenience getters and control-capability forwarding methods are
    not retained merely for source compatibility.
20. Registration dispatch uses VPP's actual call sites and per-list state:
    global init progress for global callbacks, per-DataPlaneMain worker init
    progress for worker callbacks, and barrier-protected worker-count change
    dispatch. Both early and ordinary config use GlobalMain's
    `init_functions_called`, keyed by callback identity and marked before
    invocation; config is not tracked by a separate called set or by clearing
    its function pointer. The actual `init.c` dispatcher establishes this,
    despite the older comment on `vlib_config_function_runtime_t.function`.
21. Successfully installed plugin images remain live for the process lifetime
    while GlobalMain may retain direct references to their declarations. A
    failed, unpublished load may release its image according to the plugin
    transaction's failure path.
22. Every current GlobalMain responsibility that is outside the VPP global-main
    authority is migrated to the corresponding owning authority. No displaced
    responsibility remains reachable through a compatibility forwarding method.
23. `PluginMain::collect_registrations` and the other PluginMain methods that
    materialize copied registration `Vec`s are removed. Registration ownership
    is handed over through direct references to the image declarations.
24. `RegistrationImage` remains only an image ABI carrier. It is not a second
    registration owner, dispatch state, or plugin-local replacement for
    GlobalMain's process-wide inventories.
25. Per-thread main ownership and `ThreadOwned<T>` are separate concepts.
    `ThreadOwned<T>` remains a generic owner-thread access rule; GlobalMain does
    not add a second pointer/table abstraction for per-thread mains.
26. `ThreadOwned<T>` is a generic per-thread access abstraction. It gives the
    owner thread exclusive mutable access and gives a non-owner only shared
    access where the value contract permits it. It has no `install` operation,
    atomic publication state, cross-thread synchronization, or lifecycle
    `clear` protocol. Its contract is independent of the worker-index relation.
27. `start_workers` follows VPP's startup order: establish thread zero and
    worker-main lookup entries before launch, launch workers, wait for their
    startup-barrier acknowledgement, run worker-count-change callbacks while
    that barrier is held, and release it. On VPP's ordinary Data Worker path,
    `vlib_worker_thread_init` waits at that barrier before the worker initializes
    its clock, timers, file poller, and worker-init callbacks. Worker-init
    callbacks therefore run after that release. The main-loop's later initial
    barrier remains in the main-loop owner. This corrects the previous source
    ordering description; the `use_pthreads` bypass is a separate VPP branch.
28. Because `DataPlaneMain` is thread-bound, its current state is accessed from
    the executing owner through ordinary Rust borrows. This design does not
    introduce a raw-pointer lookup facade or a public cross-thread mutable
    access path.
29. A registered worker `DataPlaneMain` remains live for the process lifetime,
    matching VPP's `vlib_mains` lifecycle. `start_workers` does not support
    independently stopping or restarting a worker after its main has been
    published.
30. Split thread responsibilities explicitly, as requested in follow-up Q2:
    `ThreadMain` corresponds to `vlib_thread_main_t`, `WorkerThread` to
    `vlib_worker_thread_t`, and `DataPlaneMain` to `vlib_main_t`; GlobalMain
    continues to correspond to `vlib_global_main_t`. ThreadMain administers
    thread registrations, counts, and placement/scheduling configuration.
    WorkerThread describes an OS thread and its synchronization role, including
    the main thread at index zero. Neither is a replacement name for a combined
    GlobalMain aggregate. Exact Rust fields and access signatures remain open.
31. Follow-up Q3 confirms allocating each worker DataPlaneMain before OS-thread
    launch and transferring the owning value once to the corresponding worker
    for the process lifetime. The transfer uses the existing DataPlaneMain
    construction path; it does not require a GlobalMain address table or an
    arbitrary `Send` implementation.
32. Follow-up Q4 confirms removing DataPlaneMain's shared RuntimeRegistry,
    all-worker control-queue/configuration forwarding, graph-publication
    completion, and shared GlobalMain exit forwarding. Per-main execution
    facts remain with DataPlaneMain: VPP itself stores main-loop exit fields,
    worker-init progress, loop counters, file-poll state, and barrier timing
    in `vlib_main_t`. These must not all be moved into WorkerThread merely
    because their names mention control, threads, or barriers.
33. Follow-up Q5 answers only “对齐vpp”. It does not approve retaining
    ControlThread or RuntimeRegistry as arbitrary standalone owners. Process
    scheduling/state must follow VPP's main-loop and NodeMain ownership;
    concrete subsystem dependencies must follow their actual owners. Moving
    the generic registry to ThreadMain, WorkerThread, or PluginMain would not
    satisfy this decision. The placement of Hammer's scheduler mechanism and
    remaining registry consumers must be derived explicitly.
34. Follow-up Q6 confirms the four allowed main-access semantics and deletion
    of broad forwarding/TLS facades. It does not approve four zero-argument
    Rust functions returning unconstrained references. Owner borrows, barrier
    phases, reentrancy, and lifetime rules must constrain any final signature.
35. Follow-up Q7 explicitly supplies the seven registration fields from
    `vlib_global_main_t`. Their owner and existence are settled. The first six
    share `_vlib_init_function_list_elt_t`; config alone uses
    `vlib_config_function_runtime_t`. ThreadMain's thread-registration list is
    separate. Exact Rust declaration linkage and image handoff still require
    a concrete design, but the seven fields must not be presented as an open
    ownership question again.
36. Follow-up Q1 locks the existing confirmed decisions as the baseline.
    Follow-up Q2-Q7 extend or clarify that baseline as recorded above. New
    interview questions concern only unresolved Hammer policy or a concrete
    Rust design choice; VPP source facts are established by inspection.
37. Follow-up Q8 requires complete VPP thread-registration coverage. ThreadMain
    and WorkerThread therefore cover both Data Worker threads with a
    DataPlaneMain clone and auxiliary registered OS threads whose registration
    has `no_data_structure_clone`; the latter still have WorkerThread
    descriptions but no DataPlaneMain execution state.
38. Follow-up Q9 selects VPP's worker-init failure behavior: report the
    callback error and enter the worker loop, without failing process startup
    or starting a worker-restart/reclaim path. The callback identity remains
    marked before invocation and later callbacks in that dispatch pass are not
    called after the returned error. Hammer's current startup error propagation
    is consequently removed; its reporting mechanism and typed error boundary
    remain to be specified.
39. Follow-up Q10 preserves the existing TOML keys, aliases, and defaults.
    Owner splitting is an internal runtime migration; it does not create a
    serialized schema migration or require callers to rewrite configuration.
40. Follow-up Q11 confirms that the VPP `no_data_structure_clone` distinction
    is part of the thread-registration design. Auxiliary registered OS threads
    have WorkerThread descriptions without DataPlaneMain state, and their entry
    functions are separate from DataPlaneMain lifecycle hooks.
41. Follow-up Q12 selects VPP's control flow after worker-init failure: the
    worker reports no stored or persisted error for now and continues into its
    worker loop. An owner-local typed error is still required as a defined
    domain value; its later reporting, retention, and consumer contract are
    explicitly deferred and must not be invented in this change.
42. Follow-up Q13 preserves the existing Worker TOML document. The document
    is parsed once, and its field groups are projected into their actual
    owners without duplicate serialized schemas or compatibility-breaking keys,
    aliases, or defaults.
43. Follow-up Q14 makes UnixMain an independent process-global host authority
    owned by `hammer-runtime`, parallel to GlobalMain and ThreadMain. The daemon
    invokes its startup entry; GlobalMain does not absorb Unix host state.
44. Follow-up Q15 includes the existing Hammer equivalents of Unix-facing
    state in UnixMain, including the control listener, startup/runtime paths,
    signal/exit, and logging policy. Productless VPP interactive CLI/pager
    state is not invented solely for naming alignment.
45. Follow-up Q16 assigns UnixMain the host startup sequence and main-thread
    entry corresponding to `vlib_unix_main`. GlobalMain retains only its VPP
    global authority, while ThreadMain, WorkerThread, and DataPlaneMain join
    the sequence through their own lifecycle boundaries.
46. Follow-up Q17 requires the UnixMain entry to use Rust ownership,
    lifetimes, and explicit borrows rather than reproducing C global/TLS
    access. The exact entry signature and installation lifetime remain open,
    but a public raw-pointer or TLS facade is excluded.
47. Follow-up Q18 assigns Process scheduling to the thread-zero
    DataPlaneMain/NodeMain path. UnixMain owns host startup; an independent
    ControlThread scheduling authority is not retained merely to preserve the
    current Tokio wrapper.
48. Follow-up Q19 removes the generic RuntimeRegistry from GlobalMain,
    DataPlaneMain, UnixMain, and the init macro injection chain. Dependencies
    and published state must cross boundaries through explicit APIs owned by
    the subsystem that defines them; the exact migration of each consumer
    remains open.
49. Follow-up Q20 clarifies that UnixMain remains a process-global authority
    and its storage is not changed to a local value by the Rust API decision.
    “More Rust” applies to the callable API, ownership boundaries, lifetimes,
    and borrows; it does not authorize replacing the global authority with a
    local variable design.
50. Follow-up Q21 requires Tokio for Process scheduling. The scheduler moves
    with the thread-zero DataPlaneMain/NodeMain owner, while the exact Tokio
    runtime and statically dispatched Process representation remain open.
51. Follow-up Q22 requires the RuntimeRegistry replacement to follow VPP:
    lifecycle hooks receive their execution main and concrete owner-defined
    dependencies; no generic typed registry injection or publication carrier
    is retained.
52. Follow-up Q23 confirms that UnixMain's storage remains global while its
    API uses Rust-safe access and lifecycle operations. UnixMain does not gain
    a current-thread lookup surface; current-main lookup belongs to the
    DataPlaneMain relation.
53. Follow-up Q24 requires Process scheduling ownership to be derived from VPP
    source. VPP stores each main's Process Node table, restore queues,
    suspended frames, and current-process identity in `vlib_node_main_t`, which
    is nested in `vlib_main_t`; `vlib_main_or_worker_loop` starts and resumes
    Process Nodes only on the main path. Hammer keeps Tokio, but the scheduler
    state follows the thread-zero DataPlaneMain/NodeMain owner boundary.
54. Follow-up Q25 reconfirms that RuntimeRegistry removal and explicit
    init/config dependencies follow VPP. The exact concrete dependency and
    image-handoff signatures remain design work, not permission to recreate a
    generic carrier.
55. Follow-up Q26 corrects the access premise: VPP's indexed main lookup does
    not require every lookup to be inside a Worker Barrier. The barrier
    synchronizes worker stop and worker-visible publication or mutation; it is
    not a prerequisite for obtaining a main reference. Hammer must preserve
    that distinction while still preventing an escaped mutable borrow or an
    unsynchronized mutation of a running worker's state.
56. Follow-up Q27 confirms that Process scheduling keeps Tokio and follows the
    VPP owner boundary: Process state and dispatch belong to the thread-zero
    DataPlaneMain/NodeMain path.
57. Follow-up Q28 confirms direct VPP-aligned plugin image handoff. PluginMain
    retains image/code lifetime authority, while registration owners receive
    direct declaration references; copied registration universes and generic
    registry carriers are removed.
58. Follow-up F22 selects the Rust access boundary for the four main lookup
    semantics: `global`, `first`, and `by_index` expose shared borrows, while
    mutable access to `DataPlaneMain` is available only from its current owner.
    This preserves VPP's lookup semantics without granting cross-worker mutable
    access through an indexed lookup.
59. Follow-up F23 selects the Tokio callback model, with one correction to the
    proposed shape: `ProcessContext` is deleted. A Process callback receives
    only its owner `DataPlaneMain`; Tokio remains the executor for the async
    callback, and Process state remains with thread-zero DataPlaneMain/NodeMain.
60. Follow-up F24 selects the borrow boundary for that async callback:
    `DataPlaneMain` is borrowed only for one callback/step and the returned
    future must not retain that borrow across `.await`. Process state that must
    survive suspension belongs to the NodeMain Process state.
61. Follow-up F25 requires the Process event, timer, restore, and suspended
    state boundary to follow VPP. These remain in the `NodeMain` nested in the
    thread-zero `DataPlaneMain`; Process signaling uses an explicit main/node
    owner API, and the standalone generic `ProcessHandle` is removed.
62. Follow-up F26 records the direct VPP consequence for Process identity:
    signal delivery targets the registered Process Node index. A Process name
    remains registration and diagnostic metadata, not a runtime signal handle
    or lookup key.
63. Follow-up F27 selects a concrete Rust `Process` type that implements
    `Future` and is scheduled by Tokio. Process scheduling does not retain the
    current `Pin<Box<dyn Future>>` type-erased alias or replace it with one
    central enum of every plugin Process. NodeMain retains the Process
    scheduling state and lifecycle relationship.
64. Follow-up requests that the Process model be simplified. The proposed
    compact boundary is one `NodeMain` owner and one concrete `Process` Future;
    existing Node registration and `NodeId` provide registration identity.
    `ProcessContext`, `ProcessFuture`, `ProcessHandle`, `ProcessEntry`,
    `ProcessEventBatch`, and `ProcessWake` are not retained as independent
    public runtime types. Event, timer, and restore details remain internal to
    `Process` and `NodeMain`.

## VPP Boundary

The names above describe Hammer roles, not a requirement to copy VPP's C
types. The semantic mapping under interview is:

| VPP fact | Hammer role | Boundary |
| --- | --- | --- |
| `vlib_global_main_t` | `GlobalMain` | Process-wide authority and seven hook inventories |
| `vlib_thread_main_t` | `ThreadMain` | Process-wide thread registrations, counts, CPU/NUMA placement and scheduling policy; no Hammer `Worker` aggregate |
| `vlib_worker_thread_t` | `WorkerThread` | Per-OS-thread description; thread zero participates; worker handshake and completion belong to this thread subsystem |
| `vlib_main_t` | `DataPlaneMain` | Per-thread execution state passed to hooks, including the main thread at index zero |
| `unix_main_t` / `vlib_unix_main` | `UnixMain` | Unix host-process authority and main-thread startup entry, independent from GlobalMain graph/registration ownership |
| VPP per-thread main table | `DataPlaneMain` owner storage | Runtime-thread-index access is supplied by the owner of the per-thread mains; GlobalMain does not add a pointer table or installation API |
| `plugin_main_t` | `PluginMain` | Independent plugin image, direct registration-reference, and code-lifetime authority |
| seven VPP registration lists | Runtime Registration Inventory | Distinct semantic lists obtained through PluginMain's direct references, not one merged called-set model |
| VPP graph-refork request | WorkerThread / WorkerBarrier state | Worker-thread authority owns request, barrier release, and completion |

The three levels contain four types, not a four-step containment chain:

1. **Process-wide authorities:** GlobalMain owns global declarations;
   ThreadMain separately administers thread counts, placement, and scheduling
   fields supplied by the startup parser.
2. **OS-thread descriptions:** WorkerThread describes each launched runtime
   thread, including thread zero, and its participation in synchronization.
3. **Per-thread execution:** DataPlaneMain is the main or Data Worker's
   execution state. Its owner exposes the permitted indexed access; ownership
   does not transfer to GlobalMain or make WorkerThread its graph owner.

VPP does not guarantee equal numbers of WorkerThread descriptions and mains:
`vlib_thread_init` counts only registrations requiring data-structure clones
(`threads.c:332-342`), while `start_workers` creates a description before
checking `no_data_structure_clone` (`:643-669`). The
non-cloning branch skips main allocation. Whether Hammer adds that auxiliary
thread role now is F1; this source fact does not reopen the four owner roles.

VPP's `vlib_get_global_main`, `vlib_get_main`, `vlib_get_first_main`, and
`vlib_get_main_by_index` are the only public access semantics that may justify
a GlobalMain/DataPlaneMain lookup surface. VPP's first main is the control
thread's index-zero `vlib_main_t`; workers occupy subsequent entries in
`vlib_mains`.

### Seven GlobalMain registration fields

The following are the exact vendored VPP fields supplied by the user in Q7
(`main.h:311-321`). Their existence and GlobalMain ownership are confirmed.
The Rust linkage/storage representation is not specified by this C table.

| GlobalMain field | VPP declaration type | Dispatch and progress |
| --- | --- | --- |
| `init_function_registrations` | `_vlib_init_function_list_elt_t *` | `vlib_call_all_init_functions`; global `init_functions_called` |
| `main_loop_enter_function_registrations` | `_vlib_init_function_list_elt_t *` | `vlib_call_all_main_loop_enter_functions`; global `init_functions_called` |
| `main_loop_exit_function_registrations` | `_vlib_init_function_list_elt_t *` | `vlib_call_all_main_loop_exit_functions`; global `init_functions_called` |
| `worker_init_function_registrations` | `_vlib_init_function_list_elt_t *` | Sorted before worker launch; `vlib_worker_thread_fn` dispatches without sorting; per-main `worker_init_functions_called` |
| `num_workers_change_function_registrations` | `_vlib_init_function_list_elt_t *` | `start_workers` dispatches under the startup barrier; global `init_functions_called` |
| `api_init_function_registrations` | `_vlib_init_function_list_elt_t *` | `vl_api_clnt_process` dispatches after API initialization; global `init_functions_called` |
| `config_function_registrations` | `vlib_config_function_runtime_t *` | One list with `is_early`; both passes use global `init_functions_called` |

The six lifecycle lists use one callback shape, `vlib_init_function_t(vm)`.
Config additionally receives its parsed input. Call-once progress records
callback identity before invocation, including when that invocation returns an
error (`init.c:315-331`, `:433-444`). A failed callback is not silently retried
by another pass. The Rust callback identity mechanism is still to be specified;
the current category-local name sets do not establish the same contract.

Thread registrations use `vlib_thread_registration_t` and
`VLIB_REGISTER_THREAD` (`threads.h:23-41`, `:253-270`), which link into
`vlib_thread_main.next`. They do not consume any of these seven fields. Node
registration also has its own `GlobalMain.node_registrations` field. Binary API
method declarations are not themselves API-init callbacks.

### Source trace and corrected ownership

This trace covers the four requested runtime types plus UnixMain and their startup, hook,
loop, refork, plugin-image, and shutdown paths. It is not a claim that every
unrelated subsystem in the vendored VPP tree has been audited.

| ID / source class | Path and symbol | Verified behavior |
| --- | --- | --- |
| V1 / VPP | `src/vlib/main.h:72`, `:267`, `:469`; `src/vpp/vnet/main.c:389` | `vlib_main_init` allocates the first main and appends its address to GlobalMain before `vlib_unix_main`. Per-main execution and process-global registration are separate. |
| V2 / VPP | `src/vlib/threads.h:46`, `:191`; `src/vlib/threads.c:30`, `vlib_thread_init`, `start_workers` | ThreadMain administers registrations/counts/placement. Actual worker descriptions are stored in the separate global `vlib_worker_threads`, including thread zero. The `worker_threads` member declared in ThreadMain has no accesses in the scoped `src` C/header search; it is not evidence for another active worker table. |
| V3 / VPP | `src/vlib/threads.c:598-859`; `src/vlib/main.c:2021-2046` | Worker mains are allocated/cloned before launch. Thread bootstrap establishes OS identity, then the worker entry obtains its own main. Ordinary worker startup waits in `vlib_worker_thread_init`; local clock/timer/file initialization and worker-init callbacks follow release. |
| V4 / VPP | `src/vlib/main.c:1846-2018`, `vlib_main_or_worker_loop`; `src/vlib/unix/main.c:678`, `vlib_unix_main` | Plugin early load and early config precede `vlib_main`; static nodes precede normal init, normal config follows normal init, worker hooks are sorted before enter hooks. Main loop performs a later initial barrier before starting processes. Final exit holds the barrier permanently while exit hooks run. |
| V5 / VPP | `src/vlib/init.h:19-52`, `:90-175`; `src/vlib/init.c:298-455`; `src/vlibapi/api.h:30`; `src/vlibmemory/memclnt_api.c:307-344` | Constructors link declaration objects directly into GlobalMain. Six lifecycle lists share one declaration type. API-init belongs to API Process startup; config uses the shared global progress set. |
| V6 / VPP | `src/vlib/threads.c:867`, `:914`, `:1317`, `:1417`; `src/vlib/threads.h:296-386` | Thread-zero worker state coordinates sync/release/completion. Workers refork from the first main's NodeMain after release while the main waits; the Hammer target places the request with WorkerThread/WorkerBarrier. Per-main clock offsets, barrier timing and debug parked state remain in `vlib_main_t`. |
| V7 / VPP | `src/vlib/unix/plugin.c:13`, `load_one_plugin`, `vlib_plugin_early_init` | `vlib_plugin_main` owns plugin loading independently. `dlopen` runs constructors that link declarations into GlobalMain; the loader does not copy seven registration vectors. Hammer's image-reference handoff is the approved Rust adaptation, not a VPP loader function. |
| V8 / VPP | `src/vlib/global_funcs.h:13-49`; `src/vlib/threads.h:132`; `src/vppinfra/os.h:27`; `src/vlib/file.c:23`, `:82` | Current-main lookup uses a thread index stored in C TLS; indexed lookup returns a C pointer. The File registry is independent, while poller descriptors are per-main. Neither C pointer access nor C TLS is automatically a permitted Rust API. |
| V9 / VPP | `src/vlib/unix/unix.h:15-91`; `src/vlib/unix/main.c:42-49`, `:677-735` | UnixMain is an independent process-global host authority. `unix_main_init` stores the first DataPlaneMain back-pointer; `vlib_unix_main` initializes GlobalMain metadata, startup-config/plugin early phases, thread-zero stack/index, and enters the main thread. Unix-specific flags, control socket, error history, paths, and terminal/log policy remain in UnixMain. |
| V10 / VPP | `src/vlib/node.h:720-780`; `src/vlib/main.c:1215-1435`, `:1460-1665` | Process Node scheduling state is held by `vlib_node_main_t` inside each `vlib_main_t`: process table, restore queues, suspended frames, timer/event state, and current process identity. `vlib_main_or_worker_loop` starts/restores Process Nodes only when `is_main`; workers run packet/handoff/barrier work and do not own the main Process scheduler. |
| H1 / 当前项目 | `crates/hammer/src/main.rs:163`, `run`; `hammer-runtime/src/global_main/metadata.rs`, `lifecycle.rs` | Daemon constructs RuntimeRegistry and GlobalMain, initializes ControlThread/File readiness, supplies the service image, calls GlobalMain lifecycle orchestration, then runs control processes and closes through GlobalMain. |
| H2 / 当前项目 | `hammer-runtime/src/global_main.rs:53`; `data_plane/main.rs:39`; `data_plane/worker.rs:8` | GlobalMain owns one main, scheduler, plugin owner, worker handles, queues and publication state. `install_global_control` copies shared registry/config/exit/coordination references into each main. |
| H3 / 当前项目 | `hammer-runtime/src/start_workers.rs:17`, `:77`, `:156`; `main_loop.rs:19` | Each main is currently constructed inside the launched worker, after the launch barrier. `start_workers` also owns the later barrier. Workers carry a Tokio runtime/control queue alongside packet dispatch; no pre-launch main lookup table exists. |
| H4 / 当前项目 | `hammer-runtime/src/plugin.rs:463`, `:506`; `registration.rs:35`, `__declare_registration_image!` | PluginMain reads RegistrationImage from each DSO. The image macro materializes arrays of declaration values; `collect_registrations` then copies selected slices into another Vec. Current fields lack worker-count-change and API-init hook lists and split early/ordinary config. |
| H5 / 当前项目 | `hammer-component-macros/src/lib.rs:2803`, `expand_registered_function`, `expand_config_function`; `hammer-runtime/src/init.rs:154` | Init/enter/exit adapters receive GlobalMain; worker-init receives DataPlaneMain. Both inject `Arc<T>` through a registry and can automatically publish returned `Arc<T>`. Config inserts its category-local called name only after success. |
| H6 / 当前项目 | `hammer-runtime/src/control_thread.rs:19`, `process.rs:74`; `hammer-service/src/binary_api.rs:651-710`; `hammer-runtime/src/config/stats.rs:45` | ControlThread owns running processes and LocalSet; ProcessContext reads RuntimeRegistry. Binary API config/init publish through the registry, and its Process Node requires that stored value. Stats config/init use the same generated dependency mechanism. |
| H7 / 当前项目 | `hammer-runtime/src/data_plane/buffer_pool.rs:13`, `:107`; `node.rs:1027`; `spawn.rs:141-164` | DataPlaneMain contains Rc/RefCell state and lazily acquires worker Buffer cache borrows. NodeMain constructs its own readiness state from a graph clone. Existing TLS accessors manufacture borrows from a cached raw pointer without establishing reentrancy exclusivity. |
| H8 / 当前项目 | `hammer-runtime/src/init.rs:175-220`; `start_workers.rs:100`, `:164-171`, `:223-254`; `hammer/src/main.rs:183-186` | Worker-init marks a callback name before invocation and propagates a returned error out of the worker. The startup join detects the exited worker; `abort_workers` logs the joined callback error but returns `WorkerExitedBeforeStartupBarrier { phase: "main-loop" }`. The daemon exits on the startup error. The original callback error is not the error returned to the startup caller. |
| H9 / 当前项目 | `hammer/src/main.rs:51-124`, `:163-245`; `hammer-runtime/src/global_main.rs:54-79`; `hammer-runtime/src/file/mod.rs:144-165` | There is no UnixMain. Daemon `main` parses early config, pins the current thread, initializes the Main Heap and tracing, reparses the document, then constructs GlobalMain. GlobalMain currently carries ControlThread, control FileMain, and IPC listener state; FileMain is independently installed through `FILE_MAIN`. |
| H10 / 当前项目 | `hammer-runtime/src/process.rs:1-90`; `hammer-runtime/src/control_thread.rs:19-120`; `hammer-runtime/src/global_main/lifecycle.rs:12-70`; `hammer-component-macros/src/lib.rs:2350-2420` | Hammer Process Nodes are async functions receiving ProcessContext; ProcessEntry adapters create boxed futures, ControlThread owns their LocalSet/runtime and Process handles, and GlobalMain starts/runs/shuts them down. This differs from VPP's per-main process table and main-loop ownership. |

All `src/...` VPP paths in this table are relative to `third_party/vpp/`;
shortened Hammer crate paths are relative to `crates/`.

The actual startup paths are:

1. **VPP process path:** `vlib_main_init` -> `vlib_unix_main` -> plugin early
   loading/constructor registration -> early config -> `vlib_main` -> core
   initialization/thread administration/static node and graph setup -> normal
   init -> normal config -> sort worker-init -> main-loop-enter callbacks
   (including `start_workers`).
2. **VPP worker/startup join:** `start_workers` prepares descriptions and main
   clones -> launches -> workers enter `vlib_worker_thread_init`'s initial
   wait -> main observes acknowledgement and runs worker-count-change -> main
   releases -> workers initialize clock/timer/file state and run worker-init
   -> worker loop barrier check. The main-loop owner performs the later
   initial barrier before dispatching Process Nodes.
3. **Hammer current process path:** `run` -> `GlobalMain::new_configured` ->
   `init_control` -> service image installation -> `main_loop_enter` -> early
   config -> `load_plugins` (early config, init, graph extension, normal
   config) -> normal config/init/stats/enter -> `start_process_nodes`.
   `install_packet_graph` itself is currently an ordinary init hook.
4. **Hammer current worker path:** `start_workers` snapshots graph and copies
   worker-init declarations -> OS-thread closure -> launch-barrier check ->
   construct DataPlaneMain -> install shared control references and TLS ->
   worker-init -> build worker Tokio runtime -> packet loop. The startup
   function releases and re-arms a second barrier before returning.

The current macro-to-callback path is also explicit: `init_function` and
`worker_init_function` call `expand_registered_function` with different main
and declaration types; enter/exit attributes go through
`expand_main_loop_function` into the same expansion. The expansion emits the
original function, a synchronous adapter, and a static declaration containing
the adapter (`hammer-component-macros/src/lib.rs:2803-2915`, `:3335-3395`).
Image macros collect those declaration values; PluginMain subsequently copies
image slices for `init.rs` dispatch. The adapter obtains required or optional
`Arc<T>` inputs through `main.registry()` and publishes returned `Arc<T>`
through that registry. An absent optional dependency returns success without
calling the original function; dispatch has already recorded its name.
Config uses `expand_config_function` (`:3155`) to parse its TOML section and
perform the same registry-based injection/publication, but config dispatch
currently records success afterward. Migrating the callback receiver alone
does not remove these generated registry consumers or their skip semantics.

Worker-init failure must be distinguished from successful initialization in
both source models. VPP reports the returned error and then enters the worker
loop (`main.c:2040-2046`); the init dispatcher has stopped at that error, so
later hooks in the list have not run. Q9 selects the same continuation for
Hammer. Q12 defers recording or persistence of that error, while still
requiring an owner-local typed error definition for later handling. Current
Hammer instead exits the worker and aborts startup, returning a barrier-phase
error after logging the callback error (H8); that startup failure path is to be
removed. Failed startup currently joins workers and drops their local mains;
that cleanup cannot be reused unchanged once worker mains are transferred to
their thread owners for the process lifetime.

The registration trace extends through concrete consumers: runtime stats
configuration and the Binary API Process Node depend on generated registry
injection. Deleting registry getters therefore requires their declarations,
macro adapters, owner initialization, and ProcessContext consumers to migrate
together. `ApplicationMain::global`, `SessionMain::global`, and plugin-local
`TLS_MAIN` already demonstrate owner-local state in this checkout; they do not
justify retaining another erased registry inside the new thread authorities.

### Three-way ownership and lifecycle comparison

| Dimension | Current Hammer | Confirmed target / remaining work | VPP evidence |
| --- | --- | --- | --- |
| Process authority | GlobalMain aggregates execution, scheduling, plugins and threads; daemon main owns Unix startup | GlobalMain retains VPP-global responsibilities; ThreadMain, UnixMain, and WorkerThread are explicit separate roles | V1, V2, V9; decisions 30, 33, 40 |
| Per-main state | Local packet state plus shared global forwarding | Retain local graph/trace/random/loop/worker-init state; remove shared registry/config/queue/completion forwarding; retain per-main exit semantics | V1, V6, V8; decision 32 |
| Thread lifecycle | Construct worker main inside launched closure; no main table | Allocate and register stable mains before launch, transfer ownership once, keep them live for process lifetime; Rust transfer proof remains required | V3; decisions 27-31 |
| Hook registration | Image value arrays followed by copied Vecs; distinct main/worker registration types | Seven fields directly on GlobalMain; six common lifecycle declarations and one config declaration family; direct declaration references | V5, V7; decisions 9, 23, 35 |
| Scheduling and progress | Repeated category dispatch, config success-only name sets, second barrier in start_workers | Use actual VPP lifecycle positions and callback-identity progress; main-loop owner owns later initial barrier; preserve settled bootstrap-only worker-init rule | V3-V5; decisions 20, 27 |
| Control dependencies | Registry passed through main objects and ProcessContext; ControlThread owns the LocalSet | Owner-local APIs and thread-zero main/NodeMain scheduling state; no new aggregate owner introduced solely to keep current registry or LocalSet ownership | V4, H6; decisions 33, 47, 48 |
| Refork publication | Separate WorkerPublication graph and completion state copied into every main | First main supplies graph state; WorkerThread/WorkerBarrier coordinates barrier, request, and completion; GlobalMain retains none of that state | V6; decisions 10, 22, 32 |
| Safe access | Public TLS setters and nested closure accessors | Explicit owner borrows and phase-limited access; no GlobalMain pointer table, installation API, or unconstrained cross-thread reference | V8, H7; decisions 28, 31, 34 |
| ABI and migration | All plugin hook adapters rely on existing image/signature layout | Rebuild daemon, macros and plugins together; no compatibility forwarding; config-schema and failure-policy choices still recorded below | H1, H4-H6 |

GlobalMain alignment is not exhausted by its seven hook fields. The remaining
field groups were checked against the current runtime and daemon as follows;
an absent current feature is not an approved addition.

| VPP owner and field group | Current Hammer location / gap | Ownership constraint |
| --- | --- | --- |
| GlobalMain `name`, `exec_path`, `argv`, `startup_config` | `hammer::main` reads arguments/config; `PluginMain::directory` resolves executable location; current GlobalMain has no corresponding metadata fields | Process metadata belongs to GlobalMain when represented; plugin search policy belongs to PluginMain. Exact retained input fields and config compatibility are pending. |
| GlobalMain `node_registrations`, seven hook lists, `init_functions_called` | RegistrationImage and PluginMain copied collections; several GlobalMain called sets | Direct process-global declaration references and global progress belong to GlobalMain; graph execution belongs to each main. |
| VPP graph-refork request and one-time-release declarations | Current `WorkerPublication` and barrier paths are split across GlobalMain/DataPlaneMain | Target ownership is WorkerThread/WorkerBarrier; no graph-refork field is retained by GlobalMain. |
| GlobalMain `cli_main`, `elog_main`, `configured_elog_ring_size`, `post_mortem_callbacks` | No matching fields in current GlobalMain; daemon logging is initialized through tracing and control commands use Binary API/ctl owners | These VPP-global roles are not a license to put plugin metadata or IPC listeners back into GlobalMain. New CLI/event-log/post-mortem functionality is not decided in this interview. |
| GlobalMain `trace_filter`; per-main `trace_main` | `trace::configure_trace` returns a registry-published TraceControlPlane and installs a TraceControlHandle on the main, later shared with workers | Global filter policy and per-main trace state must be distinguished; existing generic registry forwarding must migrate with the trace hook. Full trace API redesign is not implied. |
| ThreadMain registration list/index, counts, CPU/NUMA/priority fields | `config::Worker`, `start_workers::resolve_worker_startup`, platform setup and daemon startup | Thread administration is process-wide. Non-cloning registrations and input-key compatibility are policy questions below. No second active worker table is inferred from an unused C member. |
| UnixMain flags, control socket, error history, startup/runtime paths, terminal and log policy | No UnixMain; `hammer/src/main.rs` owns startup/config/logging and current runtime owns control/FileMain | Unix host-process lifecycle is independent from GlobalMain graph/registration ownership. Existing Hammer equivalents move to UnixMain; productless interactive CLI/pager state is not added solely for naming alignment. |
| WorkerThread launch/stack/heap/function, identity/placement and event-track fields | Local worker closure/JoinHandle list, `config::Worker`, `worker_thread.rs` setup; no WorkerThread descriptor | Thread descriptions include main thread zero; Main Heap remains the independent allocation authority. A description is not the per-main graph object. |
| WorkerThread wait/acknowledgement, recursion and refork-completion fields | Private barrier state plus GlobalMain/DataPlaneMain completion references | Shared synchronization stays in the worker-thread subsystem, using existing runtime synchronization boundaries. Per-main timing/debug fields stay per-main. |
| DataPlaneMain node/error/frame, random, trace, handoff, loop and exit facts | DataPlaneMain, NodeMain, DataPlaneTrace, DataPlaneHandoffWorker; exit facts currently shared through GlobalMain | These are execution facts. The change removes cross-owner forwarding, not legitimate per-main state. |
| DataPlaneMain File poller, timer, RPC and main Process state | FileMain stores indexed pollers; worker Tokio runtime lives in the thread closure; ControlThread/ProcessContext own separate process tasks and registry access | VPP File registry is independent (`file.c:23`), pollers/timers/RPC are per-main, and Process state belongs to main NodeMain. Concrete Rust storage and borrow changes must cover these actual consumers. |
| DataPlaneMain Buffer/physmem fields | Independent BUFFER_MAIN, core Buffer caches and mapping owners | ADR-0007's established owner split remains binding; no GlobalMain packet-storage owner is added. |

The absence statements above are scoped to the inspected runtime/daemon
definitions and searches, not claims that every plugin lacks analogous
diagnostic behavior. Fields without an existing Hammer counterpart remain
unimplemented unless a later explicit scope decision adds the feature.

The same distinction applies to DataPlaneMain: barrier timing, parked-state
diagnostics, per-thread poller state, handoff state, RPC processing and timers
are present in `vlib_main_t`. Shared barrier handshake and refork completion
live in the worker-thread subsystem. Existing standalone Buffer Main ownership
from ADR-0007 is retained; matching this VPP version's `physmem_main` field does
not reopen that already confirmed allocation decision.

## 目标 Rust 设计（待评审）

本节把前面的 VPP 语义收敛为可实现的 Rust 类型和方法边界。它是设计
草案，不是实现授权。方法名和签名表达 owner、借用和生命周期；它们不
复刻 VPP 的 C ABI，也不增加与 VPP 无对应关系的通用转发层。

下面是本设计的 Rust 接口草图。它只表达字段、owner 和方法边界，不是
可以直接编译的实现。字段类型优先使用当前项目和 VPP 已有的类型；没有
现成 Rust 类型的地方只保留字段语义，不能据此偷偷新增 `WorkerParts`、
`MainConfig`、`ConfigDocument`、`StartupConfig` 或其他包装类型。

```rust
use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::bitmap::Bitmap;

#[repr(C)]
struct GlobalMain {
    cacheline0: CacheLineAlignMark,
    name: String,
    exec_path: String,
    argv: Vec<String>,
    startup_config: String,
    node_registrations: Vec<&'static NodeEntry>,
    init_function_registrations: Vec<&'static InitFunction>,
    main_loop_enter_function_registrations: Vec<&'static InitFunction>,
    main_loop_exit_function_registrations: Vec<&'static InitFunction>,
    worker_init_function_registrations: Vec<&'static InitFunction>,
    num_workers_change_function_registrations: Vec<&'static InitFunction>,
    api_init_function_registrations: Vec<&'static InitFunction>,
    config_function_registrations: Vec<&'static ConfigFunction>,
    init_functions_called: Bitmap,
}

impl GlobalMain {
    fn global() -> &'static Self;
    fn register_init(&mut self, registration: &'static InitFunction);
    fn register_main_loop_enter(&mut self, registration: &'static InitFunction);
    fn register_main_loop_exit(&mut self, registration: &'static InitFunction);
    fn register_worker_init(&mut self, registration: &'static InitFunction);
    fn register_num_workers_change(&mut self, registration: &'static InitFunction);
    fn register_api_init(&mut self, registration: &'static InitFunction);
    fn register_config(&mut self, registration: &'static ConfigFunction);
    fn run_init_functions(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>;
    fn run_config_functions(
        &mut self,
        main: &mut DataPlaneMain,
        document: &str,
        early: bool,
    ) -> RuntimeResult<()>;
    fn run_main_loop_enter(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>;
    fn run_main_loop_exit(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>;
}

#[repr(C)]
struct ThreadMain {
    thread_count: u32,
    worker_count: u32,
    cpu_core_bitmap: Bitmap,
    cpu_socket_bitmap: Bitmap,
    worker_threads: Vec<WorkerThread>,
}

impl ThreadMain {
    fn configure(&mut self, worker_count: u32, stack_size: usize) -> RuntimeResult<()>;
    fn thread_count(&self) -> u32;
    fn worker_count(&self) -> u32;
    fn thread_by_index(&self, index: u32) -> Option<&WorkerThread>;
    fn start_workers(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>;
    fn join_workers(&mut self) -> RuntimeResult<()>;
}

#[repr(C)]
struct WorkerThread {
    cacheline0: CacheLineAlignMark,
    thread_index: u32,
    cpu_index: Option<u32>,
    numa_node: Option<u32>,
    stack_size: usize,
    entry: fn(),
    no_data_structure_clone: bool,
    startup_acknowledged: bool,
    join_handle: Option<std::thread::JoinHandle<RuntimeResult<()>>>,
    cacheline1: CacheLineAlignMark,
}

impl WorkerThread {
    fn launch(&mut self, main: DataPlaneMain) -> RuntimeResult<()>;
    fn acknowledge_startup(&mut self);
    fn enter_loop(&mut self) -> RuntimeResult<()>;
    fn complete_refork(&mut self);
    fn join(self) -> RuntimeResult<()>;
}

// hammer_runtime::file，crate 内部的互斥调度模式。
enum FileMode {
    // 直接调用已有全局 FileMain 的 poll_for_worker。
    Sync,
    // 仅 thread zero，拥有主线程的 Tokio readiness adapter。
    Async(AsyncFileMain),
}

#[repr(C)]
struct DataPlaneMain {
    cacheline0: CacheLineAlignMark,
    thread_index: u32,
    active_numa_node: u32,
    nodes: NodeMain,
    random: SmallRng,
    buffer_caches: OnceCell<Box<[RefMut<'static, BufferThreadCache>]>>,
    handoff: Option<DataPlaneHandoffWorker>,
    trace: DataPlaneTrace,
    simd_bytes: usize,
    file_main: FileMode,
    main_loop_count: u32,
    main_loop_exit_now: bool,
    main_loop_exit_status: i32,
    worker_init_functions_called: Bitmap,
}

impl DataPlaneMain {
    fn new_main(/* existing startup inputs */) -> RuntimeResult<Self>;
    fn new_worker(/* ThreadMain/WorkerThread prepared inputs */) -> RuntimeResult<Self>;
    fn thread_index(&self) -> u32;
    fn random(&mut self) -> &mut SmallRng;
    fn nodes(&self) -> &NodeMain;
    fn nodes_mut(&mut self) -> &mut NodeMain;
    fn run_main_loop(&mut self, unix: &mut UnixMain) -> RuntimeResult<()>;
    fn run_worker_loop(&mut self) -> RuntimeResult<()>;
    fn run_worker_init(&mut self) -> RuntimeResult<()>;
    fn request_exit(&mut self, status: i32);
    fn signal_process(&mut self, node: NodeId, event_type: u64, data: u64)
        -> RuntimeResult<()>;
    fn process_wait(&mut self, node: NodeId, deadline: Option<Instant>);
}

struct NodeMain {
    nodes: Vec<NodeEntry>,
    node_by_name: HashMap<String, NodeId>,
    nodes_by_type: Vec<NodeId>,
    next_frames: Vec<NextFrame>,
    pending_frames: Vec<PendingFrame>,
    frame_sizes: Vec<usize>,
    process_node_indices: Vec<NodeId>,
    process_restore_current: Vec<(NodeId, u64)>,
    process_restore_next: Vec<(NodeId, u64)>,
    suspended_process_frames: Vec<NodeId>,
    event_state: Vec<(NodeId, u64, Vec<u64>)>,
    timer_state: Vec<(NodeId, Instant)>,
    current_process_index: Option<NodeId>,
    time_next_process_ready: Option<Instant>,
}

impl NodeMain {
    fn register_process(&mut self, node: NodeId, process: Process) -> RuntimeResult<()>;
    fn start_processes(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>;
    fn restore_processes(&mut self, now: Instant) -> RuntimeResult<usize>;
    fn signal_process(
        &mut self,
        node: NodeId,
        event_type: u64,
        data: u64,
    ) -> RuntimeResult<()>;
    fn poll_processes(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<usize>;
    fn stop_processes(&mut self) -> RuntimeResult<()>;
    fn process_node_index(&self, name: &str) -> Option<NodeId>;
}

struct Process<S, F>
where
    F: Future<Output = RuntimeResult<()>>,
{
    node_index: NodeId,
    state: S,
    future: F,
}

impl<S, F> Process<S, F>
where
    F: Future<Output = RuntimeResult<()>>,
{
    fn new(node_index: NodeId, state: S, future: F) -> Self;
    fn node_index(&self) -> NodeId;
    fn state(&self) -> &S;
    fn state_mut(&mut self) -> &mut S;
    fn complete(self) -> RuntimeResult<()>;
    fn cancel(self);
}

impl<S, F> Future for Process<S, F>
where
    F: Future<Output = RuntimeResult<()>>,
{
    type Output = RuntimeResult<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>)
        -> Poll<Self::Output>;
}

struct UnixMain {
    flags: u32,
    error_history: Vec<(Instant, RuntimeError)>,
    error_history_index: usize,
    total_errors: u64,
    startup_path: Option<PathBuf>,
    runtime_path: Option<PathBuf>,
    pid_path: Option<PathBuf>,
    log_path: Option<PathBuf>,
    log_fd: Option<RawFd>,
    poll_sleep: Duration,
}

impl UnixMain {
    fn global() -> &'static Self;
    fn startup(document: &str) -> RuntimeResult<()>;
    fn run(
        &mut self,
        global: &mut GlobalMain,
        threads: &mut ThreadMain,
        main: &mut DataPlaneMain,
    ) -> RuntimeResult<i32>;
    fn record_error(&mut self, error: RuntimeError);
    fn request_exit(&mut self, status: i32);
    fn shutdown(&mut self, status: i32) -> RuntimeResult<()>;
}

struct PluginMain {
    modules_by_plugin: HashMap<String, PluginModuleRef>,
    library_index_by_name: HashMap<String, usize>,
    load_order: Vec<String>,
    registration_images: Vec<&'static RegistrationImage>,
    libraries: Vec<PluginLibrary>,
}

impl PluginMain {
    fn register_image(&mut self, image: &'static RegistrationImage);
    fn load(&mut self, host_version: &str, roots: &[String]) -> Result<(), PluginError>;
    fn loaded_plugins(&self) -> &[String];
    fn visit_images(&self, visit: impl FnMut(&RegistrationImage));
    fn retain_loaded_images(&self);
}

struct RegistrationImage {
    init_functions: &'static [InitFunction],
    config_functions: &'static [ConfigFunction],
    early_config_functions: &'static [ConfigFunction],
    main_loop_enter_functions: &'static [InitFunction],
    main_loop_exit_functions: &'static [InitFunction],
    worker_init_functions: &'static [InitFunction],
    num_workers_change_functions: &'static [InitFunction],
    api_init_functions: &'static [InitFunction],
    process_nodes: &'static [NodeEntry],
    graph_nodes: &'static [NodeEntry],
    node_functions: &'static [NodeFunctionRegistration],
    binary_api_methods: &'static [BinaryApiMethodEntry],
    stats_registrations: &'static [StatsRegistration],
}

struct InitFunction {
    name: &'static str,
    runs_before: &'static [&'static str],
    runs_after: &'static [&'static str],
    func: fn(&mut DataPlaneMain) -> RuntimeResult<()>,
}

struct ConfigFunction {
    name: &'static str,
    section: &'static str,
    runs_before: &'static [&'static str],
    runs_after: &'static [&'static str],
    early: bool,
    func: fn(&mut DataPlaneMain, &str) -> RuntimeResult<()>,
}
```

### 现有类型复用表

目标设计新增已确定的 runtime authority、`Process<S, F>`，以及本轮明确
要求的互斥 File 模式 enum `FileMode`。CPU/NUMA 位图保留并直接复用
`hammer_infra::bitmap::Bitmap`，不能把裸 `Vec<u64>` 当作 bitmap，也不能
用配置类型或删除字段代替位图语义。

| 目标字段/职责 | 复用类型 | 当前来源 |
| --- | --- | --- |
| ThreadMain 的数量与栈配置 | `u32`、`usize` | 直接表达线程数量和栈大小，不新增聚合配置类型 |
| 可用 CPU core 与 CPU socket/NUMA node 集合 | `Bitmap`，即现有 `Bitmap<usize>` | `hammer_infra::bitmap`；保留 `cpu_core_bitmap` 与 `cpu_socket_bitmap`，复用其 `set`、`clear`、`is_set`、`count_set`、`iter_set` |
| global / per-main init 调用进度 | `Bitmap` | `hammer_infra::bitmap`；注册期建立稳定 callback 索引映射；调用前 `set`，错误不清位；映射尚需实现证明，不复用临时排序位置 |
| CPU、调度、NUMA 配置输入 | `WorkerCpu`、`WorkerScheduler`、`WorkerNuma` | 当前配置模块已有字段类型；配置输入与运行期可用资源 bitmap 是不同事实 |
| DataPlaneMain 的 graph | `NodeMain`、`NodeEntry`、`NodeId`、`NodeRuntime` | `hammer_runtime::node`、`hammer_core::data_plane` |
| DataPlaneMain 的 Buffer 构造输入 | `DataPlaneBufferConfig` | `hammer_runtime::data_plane::config` |
| DataPlaneMain 的 handoff、trace、file | `DataPlaneHandoffWorker`、`DataPlaneTrace`；`FileMode::Async` 拥有现有 `AsyncFileMain`，`Sync` 使用已有全局 `FileMain` | `hammer_runtime::handoff`、`trace`、`file`；不复制 File registry/poller storage |
| RegistrationImage 的 declarations | `InitFunction`、`ConfigFunction`、`NodeEntry`、`NodeFunctionRegistration`、`BinaryApiMethodEntry`、`StatsRegistration` | 当前 `registration.rs`、`init.rs`、`node.rs`、`binary_api.rs` |
| PluginMain 的 image/code lifetime | `PluginModuleRef`、`PluginLibrary`、`RegistrationImage` | 当前 `plugin.rs`、`plugin_loader.rs`、`registration.rs` |
| NodeMain 的 packet graph queues | 当前 `NodeMain` 已有的 `NextFrame`、`PendingFrame`、`FramePool` 字段 | `hammer_runtime::node` 与 `hammer_core::buffer` |

`ThreadRole`、`WorkerLaunchState`、`WorkerParts`、`BufferCaches`、
`TerminalPolicy`、`ThreadRegistration`、`MainConfig`、`ConfigDocument`、
`StartupConfig` 等名称都不属于本设计。它们不能作为字段类型、参数包装或
兼容层重新出现。

### GlobalMain

`GlobalMain` 是唯一的 process-global global authority，对应
`vlib_global_main_t`。它只保存 process metadata、node declarations、七类
registration lists 和 init progress；不拥有 `DataPlaneMain`、`WorkerThread`、
Tokio executor、PluginMain、IPC listener 或 graph-refork state。

| 成员 | 设计 |
| --- | --- |
| owner state | seven registration lists、`init_functions_called` 和 GlobalMain metadata |
| lookup | `global` 只返回 GlobalMain；per-thread `DataPlaneMain` 的索引查找由持有线程执行状态的 owner 提供，不在 GlobalMain 增加字段或 forwarding getter |
| current | current main 通过调用方已有的 `&mut DataPlaneMain` 表达；不保留 TLS current-main setter、getter 或 raw-pointer facade |

| 字段 | 所有权与语义 |
| --- | --- |
| `name`、`exec_path`、`argv`、`startup_config` | process metadata；由 GlobalMain 保存其生命周期内需要的启动事实 |
| `node_registrations` | constructor/plugin 提供的 graph node declarations |
| `init_function_registrations`、`main_loop_enter_function_registrations`、`main_loop_exit_function_registrations`、`worker_init_function_registrations`、`num_workers_change_function_registrations`、`api_init_function_registrations` | VPP 七类 registration 中的六类 common lifecycle lists |
| `config_function_registrations` | 唯一 config list；early/ordinary 是 declaration 属性，不是第二个 list |
| `init_functions_called: Bitmap` | 稳定回调索引对应的 global call-once progress；注册期必须建立 callback identity 映射，调用前置位，错误不清位 |

| 方法 | 可见性与签名 | 作用 |
| --- | --- | --- |
| `global` | `pub fn global() -> &'static GlobalMain` | 取得唯一 process-global authority |
| `register_*` | `pub(crate)`，分别对应七个 registration list | 接收 PluginMain 提供的 direct declaration reference |
| `run_init_functions` | `pub(crate) fn run_init_functions(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>` | 按 VPP global progress 调度 init declarations |
| `run_config_functions` | `pub(crate) fn run_config_functions(&mut self, main: &mut DataPlaneMain, document: &str, early: bool) -> RuntimeResult<()>` | 使用 config list 和 GlobalMain 的统一 call progress |
| `run_main_loop_enter` | `pub(crate) fn run_main_loop_enter(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>` | 调度 main-loop-enter declarations |
| `run_main_loop_exit` | `pub(crate) fn run_main_loop_exit(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>` | 调度 main-loop-exit declarations |

`GlobalMain` 不提供 `data_plane_main()`、`data_plane_main_mut()`、
`worker_barrier()`、`control_thread()`、`registry()`、`plugin_main()`、
`ipc_listener()`、`schedule_on_worker()` 或 lifecycle forwarding 方法。

### ThreadMain 与 WorkerThread

`ThreadMain` 对应 `vlib_thread_main_t`，只管理线程数量、线程 placement 和
scheduling policy。当前 Hammer 没有独立的 thread registration Rust 类型，
因此这里不凭空增加 `ThreadRegistration`；线程声明继续复用现有
image/macro registration 输入。现有配置字段按职责传入，不以一个名为
`Worker` 的聚合类型进入目标 authority。`WorkerThread` 对应
`vlib_worker_thread_t`，描述一个 OS thread，包括 thread zero；它不拥有
该线程的 `DataPlaneMain` graph state。

结构布局也遵守 VPP 的 cacheline 分区：`GlobalMain` 和每个
`DataPlaneMain` 以 `cacheline0` 开始；`WorkerThread` 保留 barrier/acknowledge
热字段的 `cacheline0` 与 launch/identity/refork 字段前的 `cacheline1`。
`ThreadMain` 与 VPP 的 `vlib_thread_main_t` 一样没有额外的 cacheline marker；
不得为了“看起来对齐”给它或其他没有 VPP marker 的结构添加 padding 字段。
复用 `hammer_infra::align::CacheLineAlignMark`，并用 Rust 的
`size_of`、`align_of`、`offset_of` 验证 marker 后的字段边界；不写 C probe。

| 类型 | 字段 |
| --- | --- |
| `ThreadMain` | `thread_count`、`worker_count`、`cpu_core_bitmap: Bitmap`、`cpu_socket_bitmap: Bitmap`、`worker_threads: Vec<WorkerThread>`；配置按字段职责传入，不持有 `Worker` 聚合类型 |
| `WorkerThread` | `thread_index`、thread role、CPU/NUMA placement、stack size、entry function、`no_data_structure_clone` result、launch state、startup acknowledgement、barrier/refork coordination、OS join state；不包含 `DataPlaneMain` graph fields |

`cpu_core_bitmap` 保存 OS 可用 CPU 集合，`cpu_socket_bitmap` 保存在线
socket/NUMA node 集合；配置中的目标 CPU 列表不能替代它们。VPP 在
`threads.h:229-233` 声明这两个字段，在 `threads.c:225-281` 填充集合、
复制可分配 core 集合、排除保留 core，并验证选择。Hammer 保留这些
集合语义，通过现有 `Bitmap` API 进行操作，不手写 word/bit 运算。

| 类型 | 方法 |
| --- | --- |
| `ThreadMain` | `new`、`configure`、`thread_count`、`worker_count`、`thread_by_index`、`start_workers`、`join_workers` |
| `WorkerThread` | `new`、`launch`、`acknowledge_startup`、`enter_loop`、`complete_refork`、`join` |

目标签名为：

```text
ThreadMain::start_workers(&mut self, main: &mut DataPlaneMain) -> RuntimeResult<()>

ThreadMain::join_workers(&mut self) -> RuntimeResult<()>
WorkerThread::launch(&mut self, main: DataPlaneMain) -> RuntimeResult<()>
WorkerThread::join(self) -> RuntimeResult<()>
```

`no_data_structure_clone` 的 auxiliary registration 只创建
`WorkerThread`，不创建或移交 `DataPlaneMain`。普通 Data Worker 在 launch
前完成 `DataPlaneMain` 构造，再一次性将 owner 移入 `WorkerThread` 的
OS-thread closure。线程 owner 负责保持该值存活；这里不添加地址登记 API、
raw-pointer facade 或独立地址表。

### DataPlaneMain

`DataPlaneMain` 对应 `vlib_main_t`，由 thread zero 或一个 Data Worker
独占。thread zero 的实例拥有现有 `AsyncFileMain`；每个 DataPlaneMain
保留 graph、NodeMain、trace、random、File poller、timer、RPC、
loop counters、worker-init progress 和 per-main exit facts；不保留
RuntimeRegistry、全体 worker config、全体 worker queue、GlobalMain
publication 或共享退出引用。

| 字段 | 所有权与语义 |
| --- | --- |
| `thread_index`、`active_numa_node` | 当前 OS thread 的 main identity 与 placement |
| `nodes` | 当前 main 独占的 NodeMain |
| `random` | 当前 main 的 non-cryptographic random state |
| `buffer_caches`、Buffer/physmem access state | 当前 main 的 packet Buffer access；仍由既有 Buffer Main/Pool owner 管理生命周期 |
| `handoff` | 当前 Data Worker 的 handoff ownership |
| `trace`、`simd_bytes` | 当前 main 的 trace state 与 selected SIMD width |
| `file_main: FileMode` | 唯一互斥字段；index zero 使用 `Async(AsyncFileMain)`，Data Worker 使用 `Sync`；不存在同时持有两个 backend 或两者都未选择的运行期状态 |
| `main_loop_count`、per-main exit facts | 当前 main 的 loop progress 与 exit status/request |
| `worker_init_functions_called: Bitmap` | 当前 main 独立的 worker-init progress；使用注册期稳定回调索引，调用前置位，错误不清位 |

| 方法 | 可见性与签名 | 作用 |
| --- | --- | --- |
| `new_main` | `pub(crate)`，由现有启动配置和 graph/Buffer owner 输入构造 | 创建 index zero 的 main；不新增 `MainConfig` 类型 |
| `new_worker` | `pub(crate)`，直接接收 ThreadMain/WorkerThread 已准备的现有构造输入 | 创建一个已分配、待移交的 worker main；不新增 `WorkerParts` 类型 |
| `thread_index` | `pub fn thread_index(&self) -> u32` | 返回当前 main 的 thread index |
| `random` | `pub fn random(&self) -> RefMut<'_, SmallRng>` | owner-thread random access |
| `nodes` / `nodes_mut` | `pub(crate)` | 访问本 main 的 NodeMain；mutable access 只从当前 owner 借用产生 |
| `run_main_loop` | `pub(crate) fn run_main_loop(&mut self, unix: &mut UnixMain) -> RuntimeResult<()>` | 驱动当前 main 的 loop；thread zero 执行 Process scheduling |
| `run_worker_loop` | `pub(crate) fn run_worker_loop(&mut self) -> RuntimeResult<()>` | 执行 Data Worker packet/handoff/barrier loop |
| `run_worker_init` | `pub(crate) fn run_worker_init(&mut self) -> RuntimeResult<()>` | 使用本 main 的 worker-init progress 执行 worker callbacks |
| `request_exit` | `pub(crate) fn request_exit(&mut self, status: i32)` | 修改本 main 的退出事实 |
| `signal_process` | `pub(crate) fn signal_process(&mut self, node: NodeId, event_type: u64, data: u64) -> RuntimeResult<()>` | 按 Process Node index 投递 signal |
| `process_wait` | `pub(crate) fn process_wait(&mut self, node: NodeId, deadline: Option<Instant>)` | 由 NodeMain 建立本次 Process restore；不得返回持有 main borrow 的 future |

`DataPlaneMain` 不提供按 `u32` 直接返回可变 Buffer、按 worker index 返回
`&mut DataPlaneMain`、GlobalMain forwarding、TLS install/current accessor，
或跨 `.await` 持有它的 Process future。

### FileMode 与现有 File 类型

`FileMode` 属于 `hammer_runtime::file`，仅在 crate 内可见。它表达用户已
明确要求的同步/异步选择，复用现有 `FileMain` 与 `AsyncFileMain`，不增加
通用 forwarding 方法。创建 main 时选定模式，正常运行中不切换。

当前 `FileMain` 在 `file/mod.rs:144-159` 是 `FILE_MAIN` 中的进程全局
registry，拥有 file/deadline pools 和按线程索引的 pollers。
`AsyncFileMain` 在 `:750-804` 只持有一个 Tokio `AsyncFd<OwnedFd>`，其
`next_ready` 仍然分派到该全局 registry。故 `Sync` 使用 unit variant：
worker loop 直接调用已有 `FileMain::poll_for_worker(thread_index, nodes)`；
`Async` variant 按值拥有现有 adapter，由 thread-zero loop 调用
`AsyncFileMain::next_ready`。两者共享既有 registry 的生命周期，不复制
FileMain，不添加 reference carrier，不用 `Option` 表达运行模式。

VPP 对应依据是 `src/vlib/file.c:23-97` 的独立 file registry 与每 main
poller（路径相对 `third_party/vpp/`）。Tokio adapter 和 `FileMode` 是
Hammer 已确认的 Rust 调度表示；不是声称 VPP 定义了这个 enum。

### NodeMain 与 Process

`NodeMain` 是 `DataPlaneMain` 内的 graph/process owner，对应 VPP
`vlib_node_main_t`。它保存 Process Node index table、Process restore
queues、suspended frame/restore facts、timer/event scheduling state 和
current Process identity。它不另建 ControlThread。

| 字段 | 所有权与语义 |
| --- | --- |
| `nodes`、`node_by_name`、`nodes_by_type` | 当前 main 的 graph node registry and runtime lookup |
| `next_frames`、`pending_frames`、`frame_sizes` | 当前 main 的 packet graph scheduling state |
| `process_node_indices` | 已注册 Process Node 的 `NodeId` 索引；不使用名称作为运行时身份 |
| `process_restore_current`、`process_restore_next` | 当前和下一轮待恢复的 Process Node restore records |
| `suspended_processes` | 已挂起 Process 的 frame/restore linkage |
| `event_state`、`timer_state` | Process event type、pending event data 和 timer restore state |
| `current_process_index` | 当前运行 Process，空值表示没有运行 Process |
| `time_next_process_ready` | 下一次 Process 可恢复的时间 |

`Process<S, F>` 是唯一的 Process execution type。它是具体的 Rust Future
类型，由 Tokio 调度；Process-specific state 通过具体 state 参数 `S` 提供，
future 通过具体类型参数 `F` 提供，不放进中央 Process enum，也不擦除为
`dyn Future`。
Tokio 接管正在运行的 `Process` Future 后，NodeMain 保留 index、registration、
restore 和 lifecycle metadata。

| 字段 | 所有权与语义 |
| --- | --- |
| `node_index: NodeId` | Process Node 的稳定运行时 identity |
| `state: S` | 跨 callback suspend 保留的 Process-specific state |
| `future: F` | 具体 async execution state；不得持有 `DataPlaneMain` borrow 跨 `.await` |

| 类型 | 方法 |
| --- | --- |
| `NodeMain` | `register_process`、`start_processes`、`restore_processes`、`signal_process`、`poll_processes`、`stop_processes`、`process_node_index` |
| `Process` | `new`、`node_index`、`poll`（`Future` 实现）、`complete`、`cancel` |

目标方法边界为：

```rust
NodeMain::register_process(
    &mut self,
    node: NodeId,
    process: Process<S, F>,
) -> RuntimeResult<()>

NodeMain::signal_process(
    &mut self,
    node: NodeId,
    event_type: u64,
    data: u64,
) -> RuntimeResult<()>

NodeMain::restore_processes(&mut self, now: Instant) -> RuntimeResult<usize>
Process::poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<RuntimeResult<()>>
```

`ProcessEventBatch`、`ProcessWake`、`ProcessHandle` 和 `ProcessContext` 不再
构成公共模型。事件批处理、clock wake、restore reason 和 pending event
data 是 `Process`/`NodeMain` 的内部状态；跨 await 的用户 Process 状态由
具体 `Process` 自身持有。Process callback 对 `DataPlaneMain` 的访问只在
一次 step 内发生，产生的 future 不携带该借用。

### UnixMain

`UnixMain` 对应 `unix_main_t` 与 `vlib_unix_main`，是独立的 process-global
host authority。它拥有现有 Hammer 的 Unix startup、runtime/control paths、
signal/exit、logging 和 error history；IPC listener 属于 daemon 的 IPC owner，
不拥有
GlobalMain registration lists、DataPlaneMain graph 或 Process scheduler。

| 字段 | 所有权与语义 |
| --- | --- |
| Unix flags | daemon/interactive、syslog、color/banner 等 host policy |
| error history、error count | Unix host error history |
| startup/runtime/pid paths | host process startup and runtime paths |
| log file/fd、terminal policy、poll sleep | Unix logging and terminal behavior |

| 方法 | 可见性与签名 | 作用 |
| --- | --- | --- |
| `global` | `pub fn global() -> &'static UnixMain` | 取得唯一 Unix host authority |
| `startup` | `pub(crate) fn startup(document: &str) -> RuntimeResult<()>` | 对应 `vlib_unix_main` 的 host bootstrap；不新增 `StartupConfig` 类型 |
| `run` | `pub(crate) fn run(&mut self, global: &mut GlobalMain, threads: &mut ThreadMain, main: &mut DataPlaneMain) -> RuntimeResult<i32>` | 按 VPP 顺序连接 host、global、thread 和 main 生命周期 |
| `record_error` | `pub(crate) fn record_error(&mut self, error: RuntimeError)` | 维护 Unix host error history |
| `request_exit` | `pub(crate) fn request_exit(&mut self, status: i32)` | 发起 process exit |
| `shutdown` | `pub(crate) fn shutdown(&mut self, status: i32) -> RuntimeResult<()>` | 关闭 host resources 和 main-thread entry |

不提供 current-thread UnixMain lookup、GlobalMain forwarding getter 或
公共 raw-pointer/TLS API。

### PluginMain 与 RegistrationImage

`PluginMain` 只拥有 loaded image、metadata、load order 和 code lifetime。
`RegistrationImage` 只携带 image 内静态 declarations 的 direct references。
它们不复制七类 registration vectors，也不提供 generic registry。

| 类型 | 字段 |
| --- | --- |
| `PluginMain` | `modules_by_plugin`、`library_index_by_name`、`load_order`、image references、loaded library handles |
| `RegistrationImage` | 复用当前的 `init_functions`、`config_functions`、`early_config_functions`、`main_loop_enter_functions`、`main_loop_exit_functions`、`worker_init_functions`、`graph_nodes`、`node_functions`、`process_nodes`、`binary_api_methods`、`stats_registrations` 字段；迁移后 hook callback 使用统一的 `InitFunction` |

| 类型 | 方法 |
| --- | --- |
| `PluginMain` | `new`、`register_image`、`load`、`loaded_plugins`、`visit_images`、`retain_loaded_images` |
| `RegistrationImage` | `init_functions`、`config_functions`、`early_config_functions`、`main_loop_enter_functions`、`main_loop_exit_functions`、`worker_init_functions`、`graph_nodes`、`node_functions`、`process_nodes`、`binary_api_methods`、`stats_registrations` |

`PluginMain::visit_images` 只把每个 image 的 direct reference 交给具体
registration owner；`collect_registrations`、按 category 复制 Vec 的方法、
以及把所有 dependency 放进 RegistrationImage 的方法都删除。image/code
的加载成功后保持到 process teardown，以保证 GlobalMain 和 owner-held
declaration references 不悬空。

### Init/config declarations 与宏生成方法

六类 lifecycle declarations 共用执行 main callback：

```rust
fn(&mut DataPlaneMain) -> RuntimeResult<()>
```

config declaration 额外接收已经解析的 owner config；worker-init 的 progress
按每个 DataPlaneMain 保存，其他 global progress 按 GlobalMain 保存。宏不
再生成 `RuntimeRegistry` lookup/set，也不自动发布返回的 `Arc<T>`。

| 类型 | 字段 |
| --- | --- |
| `InitFunction` | `name`、`runs_before`、`runs_after`、`func: fn(&mut DataPlaneMain) -> RuntimeResult<()>` |
| `ConfigFunction` | `name`、`section`、`runs_before`、`runs_after`、`early`、owner-local config callback |
| `StatsRegistration` | 继续由 stats owner 持有其 name、registration callback 和 binding callback；不进入七类 GlobalMain lifecycle lists |

| 宏/方法 | 目标方法或生成结果 |
| --- | --- |
| `init_function` | 生成 `fn(&mut DataPlaneMain) -> RuntimeResult<()>` declaration |
| `worker_init_function` | 生成同一 callback shape，调用方是 worker-owned DataPlaneMain |
| `main_loop_enter_function` | 生成同一 callback shape，调用方是 thread-zero DataPlaneMain |
| `main_loop_exit_function` | 生成同一 callback shape，调用方是 thread-zero DataPlaneMain |
| `config_function` / `early_config_function` | 生成 `fn(&mut DataPlaneMain, &str) -> RuntimeResult<()>`，early 只是同一 config list 的属性 |
| `process_node` | 生成具体 `Process` constructor/step declaration；只使用 DataPlaneMain 的一次 step borrow |
| `run_*_functions` | 由 GlobalMain 或 DataPlaneMain 的实际 owner 调度，不接收 registry 或 copied image Vec |

插件依赖由插件自己的 owner-defined API 取得和发布。缺失依赖返回其
owner-local typed error；不得用 `None`、跳过 callback、日志或 generic
registry 隐藏失败。

### 错误与生命周期方法

worker-init callback 返回错误时，停止当前 dispatch pass 的后续 callback，
随后进入 worker loop。VPP 在此报告错误；用户 Q12 对 Hammer 的最新要求是
先定义 owner-local typed error，暂不记录、持久化或接入报告消费者。
错误仍以 owner-local typed value 表达，例如：

```rust
struct WorkerInitError {
    worker: u32,
    callback: &'static str,
    source: RuntimeError,
}
```

该错误在 worker-thread startup boundary 被识别；报告与保留机制后置，
不重新包装成 `WorkerExitedBeforeStartupBarrier`，也不添加 restart/reclaim API。

### 目标调用链

```text
UnixMain::startup
  -> install process authorities and establish thread-zero main
  -> plugin early load and direct registration publication
  -> GlobalMain::run_config_functions(thread_zero_main, early = true)
  -> thread administration, static node registration and graph installation
  -> owner-local timer/File setup; enter Tokio reactor before AsyncFileMain construction
  -> GlobalMain::run_init_functions(thread_zero_main)
  -> GlobalMain::run_config_functions(thread_zero_main, early = false)
  -> sort worker-init declarations
  -> GlobalMain::run_main_loop_enter(thread_zero_main)
     -> ThreadMain::start_workers(thread_zero_main)
       -> construct worker DataPlaneMain values
       -> launch WorkerThread descriptors
       -> startup barrier acknowledgement
       -> worker-count-change declarations under barrier
       -> release launch barrier
       -> worker-local init and worker-init declarations
  -> UnixMain::run(...)
       -> thread-zero DataPlaneMain::run_main_loop
       -> later initial worker barrier sync/release
       -> NodeMain starts/restores Tokio-scheduled Process values
```

Process Node scheduling remains on thread zero. Data Workers only execute packet,
handoff, worker barrier and graph-refork work. Final shutdown keeps the VPP
barrier held while exit hooks run. Published workers and plugin code remain live
through process exit; normal exit does not release the barrier to join/restart
workers or unload referenced images. The sketch's join methods are limited to
an explicitly proved unpublished launch-failure path, never ordinary shutdown.

## 变更清单

### 新增

| 类别 | 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- | --- |
| 类型 | `ThreadMain` | `hammer-runtime`，thread administration owner；最终模块位置待定 | Q2 确认新增与 `vlib_thread_main_t` 对应的进程级职责：线程注册、数量、CPU/NUMA 和调度配置；不持有七类 GlobalMain hook inventory 或通用服务 registry；具体字段待细化 | 启动和配置调用方迁移；Rust 内部类型，无持久化对象；TOML 键兼容性见未决问题 | thread-zero/worker 索引与配置应用测试；owner 边界审查 |
| 类型 | `WorkerThread` | `hammer-runtime`，当前 `worker_thread.rs` 仅有配置类型的平台方法 | Q2 确认新增与 `vlib_worker_thread_t` 对应的线程描述，包含 thread-zero 角色、身份、placement 和启动事实；barrier 握手与 refork completion 由线程子系统持有；字段与内部存储形状待细化 | 从 GlobalMain 迁出线程生命周期；与 DataPlaneMain 分离；无持久化对象 | 真实 worker 启动、thread-zero、barrier/refork 与进程退出测试 |
| 类型 | `Process<S, F>` | `hammer-runtime::process` | 新增唯一具体 Process execution type；字段 `node_index: NodeId`、`state: S`、`future: F`；实现 `Future<Output = RuntimeResult<()>>` 供 Tokio 调度，方法见目标 Rust 设计 | 替换 ProcessEntry/ProcessFuture/ProcessContext 等旧模型；所有 Process 宏和消费者同步迁移；不保持旧 ABI | 具体 Future 构造、poll、挂起/恢复/退出及 main 短借用验证 |
| 类型 | `FileMode` | `hammer-runtime::file`，`pub(crate)` | 按用户要求新增 `Sync` / `Async(AsyncFileMain)` 互斥 enum；DataPlaneMain 只持有 `file_main: FileMode`；Sync 使用现有全局 FileMain，Async 按值拥有现有 adapter；无新增 forwarding API | 迁移 GlobalMain control File 状态与 main/worker polling 分支；不改变 TOML 或复制 registry | 主线程 async readiness、worker 同步 polling、variant 选择与关闭生命周期 |
| 复用类型/字段 | `Bitmap`；`ThreadMain::{cpu_core_bitmap,cpu_socket_bitmap}` | `hammer-infra::bitmap` -> `hammer-runtime` | 恢复两类资源集合，直接使用已有 `Bitmap<usize>` 与集合操作；不新增 bitmap 类型，不用裸 `Vec<u64>`，不以配置列表代替可用资源集合 | 启动期探测、保留 CPU 排除、placement 校验共同迁移；不改变外部配置键 | 稀疏 CPU/NUMA 编号、保留 CPU 排除、不可用 CPU 拒绝、目标平台探测 |
| 类型 | `UnixMain` | `hammer-runtime`，当前不存在 | 新增与 VPP `unix_main_t` 对应的 Unix host-process authority；职责包括 Unix 启动入口、信号/退出、运行时路径和错误/日志策略；IPC listener 归 daemon IPC owner；不持有 DataPlaneMain back-pointer | daemon 启动链迁移；内部运行时类型，无持久化对象；现有 TOML keys 保持，映射见 Q13/F6 | Unix startup、signal/exit、error-path 和 GlobalMain 独立性测试 |
| API | 无（本轮未确认新增公共 API 的具体签名） | main lookup、thread registration、image handoff 的签名待定 | 记录职责与允许的访问语义；新增类型不自动授权公共构造器、pointer accessor、queue facade 或 registry | 新签名必须连同消费者、错误和借用范围列出后才能实现 | API inventory 与实现 diff 对照 |

### 修改

| 类别 | 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- | --- |
| 类型 | `GlobalMain` | `hammer-runtime::global_main` | 从混合 owner 改为进程级 GlobalMain；只保留七类 registration inventory、`init_functions_called` 和 process metadata；worker creation、thread handles、barrier/refork coordination 及其他不属于其 authority 的 owner state 迁出 | Rust 内部 API 和启动编排破坏性迁移；无持久化格式 | workspace compile、owner 边界审查、真实多 worker 生命周期测试 |
| 字段 | `GlobalMain::init_functions_called`、`DataPlaneMain::worker_init_functions_called` | 各自的 registration progress owner | 撤回草案裸指针集合，复用 `Bitmap`；注册阶段建立稳定 callback 索引，同一 callback 的 global 去重与逐 worker 独立进度必须保留；调用前置位，错误不清位 | 当前名称集合及 per-category progress 一同迁移；不是直接把名称/函数地址 cast 成 bit 索引；无持久化迁移 | 跨列表重复 callback、重复/追加 image、early/ordinary config、失败重入和 worker 隔离的真实调用验证 |
| 类型 | `DataPlaneMain` | `hammer-runtime::data_plane` | 对齐每线程 `vlib_main_t`；index 0 是主线程并拥有现有 `AsyncFileMain`；移出 `registry`、全体配置转发、`publication`、`workers_updating_graph` 和共享 exit 转发；保留 per-main graph/trace/random/loop/worker-init 与本地退出事实；worker allocation 在 launch 前建立 | 所有 worker/config/init/Process/Buffer 借用消费者复核；不为传递而强制 `Send`；无持久化对象 | 主线程 `AsyncFileMain`、线程绑定、借用排他性、per-main 状态与真实启动测试 |
| 类型 | `NodeMain`、`Process` | `hammer-runtime::{node,process}` | 依据 V4/H6/F23/F25/F27/F28 将 Process 调度机制、event、timer、restore 与 suspended state 对齐到 thread-zero DataPlaneMain/NodeMain；新增具体 `Process` 类型并实现 `Future` 交给 Tokio 调度；复用现有 Node registration / `NodeId` 作为身份；事件和恢复细节收进 `Process` 与 `NodeMain` 内部状态；清除依赖 GlobalMain owner、`ProcessContext` 和共享 registry 的调用路径；Process callback 改为只接收其 owner `DataPlaneMain`；具体 Process state 字段与 poll 生命周期尚待细化 | Binary API、stats collector、IP reassembly 等 Process 消费者共同迁移；不新增第二个主线程聚合 owner；无持久化对象 | 主线程 Process 启动/恢复/退出、signal/wake、Future poll 及 owner-bound 访问验证 |
| 类型 | `InitFunction`、`ConfigFunction` | `hammer-runtime::init` | 六类 lifecycle declaration 使用同一 `DataPlaneMain` callback 契约；删除单独的 `WorkerInitFunction`，worker-init 只由 GlobalMain 的对应 list 和 per-main progress 区分；config 单独表达输入与 early 属性；最终 Rust record/linkage 由 direct image handoff 提供 | macro、static declarations、RegistrationImage 和所有 hook 共同重编译；不承诺旧 ABI | 六表分离、同 callback 全局去重、per-worker 去重、config early/普通分派测试 |
| 类型 | `PluginMain` | `hammer-runtime::plugin` | 与 GlobalMain 独立表达 plugin image、metadata、load order 和 code lifetime；负责从 image 取得全局 registration variable 的直接引用并交给 registration owner，不复制 registration owner | runtime/plugin callers migrate to independent owner | plugin load/lifetime and direct registration-reference publication test |
| 类型 | `RegistrationImage` | `hammer-runtime::registration` | 继续仅作声明引用的 image ABI carrier；补齐 worker-count-change/API-init hook 交付；early 与普通 config 进入同一 owner list；删除重复复制声明的 materialization；其他 graph/stats/binary-method 声明依实际 owner 分类 | `new`、`new_with_stats`、所有 image 宏和 plugin image 的参数共同变化，最终形状待定；无旧 ABI 兼容承诺 | real image registration identity and dispatch test |
| 类型 | `ThreadOwned<T>` | `hammer-infra::thread_owned` | 从 install-once、原子发布和运行时 borrow/clear 槽改为构造即绑定当前线程的泛型 per-thread access abstraction；owner 线程可独占可变访问，非 owner 只能共享访问（若 `T` 的契约允许）；不参与 worker-index reference table | generic callers migrate; no serialized state | compile-time access-boundary checks and owner-thread lifecycle test |
| API | `InitFunction`, `ConfigFunction`, main-loop hook signatures | `hammer-runtime::init` | hook callback input 改为执行线程的 `DataPlaneMain`，对应 VPP `vlib_main_t *`；具体 Rust receiver/lifetime 待确认 | generated plugin hooks and call sites recompile | callable init/config/enter/exit lifecycle test |
| API | `init_function`、`worker_init_function`、`main_loop_enter_function`、`main_loop_exit_function`、`config_function`、`early_config_function` | `hammer-component-macros` | adapter 的 main 参数改为 DataPlaneMain；当前 `Arc<T>` 注入和返回值自动 `registry.set` 是明确待迁移消费者，不能通过新 main getter 保留；typed serde 输入及 owner 发布的最终宏契约待细化 | runtime stats、Binary API、service 和插件声明一起迁移；缺失 owner 的 typed failure 必须由实际 owner 定义，不能吞为成功跳过 | 宏编译检查与真实生成 hook 调用；配置错误及缺失依赖路径验证 |
| API | `run_init_functions`、`run_config_functions`、`run_main_loop_enter`、`run_main_loop_exit`、`run_worker_init_functions`、`run_stats_registrations` | `hammer-runtime::init` | 从 PluginMain copied Vec / GlobalMain receiver 迁到实际 registration owner 与执行线程 main；config progress 改为调用前记录，main/worker called 状态按 VPP 区分；stats 不并入七类 hook 的进度集合 | daemon/bootstrap、plugin additive load、stats 注册消费者同时迁移；具体错误处理策略见未决问题 | 实际调用顺序、错误后不重复调用、worker 隔离与 stats owner 注册测试 |
| API | `start_workers(&mut GlobalMain) -> RuntimeResult<()>` | `hammer-runtime::start_workers` | 改为执行线程 DataPlaneMain hook 输入并使用独立线程 owner；预先构造 worker main，随后一次性移交启动；worker-init 位于启动屏障释放后，后续 initial barrier 迁回主 loop | 调用方为生成 enter adapter；startup error/rollback 策略待确认；无 public raw-pointer surface | startup order, barrier access, and worker lifetime test |
| API | `GlobalMain` lookup/access surface | `hammer-runtime::global_main` | 收窄到 GlobalMain 自身的 process-global access；per-thread main access 回到持有 DataPlaneMain 的 owner；不保留 GlobalMain 对 scheduler、registry、plugin、barrier、queue 或 DataPlaneMain owner 的 broad getter | all callers migrate to owner-specific access; no pointer/table installation API | public API audit and worker-index lifecycle test |
| API | PluginMain registration handoff | `hammer-runtime::plugin` -> GlobalMain registration owner | PluginMain 取得并交出 process-global registration variable 的直接引用；GlobalMain 持有对应 process-wide inventory | plugin image ABI and registration handoff migrate together | real image registration test |
| API | `RegistrationImage::{new,new_with_stats}`、`__declare_registration_image!`、`hammer_plugin!` | runtime image ABI 与宏 | 当前 `&[declaration_value]` 物化改为直接引用原始声明；hook 字段按七表契约交付；构造参数/宏语法的具体差异待细化 | runtime、service 和所有 plugin image 声明与导出一起重编译；无兼容转发 | real image 声明身份、重复声明与进度验证 |

### 删除

| 类别 | 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- | --- |
| 类型/字段 | `GlobalMain` 内的 `ControlThread` owner state | `global_main.rs` | 从 GlobalMain 移出 | runtime/daemon owner migration | compile and lifecycle ownership audit |
| 类型 | `ControlThread` | `hammer-runtime::control_thread` | 删除独立调度 owner；其 Process scheduling、LocalSet 和 lifecycle state 按 Q18 迁入 thread-zero DataPlaneMain/NodeMain 的 Rust 设计 | Process callers migrate together; no compatibility type or forwarding facade | main-thread Process lifecycle and owner-bound scheduling test |
| 类型 | `Worker` | `hammer-runtime::config::worker` | 删除 Hammer 自定义聚合配置类型；现有 TOML 字段按职责解析并直接传给 ThreadMain、WorkerThread、Buffer 和 App Session owner | 解析入口、平台 setup、Buffer/AppSession 配置调用点迁移；外部 TOML keys、aliases、defaults 保留 | 配置解析与 owner 安装顺序测试 |
| 类型 | `ProcessContext` | `hammer-runtime::process` | 删除通用 Process 上下文类型；Process callback 的唯一 runtime owner 输入改为 `DataPlaneMain`，不以新上下文类型替代 | Process declarations, macro adapters, and all Process consumers migrate together; no compatibility alias | generated callback compile and Process lifecycle test |
| 类型 | `ProcessHandle` | `hammer-runtime::process` | 删除独立通用 Process handle；Process signal identity 和队列访问回到 `DataPlaneMain`/`NodeMain` 的 owner-defined API | signal producers and Process consumers migrate; no compatibility alias | main/node signal and wake lifecycle test |
| 类型 | `ProcessFuture` | `hammer-runtime::process` | 删除 `Pin<Box<dyn Future<Output = RuntimeResult<()>> + 'static>>` 类型擦除别名；由具体 `Process` 实现 `Future` 并交给 Tokio 调度 | Process registration and scheduler consumers migrate; no compatibility alias | compile-time Future implementation and Tokio lifecycle test |
| 类型 | `ProcessEntry` | `hammer-runtime::process` | 删除独立 Process registration carrier；Process registration复用现有 Node registration / `NodeId`，具体 `Process` 由静态 declaration 创建 | Process macro and image declarations migrate; no compatibility alias | static node registration and Process construction test |
| 类型 | `ProcessEventBatch`、`ProcessWake` | `hammer-runtime::process` | 删除独立公共事件/wake 类型；event batching、clock wake 和 restore reason 收进 `Process`/`NodeMain` 内部状态 | Process consumers migrate to owner APIs; no serialized data | event, timer and restore behavior test |
| 类型/字段 | `GlobalMain` 内的 `RuntimeRegistry` owner state | `global_main.rs` | 从 GlobalMain 和 DataPlaneMain 移出 | registry consumers migrate to its owning authority | workspace compile and registry lifecycle test |
| 类型 | `RuntimeRegistry` | `hammer-runtime::registry` | 删除通用 typed registry carrier；Q19 要求依赖与发布回到实际 owner | Process, init macro, stats, plugin and service consumers migrate; no compatibility registry | typed dependency lifecycle and absence of cross-owner carrier |
| 字段 | `DataPlaneMain::{registry,worker_config,worker_control_queues,publication,workers_updating_graph}` 及共享 `main_loop_exit_now/main_loop_exit_status` 引用 | `data_plane/main.rs` | Q4 确认删除跨 owner 存储；每线程退出状态另按 `vlib_main_t` 保留，不删除其领域语义 | worker loop、generated init、service/plugin 配置与调度调用点一起迁移；无序列化字段 | per-main state、启动/退出、registry 消费者与同步边界审查 |
| API | `DataPlaneMain::{install_global_control,registry,worker_barrier,configured_worker_count,worker_config,set_worker_config}` | `data_plane/worker.rs` | 删除 global-control 安装与 forwarding；线程数量/配置查询归 ThreadMain，barrier 由线程子系统及现有 scope macro 协作 | start_workers、宏 adapter、File/session/plugin 调用方迁到 owner-defined API；无兼容 getter | workspace 编译、实际 owner 初始化与 main/worker 访问验证 |
| API | `Worker::{create_runtime,apply_current_thread_setup}` | `data_plane/config.rs`、`worker_thread.rs` | 随 `Worker` 聚合配置类型一并删除；DataPlaneMain 构造和 OS-thread setup 由 ThreadMain/WorkerThread 直接调用已有字段与平台 API | daemon、start_workers、Buffer owner 和线程启动调用点迁移 | 编译期边界、配置应用与 worker-local 资源初始化测试 |
| API | `ProcessContext::require`、`RuntimeRegistry::{new,get,require,set}` 及 init 宏中的 registry injection/publication | `process.rs`, `registry.rs`, `hammer-component-macros` | 删除通用 registry lookup/set 和 `ProcessContext` 依赖；各消费者改用其实际 owner 的显式 typed dependency/API，Process callback 只接收 `DataPlaneMain` | 生成的 hook、Process Node、stats、service/plugin 初始化一起迁移；无兼容 carrier | typed dependency compile/lifecycle tests and migration audit |
| 字段 | `ipc_listener` | `global_main.rs` | 删除 IPC Listener state and its GlobalMain accessors | daemon IPC owner retains any required listener lifecycle | daemon IPC startup/shutdown test |
| API | `GlobalMain::{main_loop_enter,configure_early,load_plugins,init_control,start_process_nodes,process_handle,run_processes_until,shutdown_process_nodes,close}` | `global_main/{lifecycle,plugins,registrations}.rs` | 删除 GlobalMain orchestration surface | callers migrate to runtime/daemon owner; no forwarding methods retained | public API compile check and lifecycle behavior test |
| API | `GlobalMain::{install_current,with_current,uninstall_current}` and current DataPlaneMain TLS accessors | `global_main/control.rs`, `spawn.rs` | 删除越过 owner boundary的 TLS/global convenience access | callers use owning runtime/thread paths; exact replacement remains open | compile-time API absence plus real lifecycle access test |
| 状态 | per-category `called_*` sets and GlobalMain-owned worker coordination/completion state | `global_main.rs` and submodules | 删除与 VPP distinct inventories、worker authority或统一 `init_functions_called` 不相符的重复状态 | dispatch and worker coordination migrate by semantic owner | registration dispatch and barrier/refork tests |
| API | `GlobalMain::{new,new_configured}` | `global_main/metadata.rs` | 删除 public construction entry；GlobalMain 只能由 process-global startup path 安装，不能由 daemon 普通实例或 fixture 构造 | all current constructors and tests migrate to real process-global startup | real process startup and uniqueness verification |
| API | `GlobalMain::{ensure_main_thread,ensure_main_thread_with_barrier}` and free functions with same names | `global_main/control.rs`, `lib.rs` | 删除 GlobalMain-bound thread/barrier convenience checks；由 VPP main access and owning scheduler/barrier path enforce | callers migrate to owner-local checks | public API audit and wrong-thread behavior at owner boundary |
| API | `GlobalMain::{file_main,poll_file_readiness}` | `global_main/control.rs` | 从 GlobalMain 移出；thread-zero DataPlaneMain 的 `FileMode::Async` 调用现有 AsyncFileMain；worker 的 `Sync` 调用现有 FileMain，ControlThread 删除 | main/worker loop、File owner 和 readiness 调用点迁移 | file readiness lifecycle test |
| API | `GlobalMain::{thread_index,configured_worker_count,worker_config,apply_worker_config}` | `global_main/metadata.rs`, `workers.rs` | 删除或移到 DataPlane/worker/config owner；GlobalMain 不作为 worker execution object | startup and worker callers migrate | worker configuration and index lifecycle test |
| API | `GlobalMain::{control_thread,data_plane_main,data_plane_main_mut,registry,worker_barrier}` | `global_main/metadata.rs` | 删除 owner-leaking getters；只保留 VPP global-main semantic lookup | callers migrate to explicit owners | compile-time API audit and borrow-boundary tests |
| API | `GlobalMain::{schedule_on_worker,install_worker_control_queues,retain_worker_threads}` | `global_main/workers.rs` | 删除 GlobalMain worker-control forwarding；队列、thread handle、worker execution authority归 worker-thread owner | worker lifecycle and control handoff migrate | worker startup/control queue lifecycle test |
| API | `GlobalMain::{loaded_plugins,plugin_main,plugin_main_mut}` | `global_main/plugins.rs` | 删除 GlobalMain plugin forwarding；plugin metadata and registration handoff由 PluginMain owner直接提供 | plugin callers migrate to PluginMain owner | plugin lifetime and registration test |
| API | `GlobalMain::{prepare_worker_publication,publish_worker_graph_refork,wait_for_worker_graph_refork}` | `global_main/publication.rs` | 删除 GlobalMain worker publication/completion orchestration；worker-thread authority拥有 barrier、refork request 和 completion | graph publication callers migrate to worker authority | barrier release/refork completion test |
| API | `GlobalMain::{set_ipc_listener,take_ipc_listener}` | `global_main/control.rs` | 删除 IPC listener accessor and state | daemon IPC owner owns listener lifecycle | daemon IPC lifecycle test |
| 状态 | `GlobalMain` public fields `registry`, `main_loop_exit_now`, `main_loop_exit_status` and all current public/in-module owner fields | `global_main.rs` | 删除或移到 owning authority；GlobalMain 不暴露跨 owner mutable state | internal callers migrate; no compatibility aliases | visibility audit and workspace compile |
| API | `PluginMain::collect_registrations` and copied registration accessors | `plugin.rs` | 删除把每个 image 的 registration slice 复制成 `Vec` 的 API；PluginMain 只负责 image/code lifetime 和 direct-reference handoff | plugin and registration callers migrate to owner-held inventories; no compatibility alias | registration ownership audit and real image dispatch test |
| API | `ThreadOwned::{install,borrow_mut,clear}` | `hammer-infra::thread_owned` | 删除运行时安装、原子状态检查和 clear API；值由构造函数提供，通过 owner-thread 的 `&mut` 和非 owner 的共享 `&` 访问；不承担 `start_workers` 的 worker-main registration/lifetime | worker lifecycle callers migrate to construction and ordinary borrows; no compatibility alias | API absence and owner-thread access checks |

No serialized or persistent data migration is currently identified. The
registration image ABI and generated hook function signatures are compatibility
surfaces and require an atomic daemon/plugin rebuild unless a later decision
defines an explicit ABI transition.

This inventory records approved ownership changes and the inspected migration
surface; rows marked pending do not approve a concrete new Rust signature.
The exact final disposition of `RuntimeRegistry`, duplicate
`WorkerInitFunction`, `WorkerPublication`, `ProcessContext::require`,
`PluginMain::binary_api_method`, and DataPlaneMain construction/readiness
facades must be reconciled with every consumer before this ADR can be accepted.
Their current locations must not be used as justification for moving the same
generic forwarding surface into ThreadMain or WorkerThread.

## 实现核对项（非提问）

下表保留历史问题与已有回答作为设计依据，不再启动新的 grill。
后续实现必须采用最新字段设计和明确修正；需要补全的技术证明列入任务 issue。

| ID | Decision | Verified prerequisite | Recommendation / status |
| --- | --- | --- | --- |
| F1 | Does ThreadMain support plugin-declared auxiliary OS threads without a DataPlaneMain in this change? | VPP `vlib_thread_registration_t.no_data_structure_clone` makes thread descriptions and main counts different; current Hammer start_workers launches Data Workers only | **已确认：**完全对齐 VPP，覆盖 auxiliary OS threads；线程注册与 hook 注册保持独立 |
| F2 | If a worker-init callback returns an error after startup-barrier release, does Hammer log and enter the worker loop as this VPP path does, or terminate the process startup? | VPP `main.c:2040-2046` reports the error and enters the loop without running the remaining init hooks. Current Hammer exits the worker and fails startup, but `abort_workers` only logs the callback error and returns the barrier-phase error (H8). Both dispatchers mark the attempted callback before invocation, with different identity keys | **已确认：**对齐 VPP，报告错误后继续进入 worker loop；不因该错误终止进程启动 |
| F3 | Does splitting Worker configuration across owners change existing TOML keys now? | `config::Worker` is serialized/deserialized and includes buffer, handoff, control, app_session, scheduler and platform fields; moving Rust ownership alone does not decide the external input format | **已确认：**保留现有 TOML keys、aliases 和 defaults；解析后的配置再交给实际 owner |
| F4 | Should the thread-registration model expose VPP's `no_data_structure_clone` distinction, with an auxiliary WorkerThread that has no DataPlaneMain? | Q8 requires complete VPP alignment; VPP keeps thread descriptions/counts separate from `vlib_mains` and skips main allocation for non-cloning registrations (`threads.c:332-342`, `:643-669`) | **已确认：**暴露该 distinction；辅助 entry function 独立于 DataPlaneMain hooks |
| F5 | How should a worker-init error be recorded after the worker continues, while preserving VPP's nonfatal startup behavior? | Q9 fixes the lifecycle: report the error, skip the remaining callbacks in that pass, and enter the worker loop; current Hammer has typed `RuntimeError` values but its startup join treats worker exit as fatal (H8) | **已确认：**暂不记录或持久化；先定义 owner-local typed error，具体处理后置 |
| F6 | With TOML compatibility retained, should one existing `Worker` document be parsed once and then projected into owner-local runtime state? | Q10 preserves keys/aliases/defaults; current `config::Worker` is the single serialized section containing buffer, handoff, control, app-session, CPU, scheduler, and NUMA fields | **已确认：**单次解析，再按字段职责投影到实际 owner，不复制 serialized schema |
| F7 | `UnixMain` 应由哪个 owner 持有？是否作为独立的 process-global host authority，与 GlobalMain、ThreadMain 并列？ | VPP `unix_main_t unix_main` 独立于 `vlib_global_main`; 当前 Hammer 的 Unix startup 在 daemon，IPC/control FileMain 分散在 GlobalMain/FileMain | **已确认：**由 `hammer-runtime` 持有独立 UnixMain；daemon 只调用其启动入口，GlobalMain 不重新吸收 Unix host state |
| F8 | UnixMain 本轮是否覆盖 VPP `unix_main_t` 的全部 Unix-facing state，包括 control socket、startup/runtime/pid paths、signal/exit、error history、terminal/pager 与 logging policy？ | VPP `unix.h:15-91` 定义这些字段；Hammer 当前只有部分对应物，Binary API listener 目前位于 GlobalMain | **已确认：**覆盖现有 Hammer 具有对应语义的状态，并将 IPC/control listener 归 UnixMain；没有 Hammer 产品语义的 interactive CLI/pager 字段暂不凭空新增 |
| F9 | 是否将 daemon `main` 的现有两阶段配置、Main Heap、logging、main-thread setup、GlobalMain/ThreadMain 初始化和最终 main-thread loop 收束为 `UnixMain` 对应的 `vlib_unix_main` 启动链？ | VPP `vlib_unix_main` 初始化 GlobalMain metadata、early plugin/config、thread zero 和 main entry；当前 Hammer `main` 直接编排这些阶段并调用 GlobalMain lifecycle methods | **已确认：**由 UnixMain 负责 host startup sequencing 和 main-thread entry；GlobalMain 只负责其 VPP global authority，ThreadMain/WorkerThread/DataPlaneMain 按各自生命周期接入 |
| F10 | UnixMain 的 Rust 启动入口是否采用一个对应 `vlib_unix_main(argc, argv, startup_config)` 的一次性 process entry，且不暴露 host-state forwarding getters？ | Q14-Q16 已确认 UnixMain owner、状态范围和启动链；精确 Rust entry signature 与安装生命周期仍未决定 | **已确认：**采用一次性 UnixMain startup entry；host state 通过 owner-local 生命周期使用，不能由 GlobalMain 转发 |
| F11 | ControlThread 在新边界中应如何归属？它是否继续作为独立调度 owner，还是并入 UnixMain 或 thread-zero DataPlaneMain/NodeMain？ | VPP `vlib_unix_main` 进入 thread-zero 的 `vlib_main_t` loop；当前 Hammer ControlThread 持有 Tokio LocalSet、Process 调度和 registry | **已确认：**Process 调度归 thread-zero DataPlaneMain/NodeMain；UnixMain 只负责 host startup；删除独立 ControlThread owner |
| F12 | `RuntimeRegistry` 是否从 GlobalMain/DataPlaneMain 和 init 宏注入链中删除，改由实际 owner 的显式依赖/API 连接各消费者？ | VPP global/unix/thread/main 结构没有对应的通用 typed registry；当前 `ProcessContext`、宏 adapter、stats 和插件 init 依赖该 registry | **已确认：**删除通用 registry carrier 和 forwarding getter；由具体 owner 定义 typed dependency 与发布边界，不能把 registry 搬进 UnixMain/ThreadMain/WorkerThread |
| F13 | “更 Rust 一点”的 UnixMain 入口是否应由一个拥有启动状态的 Rust value 执行一次，并以 typed `Result`/exit status 结束，而不是复刻 `extern` 参数、全局变量和 TLS？ | Q17 已确认 UnixMain 保持 process-global；Q17 的 Rust API要求不改变变量/storage authority；Q16 已将 host startup 和 main-thread entry 归 UnixMain | **已确认：**保持 process-global UnixMain authority；只将 callable API 设计为 Rust-safe 的一次性入口与显式借用，不暴露 raw pointer 或 TLS facade |
| F14 | Q18 确认后，Process scheduling 是否完全进入 thread-zero DataPlaneMain/NodeMain，同时继续使用 Tokio？ | VPP `vlib_unix_main` 最终进入 thread-zero `vlib_main_t` loop；当前 Hammer `ControlThread` 持有 Tokio runtime、LocalSet 和 `Pin<Box<dyn Future>>` | **已确认：**保留 Tokio；将 Tokio runtime/LocalSet 及 Process scheduling 放入 thread-zero DataPlaneMain/NodeMain，具体静态 Process 表示仍待确认 |
| F15 | RuntimeRegistry 删除后，init/config 宏是否改为由声明显式列出其 owner-defined typed dependencies，并通过 owner API 发布结果？ | Q19 要求对齐 VPP；当前宏自动从 registry 查找 `Arc<T>` 并把返回 `Arc<T>` 写回 registry；VPP hook 只接收执行线程 main | **已确认：**宏只生成显式参数和 owner-local publication 调用；缺失依赖成为具体 owner 的 typed error，不使用 generic carrier |
| F16 | UnixMain 的 Rust API 是否只提供 process-global authority 的安全访问和 typed startup/shutdown 操作，不新增 VPP 没有的 current-thread UnixMain lookup？ | Q17/Q20 保持 UnixMain 的 global storage/authority，同时要求 API 更 Rust；VPP `vlib_unix_get_main` 返回唯一 UnixMain，current-main lookup 属于 `vlib_main_t` | **已确认：**UnixMain 只暴露共享 process authority 和明确的 startup/shutdown borrow；不增加 TLS、current-thread UnixMain 或可逃逸的全局可变引用 |
| F17 | Tokio runtime/LocalSet 在 thread-zero 目标中由 DataPlaneMain 持有，还是由 NodeMain 持有并由 DataPlaneMain 驱动？ | Q18 已将调度 owner 归 thread-zero DataPlaneMain/NodeMain，Q21 要求继续使用 Tokio；VPP 将 Process table/restore/suspended state 放在 `vlib_main_t.node_main`（V10） | **已确认：**Process scheduler state 跟随 thread-zero DataPlaneMain 的 NodeMain；Tokio 继续使用，具体 runtime 字段位置按 Rust ownership 确定，不再创建 ControlThread |
| F18 | init/config 宏的显式依赖是否采用函数签名中的具体 owner 类型和参数，而不是宏参数中隐藏 lookup 规则？ | Q19/Q22 要求对齐 VPP；VPP registration callback 由执行 main 作为明确输入，当前宏把 `Arc<T>` lookup/publication 隐藏在 adapter | **已确认：**依赖成为生成函数的显式 typed 参数，由 owner-defined registration/publication API 处理结果；不保留 implicit registry injection |
| F19 | GlobalMain/DataPlaneMain 的 main access 是否继续由 GlobalMain 提供？ | Q6 已确认访问必须服从 owner borrow；当前 Hammer 使用 TLS raw-pointer facade | **已修正：**GlobalMain 只提供自身的 process-global access；`DataPlaneMain` 的 `current`、`first`、`by_index` 访问回到持有 main 的 owner，不新增 GlobalMain pointer/table facade |
| F20 | Process Node 是否保留 Tokio async 执行，但把 Process state、事件、restore 和 timer ownership 对齐到 thread-zero DataPlaneMain/NodeMain，并让 callback 接收该 owner 的显式借用？ | V10 证明 VPP Process state 属于 `vlib_main_t.node_main`；H10 显示当前 `ProcessContext`、boxed future 和 ControlThread 是独立模型 | **语义已确认：**保留 Tokio executor，去除 ControlThread/registry 作为隐含 owner；按 F23，Process callback 只接收 thread-zero `DataPlaneMain`，不保留 `ProcessContext`；具体 future 存储和轮询边界仍待确认 |
| F21 | 删除 RuntimeRegistry 后，plugin image handoff 是否仍保留声明引用的 direct image boundary，并由每个实际 owner 定义具体 registration/dependency handoff？ | Q22/Q25 要求对齐 VPP；PluginMain 当前通过 RegistrationImage 和 copied Vec 向 GlobalMain/dispatch 提供声明 | 建议保留 image/code lifetime authority 与 direct declaration references，删除 copied registration universe；各 owner 直接接收其声明和 typed dependency |
| F22 | Rust 中四类 main access 的共享与可变边界如何确定？ | Q26 已纠正 lookup 与 Barrier 的关系；VPP lookup 本身不要求 Barrier；当前 Hammer 的 TLS facade 允许越过 owner 边界 | **已确认：**`global`、`first`、`by_index` 使用共享借用；只有当前 owner 可以获得 `&mut DataPlaneMain`；具体签名和生命周期证明另列为实现 frontier |
| F23 | Process callback 在保留 Tokio async 执行的前提下，是否删除 `ProcessContext`，只接收其 owner 的 `DataPlaneMain`？ | 当前 Hammer 的 `ProcessEntry` 使用 `fn(ProcessContext) -> ProcessFuture`；VPP Process state 属于 thread-zero `vlib_main_t.node_main`；Q21 已锁定 Tokio | **已确认：**删除 `ProcessContext`；Process callback 只接收 `DataPlaneMain`，Tokio 继续执行 async callback；具体 future 的存储、轮询和生命周期边界仍待确认 |
| F24 | `DataPlaneMain` 借用是否只覆盖一次 Process callback/step，禁止 future 跨 `.await` 持有该借用？ | 当前 `DataPlaneMain` 含线程绑定的 `Rc/RefCell` 状态；Tokio future 可能跨 await；F23 已删除 `ProcessContext` 并保留 async callback | **已确认：**采用一次 callback/step 的短借用；future 不持有 `DataPlaneMain` 借用跨越 `.await`；需跨挂起保存的 Process state 归 NodeMain |
| F25 | Process 的 event、timer、restore、suspended state 与 signal API 是否按 VPP 归入 `DataPlaneMain` 内的 `NodeMain`，并删除独立通用 `ProcessHandle`？ | VPP `vlib_main_t.node_main` 持有 Process table、restore queues 和 suspended frames；signal 通过 `vlib_main_t` 与 process node index；当前 Hammer 由 `ProcessContext`、`ProcessHandle` 和 ControlThread 分散持有 | **已确认：**按 VPP，Process 状态归 thread-zero `DataPlaneMain/NodeMain`；signal 通过显式 main/node owner API；删除独立通用 `ProcessHandle` |
| F26 | Process signal 的目标身份 | VPP signal API 使用 Process Node index；当前 Hammer 使用按名称查找的 `ProcessHandle` | **已按 VPP 确定：**使用注册后的 Process Node index；名称只作注册和诊断信息，不作运行时 signal handle 或 lookup key |
| F27 | 异构 Process future 的 Rust 表示 | 当前 Hammer 使用 `Pin<Box<dyn Future>>`；仓库禁止生产代码使用 `dyn` 动态分发；VPP 使用静态 Process Node 注册 | **已确认：**定义具体 `Process` 类型并实现 `Future`，由 Tokio 调度；删除 `ProcessFuture` 类型擦除别名，不集中维护所有 Process 的枚举 |
| F28 | Process 类型数量与边界的简化 | 前序草案拆出了 `ProcessEntry`、`ProcessEventBatch`、`ProcessWake`、`ProcessHandle` 和 `ProcessContext`；VPP 的这些事实由 `vlib_process_t` 与 `vlib_node_main_t` 的 owner 关系表达 | **设计已给出：**收敛为 `NodeMain` + `Process` 两个 Process runtime 类型，复用现有 Node registration / `NodeId`；其余类型删除或变为 owner 内部字段 |

The target design above replaces the earlier open design questions. The items
below are implementation proof tasks and do not introduce another interview
frontier:

- Reconcile the exact ThreadMain/WorkerThread fields, module visibility, and
  thread-registration declaration shape with the method tables above; avoid
  manufacturing an extra global table from unused VPP fields.
- Prove the confirmed pre-launch ownership transfer in Rust. Current
  DataPlaneMain/NodeMain contain Rc/RefCell state and lazily acquired Buffer
  cache borrows; an internal raw pointer does not itself prove these resources
  can cross threads. Record which state is initialized before launch and which
  is bound on the worker, plus failed-launch lifetime handling after F2.
- Verify the concrete safe main-access signatures in the target tables using
  the F22 shared/current-owner boundary and existing synchronization scopes.
  Check index-zero aliasing, nested calls, reference escape, refork's
  first-main read phase, and the repository's ban on new TLS.
- Verify six common lifecycle declaration lists and one config list, direct
  image references, callback identity, macro inputs/outputs, and owner-local
  replacements for all registry consumers. Keep API-init at API Process
  startup; do not confuse it with method registration.
- Verify UnixMain's exact field ownership and startup boundary against the
  methods above, including which current daemon and GlobalMain fields move
  there. Keep UnixMain independent from graph registration and per-thread
  packet state.
- Reconcile each inventory row and its transitive consumers against the target
  type and method tables before implementation. The ADR remains `proposed`
  until the user reviews this concrete design; no additional design question
  is being asked here.

## Verification matrix

These are planned Rust compile/behavior checks for implementation, derived
from the cited VPP code. They have not been run for this documentation update.
The scoped `third_party/vpp/test` search for init/thread/worker files did not
establish a dedicated test of all mapped authorities and seven hooks; it is not used
as proof of test absence across VPP.

| Check | Level / setup | Required observations | Decisions / evidence |
| --- | --- | --- | --- |
| Main uniqueness and worker lifetime | Real process startup with multiple workers | One GlobalMain and thread administration authority; index zero is main; worker addresses are established before launch and stable through process teardown | 2-4, 16, 29-31; V1-V3 |
| Bootstrap ordering | Real workers with observed hook phases | Worker-count-change runs under initial barrier; worker-init follows release; later initial barrier belongs to main-loop startup before processes | 20, 27; V3-V5 |
| Worker-init returned error | Failing callable worker hook in a startup subprocess | Verify the selected F2 loop/exit policy and that later hooks are not called after the error; preserve the worker's published-main lifetime while it continues; define the deferred typed error without recording it yet | F2/F5 are settled; V3, V5, H8 |
| Seven registration lists and identity | Builtin plus real dlopen/dlsym DSO integration | Image declarations retain identity, six lifecycle lists remain distinct, early/ordinary config share one list and global progress, per-worker progress is independent; callback errors follow F2 and are not silently replayed | 9, 12, 20-24, 35; V5, V7 |
| API-init lifecycle | Actual Binary API Process startup | API-init callbacks run after API setup, in the main thread's process path; API method entries remain separately owned | 20, 35; V5, H6 |
| Borrow and transfer boundary | Rust compile checks plus real worker/barrier behavior | No public raw pointer, force-Send shortcut, TLS aliasing facade, escaped mutable borrow, stale entry, or premature cache/resource binding | 28, 31, 34; V8, H7 |
| Refork and final exit | Real worker integration and shutdown subprocess | Main graph is stable through refork reads, completion occurs after each worker's clone replacement, final barrier stays held and no worker joins/restarts occur | 10, 22, 29, 32; V6 and `main.c:2003-2018` |
| Configuration and owner migration | Config and callable hook tests across runtime/service/plugins | F3's schema contract, ThreadMain count/placement installation, owner-local config/state availability, and no registry forwarding through new owners | 30, 32, 33; H1, H5, H6 |
| UnixMain startup boundary | Real daemon startup and shutdown path | Unix host state has one owner; startup metadata, Main Heap, logging, signals, control listener, thread zero, and final main-thread entry occur in the selected VPP-equivalent order | F7-F10, F16 settled; V9, H9 |
| Control scheduler and registry migration | Real process-node and init-hook lifecycle | Process scheduling and typed dependencies have their selected owners; no GlobalMain/DataPlaneMain/UnixMain forwarding registry remains; Tokio remains in the selected thread-zero owner | F11-F15, F17-F18 settled; F20-F21 pending; V4, V10, H1, H5, H6, H10 |
| Main access and Process ownership | Rust compile/lifecycle checks for main lookup and Process Nodes | Main references follow F22 shared/current-owner borrowing and publication synchronization rules; lookup itself does not require a barrier; Process state, event, timer, restore, and suspended state follow thread-zero NodeMain, Process signal targets use Process Node index, its callback receives DataPlaneMain for one step, and Tokio schedules the concrete Future-implementing Process without a cross-await main borrow | F19/F22/F23/F24/F25/F26/F27 settled semantically; exact signatures and Process lifecycle remain pending; V8, V10, H7, H10 |
| Migration closure | Workspace/macros/plugin compile and focused lifecycle checks | Old GlobalMain/DataPlaneMain APIs and copied registration consumers are gone; generated adapters, Process users and real image loading agree on the new ABI | All inventory rows; H1-H7 |

Existing checks inspected include `barrier.rs::final_barrier_remains_held_after_exit_hooks`,
`data_plane/worker.rs::worker_reforks_after_pending_dispatch`, and the real-image
example `hammer/examples/plugin_additive_load.rs`. Their present coverage does
not establish pre-launch main-table lifetime or seven-list dispatch. Future
verification uses Rust layout/compile/behavior checks, not compiled C probes
or source-text assertion tests. Runtime and lab test commands remain reserved
for the repository's final pre-commit gate; TUN/TCP lab execution remains CI-only.

## 依据与假设

| 分类 | 内容 |
| --- | --- |
| 当前项目事实 | H1-H10 record the inspected checkout: existing aggregate owners, actual startup/Process/error paths, value-copying image macros, registry injection, thread-bound main state, daemon-owned Unix startup, and the current ControlThread/boxed ProcessFuture scheduler. Current startup loses the callback error at the join boundary; Q12 defers its recording. ThreadMain, WorkerThread, and UnixMain are proposed additions, not existing implementation. |
| VPP事实 | V1-V10 and the exact seven-field table record definitions and executable call sites. Config shares global call-once progress; worker-init follows ordinary startup-barrier release; API-init is reached from API Process startup; thread registration is a separate ThreadMain concern; Unix startup is a separate UnixMain concern; Process state is held by `vlib_main_t.node_main` and scheduled from the main loop. |
| 既有 ADR 记录 | The pre-existing document attributes decisions 1-29 to earlier Q1-Q34/Q38. This update preserves that baseline as explicitly requested by follow-up Q1; those earlier turns are not independently reconstructed here. |
| 本轮用户确认 | Follow-up Q1: lock baseline; Q2: explicitly split WorkerThread, ThreadMain, and DataPlaneMain as in VPP; Q3: pre-launch allocation/address publication and process-lifetime worker ownership; Q4: remove shared control forwarding from DataPlaneMain; Q5: “对齐vpp”, not approval for arbitrary standalone owners; Q6: narrow access semantics; Q7: seven exact GlobalMain fields; Q8: include auxiliary VPP threads; Q9: report worker-init errors and continue the worker loop; Q10: retain TOML keys/aliases/defaults; Q11: expose the no-clone distinction; Q12: defer error recording but define the error; Q13: parse Worker once and project to owners; Q14: make UnixMain a separate hammer-runtime authority; Q15: include existing Unix-facing state and move control listener there; Q16: make UnixMain own host startup sequencing and main-thread entry; Q17: keep UnixMain storage/authority global while making only its API more Rust-like; Q18: retain Tokio for Process scheduling; Q19: align the registry replacement with VPP; Q20: keep UnixMain global and make only its API Rust-like; Q21: retain Tokio; Q22: align registry removal with VPP; Q23: no current-thread UnixMain lookup; Q24: derive Process ownership from VPP; Q25: align init/config with VPP; Q26: distinguish indexed lookup from Worker Barrier synchronization; Q27: keep Tokio with Process ownership in thread-zero DataPlaneMain/NodeMain; Q28: use direct VPP-aligned plugin image handoff; F22: `global`、`first`、`by_index` 使用共享借用，`current` 只允许当前 owner 的可变借用；F23: 删除 `ProcessContext`，Process callback 只接收 `DataPlaneMain`，Tokio 保留；F24: `DataPlaneMain` 借用只覆盖一次 callback/step，future 不跨 `.await` 持有该借用；F25: Process event/timer/restore/suspended state 归 `DataPlaneMain/NodeMain`，signal 走 owner API，删除通用 `ProcessHandle`；F26: Process signal 使用 Process Node index，名称只作注册和诊断信息；F27: 定义实现 `Future` 的具体 `Process` 类型交给 Tokio 调度，删除 `ProcessFuture` 类型擦除。These are recorded in decisions 30-63. |
| 推断 / 待确认 | F19/F22/F23/F24/F25/F26/F27's ownership semantics are confirmed. F28's compact `NodeMain` + `Process` model is the target design presented for review. Exact safe main access signatures, Process fields, callback result, Future polling/completion boundary, declaration linkage/callback identity, direct image/dependency handoff, transfer proof and final explicit dependency migration are implementation details to verify against the design. Q26's VPP lookup/barrier distinction and F25/F26's VPP Process owner and identity mapping are verified source facts. |
| 待验证 | Compile, real DSO, cross-thread borrow/lifetime, callback errors and config compatibility require the implementation verification matrix. Reading this trace does not establish that current code conforms or that the future design has passed tests. |

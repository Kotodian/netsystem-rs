# Vlib Main Migration Review

## Feature and Changed Surface

Issue #274 migration slice on branch `feature/274`. The changed surface is
primarily `hammer-runtime`, with call-site updates in `hammer-service`, the
daemon, registration macros, benchmarks, and the TCP/UDP/IP/TLS/QUIC/TUN
plugins.

This slice establishes the two runtime main objects requested by the issue:
`GlobalMain` for process-level coordination and `DataPlaneMain` for one
per-thread graph execution context. It removes the overlapping `Engine`,
`EnginePool`, worker seed/config carriers, `DataPlaneExecutor`, `DataRuntime`,
and `NodeResult` surfaces. Node callbacks now return unit, and worker
construction transfers concrete worker-owned parts through
`worker_parts`/`from_worker_parts`.

The stable packet-graph ABI is split under `hammer-core/src/graph/` and
`hammer-core/src/buffer/`. Runtime execution is split under
`hammer-runtime/src/data_plane/`, while `GlobalMain` ownership is split under
`hammer-runtime/src/global_main/`. The `data_plane.rs` files are facades only;
they do not define a third aggregate. The former `WorkerTaskContext` and its
custom local-task/join hierarchy were removed because they had no VPP or
Hammer main ownership role and no construction or call sites; worker control
now uses only the queue owned by `GlobalMain` and each worker's
`DataPlaneMain`.

## VPP Analog and Evidence

- `third_party/vpp/src/vlib/main.h:72-264` defines the per-thread
  `vlib_main_t` execution state, including loop counters, exit state, thread
  and NUMA identity, node graph, trace, and file-poll state. Hammer's
  `DataPlaneMain` owns the graph, buffer/frame, trace, handoff, timing and
  worker-local execution state. `GlobalMain` holds the main-thread instance
  plus process-level coordination; worker packet state is constructed inside
  the owning worker thread.
- `third_party/vpp/src/vlib/main.h:267-326` defines
  `vlib_global_main_t` and its `vlib_mains` collection. Hammer's
  `GlobalMain` is the process/control authority and creates worker-local
  `DataPlaneMain` instances; it does not expose a second pool wrapper.
- `third_party/vpp/src/vlib/threads.h:45-71` and
  `third_party/vpp/src/vlib/threads.c:1317-1497` define worker barrier
  acknowledgement, release, and deferred node refork. Hammer retains this in
  `WorkerBarrier`, `WorkerPublication`, and the graph refork completion path in
  `GlobalMain`.
- `third_party/vpp/src/vlib/node.h:101-172,401-549` defines node
  registration, pending frames, next-frame ownership, and mutable node runtime
  dispatch facts. Hammer keeps node identity/role/registration and buffer/frame
  values in `hammer-core/src/graph/` and `hammer-core/src/buffer/`; mutable
  scheduling and dispatch remain in `hammer-runtime::node` and
  `hammer-runtime/src/data_plane/`.
- `third_party/vpp/src/vlib/buffer.h:77-206` defines the cache-line header,
  current offset/length, flags, opaque areas, chain links and invalid index.
  Hammer maps these to `buffer/header.rs`, `flags.rs`, `opaque.rs`,
  `chain.rs`, `index.rs`, and `clone.rs`; pool storage and arena construction
  remain owner-specific rather than being duplicated in the main object.
- `third_party/vpp/src/vlib/error.h:22-50` keeps error descriptors and
  counters separate from node execution. Hammer's descriptor/index values are
  in `graph/error.rs`, while installation and counting stay in runtime node
  materialization.
- `third_party/vpp/src/vlib/threads.h:45-121` and
  `main.h:267-323` separate worker identity/barrier control from the per-thread
  main. Hammer mirrors this with `global_main/{workers,publication}.rs` and
  the worker-local `DataPlaneMain` construction path.
- `third_party/vpp/src/vlib/main.c:1433-1693` shows the fixed main-loop order.
  Hammer's `data_plane_main_loop` in
  `crates/hammer-runtime/src/main_loop.rs` preserves the barrier, file
  readiness, graph dispatch, reactor, and exit checkpoints while intentionally
  deferring a data-plane timer wheel.
- `DataPlaneMain::Clone` is a same-thread process-node handle: its graph and
  buffer internals use the existing owner-aware `Rc`/arena sharing, while
  worker construction still creates a distinct `DataPlaneMain` directly on
  the owning worker thread. It is not a worker pool or a second global main.

## Verdict

Aligned for the requested migration slice, subject to the final format,
diff, and workspace compile gate recorded below.

## Findings

### Non-blocking: compatibility facades and runtime-specific values

- **Evidence:** `hammer-core/src/data_plane.rs` and
  `hammer-runtime/src/data_plane.rs` remain narrow re-export facades. The
  canonical implementations are in graph/buffer and data-plane owner files.
- **Impact:** established module paths remain usable during migration without
  a second aggregate type or duplicate implementation.
- **Action:** keep facades limited to re-exports; do not add aliases, wrappers,
  or runtime-owned mutable state to them.

### Non-blocking: intentional Hammer differences

- `DataPlaneMain` uses Rust ownership and `Rc<RefCell<_>>` for state owned by a
  worker instead of VPP's raw pointers and vectors.
- Worker graph publication uses the existing barrier/completion protocol and
  does not expose VPP's global pointer array as shared mutable state.
- Data-plane timer-wheel dispatch is still deferred; the main loop documents
  this explicit difference rather than inventing a second timer owner.
- Tokio worker-control queues are a Hammer control-plane transport primitive;
  they are not presented as a VPP main/runtime aggregate. The removed
  `WorkerTaskContext` had no corresponding VPP domain role.

### Non-blocking: repository context files unavailable

The checkout contains no root `CONTEXT.md` or `docs/adr/` directory, despite
the repository instructions referring to them. This review therefore relies
on `AGENTS.md`, the current source, existing SDD records, issue #274, and the
vendored VPP tree. Historical compatibility promises require additional
records before making further public API removals.

## Commands Run

- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo check --workspace`
- Static search across crates confirming removed runtime symbols, including
  `Plain`, have no remaining matches.

No tests were added or run, per the user request. The final delivery gate is
`cargo fmt --all -- --check`, `git diff --check`, and `cargo check --workspace`.

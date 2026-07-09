# Task #38 Report: Move Graph Runtime Execution Ownership To hammer-runtime

## What I Implemented

- Moved graph runtime execution contracts from `hammer-adapter` into `hammer-runtime`:
  - `DataPlaneRuntime`, `DataPlaneRuntimeConfig`, worker seed/clone behavior, buffer/frame runtime helpers, handoff draining/enqueue APIs.
  - `DataPlaneInstructionSet`, `FrameBatchWidth`, and `BufferFrameBatchWidthPolicy` implementation.
  - `DataWorkerId`, `DataPlaneHandoff`, `DataPlaneHandoffWorker`, and runtime-private handoff slot/frame internals.
  - Node execution contracts and runtime state: `Node`, `DriverNode`, `InternalNode`, `NodeDescriptor`, `NodeEntry`, `NodeProcessFn`, `NodeResult`, `NodeRuntime`, `NodeRuntimeData`, `NodeRuntimeReady`, `NodeErrorCounters`, `NodeRuntimeStatsRow`, `NoopNode`, `process_frame!`, next resolution, current-node context, pending scheduling, driver polling/interrupt state, runtime stats, and node error encoding/decoding.
  - Runtime tracing surfaces: `DataPlaneTrace`, `PacketTrace`, `add_packet_trace!`, `TraceFormatter`, `TraceControlHandle`, `TraceControlPlane`, `TraceRecordSink`, `TraceRecord`, `TraceEntry`, `TraceInputPolicy`, and `TracePolicy`.
- Added direct `hammer-runtime` module ownership and top-level re-exports for the moved graph runtime contracts.
- Narrowed `hammer_runtime::adapter` to explicit remaining adapter OS/component/platform traits instead of wildcard re-exporting `hammer_adapter`.
- Removed adapter compatibility re-exports and adapter graph runtime modules/tests/bench entries for moved contracts while keeping `hammer-adapter` and its remaining OS-facing surfaces.
- Updated `hammer-component-macros` graph-node output to use:
  - `hammer_core::data_plane` for graph identity and buffer/frame primitives.
  - `hammer_runtime` / `hammer_runtime::node` for executable graph contracts.
  - Existing `hammer_adapter` component metadata paths where intentionally still adapter-owned.
- Updated runtime/service/test call sites to import runtime execution contracts from `hammer_runtime` and buffer/frame primitives from `hammer_core::data_plane`.
- Moved relevant adapter graph runtime tests and buffer bench into `hammer-runtime`.

## What I Tested And Results

- `cargo test -p hammer-runtime --test graph_runtime_owner` - passed, 1 test.
- `cargo test -p hammer-runtime --test graph_runtime_owner_guard` - passed, 1 test.
- `cargo test -p hammer-runtime` - passed, including 63 unit tests plus runtime integration tests/doc-tests.
- `cargo test -p hammer-component-macros` - passed, 5 unit tests and doctest compilation with ignored examples.
- `cargo test -p hammer-service` - passed, 151 unit tests plus service integration/doc-test targets.
- `cargo test -p hammer-adapter` - passed, remaining adapter tests.
- `cargo fmt --all -- --check` - passed.
- Source scan for old adapter graph runtime owner paths across runtime/service/app/IPC/macro/daemon crates found only the guard test's banned-path string literals.

The test commands still emit pre-existing warning noise, mostly deprecated `FlatHashTable`/`FlatHashKey` warnings and existing runtime/test unused/private-interface warnings. No command failed after formatting.

## TDD Evidence

### RED

- Command: `cargo test -p hammer-runtime --test graph_runtime_owner`
- Expected failure observed before implementation: the test could not compile because `hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, DriverNode, Node, NodeDescriptor, NodeProcessFn, NodeResult, NodeRuntimeData, PacketTrace, TraceControlPlane, TraceFormatter, TraceInputPolicy, TracePolicy, add_packet_trace, process_frame}` were not exported/owned by `hammer-runtime`.

### GREEN

- Command: `cargo test -p hammer-runtime --test graph_runtime_owner`
- Result: passed, `runtime_owner_registers_dispatches_traces_and_reports_stats ... ok`.
- Command: `cargo test -p hammer-runtime --test graph_runtime_owner_guard`
- Result: passed, `graph_runtime_owner_paths_do_not_point_at_adapter ... ok`.
- Broader GREEN verification: `cargo test -p hammer-runtime`, `cargo test -p hammer-component-macros`, `cargo test -p hammer-service`, `cargo test -p hammer-adapter`, and `cargo fmt --all -- --check` all exited 0.

## Files Changed

- `Cargo.lock`
- `crates/hammer-adapter/Cargo.toml`
- `crates/hammer-adapter/src/lib.rs`
- Removed moved adapter graph runtime sources/tests/bench:
  - `crates/hammer-adapter/src/buffer.rs`
  - `crates/hammer-adapter/src/handoff.rs`
  - `crates/hammer-adapter/src/instruction_set.rs`
  - `crates/hammer-adapter/src/node.rs`
  - `crates/hammer-adapter/src/node/next.rs`
  - `crates/hammer-adapter/src/trace/mod.rs`
  - `crates/hammer-adapter/tests/buffer.rs`
  - `crates/hammer-adapter/tests/buffer_frame_guard.rs`
  - `crates/hammer-adapter/tests/buffer_per_numa.rs`
  - `crates/hammer-adapter/tests/node_runtime.rs`
  - `crates/hammer-adapter/tests/process_frame.rs`
  - `crates/hammer-adapter/tests/trace_architecture.rs`
  - `crates/hammer-adapter/benches/buffer_alloc_free.rs`
- `crates/hammer-component-macros/src/lib.rs`
- `crates/hammer-runtime/Cargo.toml`
- `crates/hammer-runtime/src/data_plane.rs`
- `crates/hammer-runtime/src/engine.rs`
- `crates/hammer-runtime/src/graph/mod.rs`
- `crates/hammer-runtime/src/handoff.rs`
- `crates/hammer-runtime/src/instruction_set.rs`
- `crates/hammer-runtime/src/lib.rs`
- `crates/hammer-runtime/src/main_loop.rs`
- `crates/hammer-runtime/src/memory.rs`
- `crates/hammer-runtime/src/node.rs`
- `crates/hammer-runtime/src/node/next.rs`
- `crates/hammer-runtime/src/spawn.rs`
- `crates/hammer-runtime/src/trace.rs`
- `crates/hammer-runtime/benches/buffer_alloc_free.rs`
- Runtime tests under `crates/hammer-runtime/tests/`, including new `graph_runtime_owner.rs` and `graph_runtime_owner_guard.rs`.
- Import-only/runtime-owner path updates across `crates/hammer-service/src/` and `crates/hammer-service/tests/`.

## Self-Review Findings

- Adapter no longer exports moved graph runtime execution contracts; `rg` found no old adapter runtime owner paths outside the guard literals.
- `hammer_runtime::adapter` is explicit and does not wildcard re-export `hammer_adapter`.
- Macro-generated graph node code now uses `hammer_runtime` for executable graph contracts and keeps only component metadata on `hammer_adapter`, matching the brief.
- Runtime graph ownership is direct: moved modules are public under `hammer_runtime`, with top-level re-exports for common contracts.
- Remaining `hammer_adapter::PlatformInterface` imports in runtime protocol/socket-protector code are intentional OS-facing adapter surfaces retained until #39.

## Concerns

- The workspace still has pre-existing warning noise in required test commands. I did not clean unrelated warnings because this task is scoped to graph runtime ownership.
- I could not dispatch an external reviewer subagent because no reviewer/subagent tool was available in this environment; I performed the required self-review manually.

## Fix Amendment (Review Findings)

### What I Fixed

- Reworked `crates/hammer-runtime/tests/graph_runtime_owner_guard.rs` to recursively scan the brief-required ownership surfaces instead of a hand-picked file list:
  - `crates/hammer-runtime`
  - `crates/hammer-service`
  - `crates/hammer-app`
  - `crates/hammer-ipc`
  - `crates/hammer-component-macros`
  - `crates/hammer`
  - `crates/hammerctl`
- Anchored the recursive scan to `env!("CARGO_MANIFEST_DIR")` so the guard only walks the intended crate roots during test execution.
- Expanded the scan to cover Rust sources plus `Cargo.toml` manifests, while excluding the guard test itself.
- Strengthened the matcher so grouped/root imports and re-exports are rejected for moved runtime contracts, including forms such as:
  - `use hammer_adapter::{DataPlaneRuntime, NodeRuntimeData};`
  - `pub use hammer_adapter::{NodeEntry};`
- Preserved the intended allowlist behavior: grouped adapter imports that keep OS/component/platform traits such as `PlatformInterface` are not flagged.
- Added a focused regression test proving grouped adapter runtime imports are rejected.

### Tests Run And Results

- `cargo test -p hammer-runtime --test graph_runtime_owner_guard` - passed, 2 tests.
- `cargo test -p hammer-runtime --test graph_runtime_owner` - passed, 1 test.
- `cargo fmt --all -- --check` - passed.

### Files Changed

- `crates/hammer-runtime/tests/graph_runtime_owner_guard.rs`
- `.superpowers/sdd/2026-07-09-runtime-ownership-adapter-ios/task-38-report.md`

### Concerns

- Required commands still emit pre-existing workspace warnings unrelated to this fix.

## Fix Amendment (Guard Coverage Follow-Up)

### What I Fixed

- Expanded `crates/hammer-runtime/tests/graph_runtime_owner_guard.rs` root banned imports to match the moved runtime contracts that are now publicly re-exported from `crates/hammer-runtime/src/lib.rs`, including:
  - node/stats symbols: `NodeErrorCounters`, `NodeRuntimeStatsRow`, `default_prefetch_indices`
  - trace contracts: `PacketTrace`, `TraceControlHandle`, `TraceControlPlane`, `TraceEntry`, `TraceFormatter`, `TraceInputPolicy`, `TracePolicy`, `TraceRecord`, `TraceRecordSink`
- Expanded direct banned adapter owner paths for the same moved symbols, plus `hammer_adapter::DataPlaneTrace`, so old root/direct adapter imports and re-exports fail the guard.
- Added grouped-import regression coverage for one previously omitted node/stats symbol and one previously omitted trace symbol:
  - `use hammer_adapter::{..., NodeRuntimeStatsRow, ...};`
  - `pub use hammer_adapter::{..., TracePolicy};`
- Kept allowed adapter OS/component/platform traits unbanned; the regression still proves `PlatformInterface` remains allowed.

### TDD Evidence

- RED: `cargo test -p hammer-runtime --test graph_runtime_owner_guard` failed because grouped `NodeRuntimeStatsRow` and `TracePolicy` imports were not rejected.
- GREEN: after expanding the banned symbol lists, `cargo test -p hammer-runtime --test graph_runtime_owner_guard` passed.

### Tests Run And Results

- `cargo test -p hammer-runtime --test graph_runtime_owner_guard` - passed, 2 tests.
- `cargo fmt --all -- --check` - passed.

### Files Changed

- `crates/hammer-runtime/tests/graph_runtime_owner_guard.rs`
- `.superpowers/sdd/2026-07-09-runtime-ownership-adapter-ios/task-38-report.md`

### Concerns

- Verification still emits pre-existing workspace deprecation/unused warnings unrelated to this guard coverage fix.

## Fix Amendment (DataPlaneTrace Grouped Import)

- Added `DataPlaneTrace` to `ROOT_BANNED_RUNTIME_IMPORTS` in `crates/hammer-runtime/tests/graph_runtime_owner_guard.rs` so grouped imports like `use hammer_adapter::{DataPlaneTrace};` are rejected by the guard.
- Extended the grouped-import regression sample and assertion to explicitly require a `DataPlaneTrace` violation.
- This is a narrow guard coverage fix only; no runtime ownership surfaces changed.

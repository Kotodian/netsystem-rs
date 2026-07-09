# Task #37 Report: Move Buffer And Frame Primitives To hammer-core::data_plane

## What I implemented

- Moved the packet buffer and frame primitive implementation to `hammer_core::data_plane`.
- Added `crates/hammer-core/src/data_plane/buffer.rs` and `crates/hammer-core/src/data_plane/memory.rs` as the core-owned implementation modules.
- Re-exported the moved buffer/frame surface from `hammer_core::data_plane`, including buffer constants, opaque/header metadata, buffer indexes, frame indexes, packet cursors, buffer pools, frame owners, frame cursors/batches, `DataPlaneBuffers`, and buffer-chain primitives.
- Reduced `hammer-adapter/src/buffer.rs` to the remaining runtime execution owner: `DataPlaneRuntime`, runtime config, scheduling, handoff, trace release, prefetch, and node dispatch integration.
- Removed root adapter compatibility re-exports for the moved primitives; adapter now only re-exports `DataPlaneRuntime` and `DataPlaneRuntimeConfig` from its buffer module.
- Updated runtime and service callers/tests to import moved buffer/frame primitives from `hammer_core::data_plane`.
- Kept executable graph contracts in adapter for Task #38: `DataPlaneRuntime`, node traits/descriptors/process functions, handoff, instruction-set policy, and runtime trace policy remain adapter-owned.
- Added/strengthened core guardrails:
  - `crates/hammer-core/tests/data_plane_buffer_frame.rs` verifies the public core buffer/frame seam and behavior.
  - `crates/hammer-core/tests/data_plane_buffer_owner_guard.rs` rejects old adapter paths for moved buffer/frame primitives in runtime/service/app/IPC/macro surfaces.
- Added `spinning_top` as a `hammer-core` dependency because the moved buffer pool implementation uses it.

## What I tested and results

- `cargo test -p hammer-core --test data_plane_buffer_frame -- --nocapture`
  - PASS: 4 passed.
- `cargo test -p hammer-core --test data_plane_buffer_owner_guard -- --nocapture`
  - PASS: 1 passed.
- `cargo test -p hammer-core`
  - PASS: all hammer-core unit, integration, and doctests passed.
- `cargo test -p hammer-adapter`
  - PASS: all hammer-adapter unit/integration/doc tests passed.
- `cargo test -p hammer-runtime`
  - PASS: all hammer-runtime unit/integration/doc tests passed.
- `cargo test -p hammer-service`
  - PASS: all hammer-service unit/integration/doc tests passed.
- `cargo fmt --all -- --check`
  - PASS after applying `cargo fmt --all` to fix one wrapping diff in the new core test.
- Manual stale-import audit:
  - `rg ... --glob '!data_plane_buffer_owner_guard.rs'`
  - PASS: no moved buffer/frame primitive imports remain from `hammer_adapter` paths in scanned runtime/service/app/IPC/macro/test surfaces.

## TDD evidence

- Inferred RED from the task brief:
  - Before this task, a focused core test importing moved buffer/frame primitives from `hammer_core::data_plane` would fail because core did not own or export those primitives yet.
  - The brief also recorded an existing adapter failure in `buffer::tests::generation_advances_across_alloc_free_cycles` with `left: 1, right: 2`; the migrated adapter/core buffer suites now pass, including generation reuse coverage.
- Current diagnostic state when I picked up the existing uncommitted work:
  - The prior worker had already created the core tests and most migration edits; the focused core tests were already passing.
  - I tightened the guard/export coverage and fixed the formatting issue found by `cargo fmt --all -- --check`.
- GREEN evidence:
  - Focused core seam and owner guard pass.
  - `hammer-core`, `hammer-adapter`, `hammer-runtime`, and `hammer-service` all pass.
  - Formatting check passes.

## Files changed

- Core ownership:
  - `crates/hammer-core/Cargo.toml`
  - `crates/hammer-core/src/data_plane.rs`
  - `crates/hammer-core/src/data_plane/buffer.rs`
  - `crates/hammer-core/src/data_plane/memory.rs`
  - `crates/hammer-core/tests/data_plane_buffer_frame.rs`
  - `crates/hammer-core/tests/data_plane_buffer_owner_guard.rs`
  - `Cargo.lock`
- Adapter runtime preservation and tests:
  - `crates/hammer-adapter/src/buffer.rs`
  - `crates/hammer-adapter/src/handoff.rs`
  - `crates/hammer-adapter/src/lib.rs`
  - `crates/hammer-adapter/src/node.rs`
  - `crates/hammer-adapter/src/node/next.rs`
  - `crates/hammer-adapter/tests/buffer.rs`
  - `crates/hammer-adapter/tests/buffer_frame_guard.rs`
  - `crates/hammer-adapter/tests/buffer_inline_layout.rs`
  - `crates/hammer-adapter/tests/buffer_layout.rs`
  - `crates/hammer-adapter/tests/buffer_per_numa.rs`
  - `crates/hammer-adapter/tests/node_runtime.rs`
  - `crates/hammer-adapter/tests/process_frame.rs`
- Runtime/service import rewrites:
  - `crates/hammer-runtime/src/data_plane.rs`
  - `crates/hammer-runtime/src/engine.rs`
  - `crates/hammer-runtime/src/main_loop.rs`
  - `crates/hammer-runtime/src/memory.rs`
  - `crates/hammer-runtime/src/spawn.rs`
  - `crates/hammer-runtime/tests/engine_numa_runtime.rs`
  - `crates/hammer-runtime/tests/memory_static_init.rs`
  - `crates/hammer-runtime/tests/worker_spawn.rs`
  - `crates/hammer-service/src/data_plane.rs`
  - `crates/hammer-service/src/interface.rs`
  - `crates/hammer-service/src/net/ip/icmp.rs`
  - `crates/hammer-service/src/net/ip/input.rs`
  - `crates/hammer-service/src/net/ip/local.rs`
  - `crates/hammer-service/src/net/ip/mod.rs`
  - `crates/hammer-service/src/net/ip/reassembly.rs`
  - `crates/hammer-service/src/net/lookup/mod.rs`
  - `crates/hammer-service/src/net/opaque.rs`
  - `crates/hammer-service/src/session/app.rs`
  - `crates/hammer-service/src/session/node.rs`
  - `crates/hammer-service/src/session/protocol.rs`
  - `crates/hammer-service/src/session/runtime.rs`
  - `crates/hammer-service/src/transport/tcp/established.rs`
  - `crates/hammer-service/src/transport/tcp/input.rs`
  - `crates/hammer-service/src/transport/tcp/listen.rs`
  - `crates/hammer-service/src/transport/tcp/mod.rs`
  - `crates/hammer-service/src/transport/tcp/output.rs`
  - `crates/hammer-service/src/transport/tcp/rcv_process.rs`
  - `crates/hammer-service/src/transport/tcp/reset.rs`
  - `crates/hammer-service/src/transport/tcp/segment.rs`
  - `crates/hammer-service/src/transport/tcp/syn_sent.rs`
  - `crates/hammer-service/src/transport/udp/input.rs`
  - `crates/hammer-service/src/tun/mod.rs`
  - `crates/hammer-service/tests/frame_owner_cleanup_hot_path.rs`
  - `crates/hammer-service/tests/icmp_input_nodes.rs`
  - `crates/hammer-service/tests/interface_control.rs`
  - `crates/hammer-service/tests/net_lookup_node.rs`
  - `crates/hammer-service/tests/net_lookup_perf.rs`
  - `crates/hammer-service/tests/session_queue_dispatch.rs`
  - `crates/hammer-service/tests/tcp_input_counters.rs`
  - `crates/hammer-service/tests/tcp_reset.rs`
- Report:
  - `.superpowers/sdd/2026-07-09-runtime-ownership-adapter-ios/task-37-report.md`

## Self-review findings

- Scope check: executable graph runtime contracts were not moved to core. They remain in adapter for Task #38.
- Adapter compatibility check: adapter root no longer re-exports moved buffer/frame primitives. Remaining adapter use of core buffer/frame types is private implementation use for runtime execution.
- Ownership guard check: direct and braced old adapter paths for moved primitives are covered; `DataPlaneRuntime` and other executable contracts are intentionally not banned yet.
- Behavior check: buffer allocation/free, generation reuse, cursor metadata, chain traversal, trace-handle release, and `Frame<Next>` / `Frame<Pending>` RAII behavior are covered by core/adapter tests.

## Concerns

- The verification runs still emit existing warning noise, mostly deprecated `FlatHashTable` usage and unrelated unused/private-interface warnings. No warnings were treated as failures in this task.
- The manual stale-import audit exits with code 1 when there are no matches; this is the expected `rg` no-match exit, not a failure.

# Project Instructions

## Architecture Reference

- Before every code modification, read the matching implementation in `third_party/vpp` and compare against it first.
- TCP, session, FIFO, app-worker, timer, recovery, and packet-output design changes must start from the matching implementation in `third_party/vpp`.
- Preserve VPP ownership and lifecycle rules unless Hammer's runtime model requires a documented difference.
- Hammer's main/control thread runs a single-thread Tokio runtime. Tokio owns asynchronous control-plane I/O only; do not add locks around main-thread-only state.
- App workers run on Hammer's custom async runtime and receive work from their owning data-plane worker. Preserve worker ownership and use bounded lock-free delivery wherever practical instead of routing app work through main-thread Tokio.
- TCP state, timers, retransmission, congestion control, RACK/TLP, ACK processing, and packet generation remain data-worker-local. Do not move them into Tokio tasks.
- Nodes access packet buffers through frame APIs such as `BufferBatchMut`. Do not introduce `DataPlaneRuntime` or `DataPlaneBuffers` as a node buffer ownership path, including TX allocation/free.

## Code Scope

- Keep visibility narrow. Use private items within a module and `pub(crate)` within a crate; expose `pub` only for real cross-crate API boundaries.
- Avoid one-use helper functions that only split sequential logic and force readers to jump between scopes. Keep single-use parsing, encoding, validation, and control-flow details in the smallest useful caller scope.
- Retain functions when they represent a reusable operation, ownership boundary, protocol abstraction, or independently testable behavior.
- Do not add unrelated refactors or abstractions.

## Performance

- Treat worker, TCP, FIFO, message-queue, and packet-processing paths as performance-sensitive.
- Preserve cache-line alignment for shared and hot data structures; use 64-byte allocation boundaries where the surrounding infrastructure does.
- Separate hot per-packet/per-session state from cold control-plane, attach, diagnostics, and publication metadata.
- Avoid unnecessary copies, heap allocation, indirection, synchronization, and work in hot paths. Prefer existing batching and frame operations.
- Do not introduce `Mutex` or `RwLock` to solve ownership or initialization convenience. Use single-owner state, worker-local state, bounded message passing, atomics, or existing lock-free queues unless genuinely shared mutable state requires a lock and the contention model is justified.
- Consider instruction-set or vectorized optimization only where the hot data path and data layout benefit. Do not add SIMD or architecture-specific complexity to one-shot control paths without evidence.

## Validation Workflow

- When fixing or expanding GitHub Actions, do not run local tests, builds, benchmarks, clippy, or TUN labs. Verification runs in GitHub Actions.
- After each coherent Rust or other business-code edit batch, run the `simplify` skill scoped to that batch.
- Do not run `simplify` for Python CI helpers, shell scripts, or workflow-only edits.
- `simplify` must not run prohibited tests, builds, benchmarks, or modify unrelated files.

## Context Budget

- Keep the main conversation's tool-result context bounded. In one assistant turn, issue at most three content-producing `Read`, `Grep`, `Glob`, or `Bash` calls in parallel. Wait for and summarize that batch before starting another content-producing batch.
- Locate relevant symbols and line ranges before reading implementation. Every `Read` call must use an explicit `offset` and a `limit` of at most 250 lines. Do not read an entire large source, transcript, generated file, lockfile, vendored tree, or build artifact.
- Bound shell output at its source. Scope `rg` to relevant paths and patterns, select narrow ranges with `sed`, and cap diagnostic or history output with an appropriate `head` or `tail`. Keep each command's expected output below roughly 12,000 characters.
- Do not repeat a broad read after a truncated result. Narrow the query or continue from the next explicit range. Summarize established facts instead of carrying large quotations into later reasoning.
- Run at most three subagents concurrently. Give each a non-overlapping scope and require a final report under 1,200 words containing findings and file/line references, without pasted source blocks or raw command output.
- Never read full Claude session JSONL files or subagent transcript/output files into the conversation. Inspect only metadata, targeted matches, or bounded excerpts.

## Working Tree

- Preserve unrelated user changes and untracked files. Do not revert, overwrite, stage, or commit them unless explicitly requested.

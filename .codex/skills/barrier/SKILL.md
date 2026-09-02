---
name: barrier
description: Design, review, or implement Hammer WorkerBarrier synchronization for main-thread publication and Data Worker acknowledgement; use when changing barrier entry, worker checks, nested scopes, graph refork, or shutdown semantics.
---

# Worker Barrier

Use this skill for Hammer's VPP-style `WorkerBarrier`, not for ordinary locks,
single-worker state, or independent counters. Start by identifying the state
being protected and the owner that can publish it.

## Contract

- The main/control OS thread is the only caller that arms and releases the
  barrier. A Data Worker only acknowledges it from its runtime loop.
- Workers must stop before the main thread mutates graph, topology, registry,
  or other worker-visible state. Mutate the barrier-owned value during that
  acknowledgement interval; do not add a `Mutex`, `RwLock`, atomic pointer,
  or second completion protocol around it.
- Enter only through the exported `worker_thread_barrier_sync!` macro. RAII
  must release on normal
  return, `?`, early return, and panic. Preserve nested recursion semantics.
- The process creates one Data Worker generation. A missed acknowledgement or
  unmatched release is a programmer deadlock and must retain the existing
  abort/assert behavior, not become a recoverable packet or control error.
- Final shutdown is special: the runtime may hold the final barrier while
  workers remain stopped and then terminate. Do not invent a restartable
  shutdown mode or join protocol.

## Macro Usage

Use the exported macro with a mutable `DataPlaneMain` expression and an inline
block. The block's value is returned by the macro, so `Result` propagates
normally:

```rust
use hammer_runtime::worker_thread_barrier_sync;

worker_thread_barrier_sync!(&mut main, {
    publish_worker_visible_state()?;
    Ok(())
})?;
```

The macro creates a private RAII guard, runs the block without an async
`.await`, then drops the guard. `?`, `return`, and panic still release it.
Nested calls are allowed and retain the existing recursion count. Do not call
`WorkerBarrier::sync` or `barrier::__sync_guard` directly; they are not the
supported application entry point. `__sync_guard` is only a `#[doc(hidden)]`
expansion helper.
The `$main` expression must identify the installed main-thread
`DataPlaneMain`; passing a worker context is a programmer invariant.

## Reasoning Checklist

Before editing, read `CONTEXT.md`, the relevant ADRs, and
`crates/hammer-runtime/src/barrier.rs`. Search the vendored VPP counterparts
under `third_party/vpp/src/vlib/threads.{h,c}` and inspect callers around each
operation.

Record these facts for the changed state:

1. **Owner:** main/control thread, one Data Worker, barrier-owned value, or
   genuinely shared infrastructure.
2. **Readers/writers:** every worker, control, RPC, cleanup, and shutdown path.
3. **Publication:** the release/acquire edge that makes the state visible.
4. **Lifetime:** why no worker can still use the old value before it is changed
   or reclaimed.
5. **Recovery:** worker exit, timeout/deadlock, cancellation, nested entry,
   no-op publication, and partial mutation.

Use `&T`/`&mut T` or owner-worker handoff when that is sufficient. Use an
atomic only for an independent scalar invariant. Use a lock only for a short
bounded cross-thread critical section; never hold one across I/O, `.await`,
unknown callbacks, allocation-heavy work, or barrier entry.

## Implementation Rules

- Keep barrier state process-wide and separate from `GlobalMain` ownership of
  worker lifecycle, graph publication, and refork completion.
- Preserve release/acquire ordering: main publishes the request with release;
  workers observe with acquire and acknowledge with release; main waits with
  acquire; release publishes control-plane writes with release before workers
  resume with acquire.
- Do not use relaxed ordering to publish object fields. Relaxed is for scalar
  accounting only.
- Synchronize at the owner that knows a worker-visible publication is needed.
  No-op operations should not synchronize. Callers that require an already-held
  scope should use the existing hidden assertion rather than adding a public
  barrier parameter or accessor.
- Do not wrap the macro in an async task or hold the scope across `.await`, I/O,
  or a blocking operation. Keep one outer scope around the complete control
  transaction; nested owners should rely on recursion instead of reacquiring
  through a second mechanism.
- Validate all participants before the first mutation. If a later operation can
  fail, complete infallibly or roll back every sibling publication and ownership
  change.
- Keep tests behavioral: acknowledgement and release, nested scopes, early
  return/error/unwind, main-vs-worker invariant, no-op publication, graph
  refork coalescing, and shutdown. Do not test by reading source text.

## Review Output

Report: (1) state owner and VPP counterpart, (2) why the barrier is the
narrowest primitive, (3) release/acquire and lifetime proof, (4) failure and
shutdown behavior, and (5) tests and residual risk. Reject designs that add a
second lock/completion protocol, let a foreign worker mutate worker-local
state, publish fields with relaxed ordering, or treat a deadlock as a normal
recoverable error.

Primary references: `docs/adr/0002-vpp-style-worker-thread-barrier.md`,
`docs/adr/0003-vpp-style-worker-graph-refork.md`, and
`.codex/skills/vpp-sync-primitives/references/vpp-sync-map.md`.

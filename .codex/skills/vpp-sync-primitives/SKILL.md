---
name: vpp-sync-primitives
description: Audit, design, and implement Hammer synchronization against the vendored VPP memory and ownership model. Use when changing WorkerBarrier, SpinLock, RwLock, atomics, fences, RCU or snapshot publication, worker-owned state, handoff queues, object lifetime, or any cross-worker state; always use it before choosing a synchronization primitive for VPP-style dataplane work.
---

# VPP Synchronization Primitives

Use this skill for synchronization decisions in Hammer. Treat VPP as the
semantic reference, then express the same ownership and ordering in Rust. Do
not begin by choosing a lock or an atomic. Begin by stating who owns the state,
which threads can read or mutate it, and what event transfers ownership.

## Gate

Before editing, read `AGENTS.md`, `CONTEXT.md`, the relevant ADRs, and the
closest files under `third_party/vpp/`. Search the vendored tree first; use an
external source only when the required VPP code is absent. Load
`references/vpp-sync-map.md` for the source map and exact search commands.

Write down these facts for the changed state:

- **Owner:** one Data Worker, the main/control thread, a barrier-owned value, or
  a genuinely shared infrastructure object.
- **Readers and writers:** every execution lane, including cleanup and RPC
  paths. A pointer or index is not ownership.
- **Publication event:** the operation that makes a new object, mapping, or
  scalar visible, and the matching observer operation.
- **Lifetime event:** the operation that proves the old object cannot be used
  before it is freed. A visibility mechanism does not provide lifetime safety.
- **Recovery:** what happens on a full queue, stale generation, failed
  allocation, cancellation, or worker exit.

If these facts are unclear, stop and investigate instead of adding a generic
shared registry or a new wrapper type.

## Choose by Ownership

Apply this order of preference:

1. Keep packet-path state worker-owned and pass `&T`/`&mut T`. Use a
   `ThreadOwned<T>` only when a shared main structure needs indexed access to
   values that remain owned by individual workers.
2. Use `WorkerBarrier` when the main/control thread must stop every Data Worker
   before mutating graph, topology, registry, or other worker-visible state.
   Mutate the barrier-owned value during the acknowledgement phase; do not add
   a mutex, `RwLock`, atomic pointer, or second completion protocol around it.
3. Use an atomic for an independent scalar invariant such as a flag, counter,
   sequence, queue index, or ownership count. Choose memory ordering from the
   producer/observer protocol, not from habit.
4. Use `hammer_runtime::sync::SpinLock` only for a short bounded critical
   section between OS threads when sleeping would cost more than the work.
   Never await, perform I/O/syscalls, allocate unpredictably, invoke an
   unknown callback, or enter a worker barrier while holding it.
5. Use `hammer_runtime::sync::RwLock` only for short read-dominant access with
   rare bounded writes. It is reader-preferring and may starve a writer; it is
   not a lifecycle-transition lock and must not wrap a worker-barrier publish.
6. Use a bounded channel, lock-free ring, handoff queue, or owner-worker RPC
   when ownership moves between threads. After transfer, the producer must
   stop accessing the item, and drop/backpressure behavior must be explicit.

Do not use `thread_local!`, a packet-hot-path mutex, a lock merely to satisfy
`Send`/`Sync`, or `dyn` dispatch to hide synchronization ownership.

## Memory Ordering

Separate three contracts:

- **Scalar accounting:** VPP uses relaxed atomics for independent counters,
  queue indices, and ownership counts in hot paths. In Hammer,
  `Ordering::Relaxed` is acceptable only when the value expresses an
  independent scalar and no object fields are published by that operation.
- **Publication:** data written before a pointer, index, generation, or ready
  flag becomes visible needs a release operation, and the observer needs the
  matching acquire operation. An atomic pointer is not an ownership model.
- **Lifetime/reclamation:** prove that no reader can still dereference an old
  object before freeing it. Use worker ownership, a barrier, a handoff, or an
  explicit reclamation protocol. `ArcSwap::rcu` or another snapshot operation
  is justified only when the object snapshot and its reader lifetime are the
  actual contract; it is not a replacement for worker ownership or an
  independent scalar invariant.

Use `compiler_barrier`, `release_fence`, `store_barrier`, and `memory_barrier`
only inside a documented low-level publication, device, FFI, non-temporal
store, or lock-free protocol. A compiler fence does not synchronize CPUs, and a
hardware fence does not make a Rust data race valid. State the accesses it
orders and name the matching atomic or device operation.

## VPP Correspondence

Use the following semantic correspondences, not a literal C-to-Rust port:

- `clib_spinlock_t` uses an acquire compare-exchange, relaxed polling, and a
  release unlock. Hammer's `SpinLock` has the same bounded, non-poisoning
  contract and cache-line isolation.
- `clib_rwlock_t` represents a writer as `-1` and readers as a positive count.
  Readers may continue to arrive while a writer waits, so Hammer's `RwLock`
  is intentionally reader-preferring and unsuitable for lifecycle state.
- `vlib_worker_thread_barrier_sync/release` stops all workers at their runtime
  check, lets the control thread mutate barrier-owned state, then waits for
  every worker to leave. Treat a missed barrier deadline as an impossible
  worker deadlock, not a recoverable packet error.
- VPP keeps protocol and graph state in worker-local pools whenever possible.
  A foreign worker does not mutate or free a local pool entry directly; it
  transfers an index or request to the owning worker through an event, RPC, or
  handoff queue.
- VPP handoff queues use relaxed queue/index accounting but pair producer
  release publication with consumer acquire observation of the slot payload.
  Copy this pair only when the same single-producer/consumer ownership model
  has been proved.
- VPP session control operations that mutate worker-visible session state run
  through the worker/barrier or an owner-thread event. Do not put a synchronous
  lock around a worker barrier to make an otherwise-invalid cross-worker pool
  access look safe.

## Workflow

1. Locate the exact VPP counterpart with `rg` and inspect its callers, not just
   the declaration. Record the state owner, the thread that performs mutation,
   the reader, and the ordering used at each edge.
2. Classify the state as worker-owned, barrier-owned, scalar-atomic,
   lock-protected shared state, or transferred ownership. Reject designs that
   mix categories without a stated boundary.
3. Draw the publication and reclamation sequence. For a registry or lookup,
   distinguish the published snapshot, worker-owned objects, scalar counters,
   lookup storage, and reclamation; they may require different mechanisms.
4. Implement the smallest owner-local API. Prefer existing `WorkerBarrier`,
   `SpinLock`, `RwLock`, `Pool`, `ThreadOwned`, handoff, and SessionQueue APIs.
   Any new public type or API in non-trivial VPP/TCP work
   needs the approval required by `AGENTS.md`.
5. Preserve failure atomicity. Validate all participants before publication;
   after the first mutation, complete infallibly or roll back every sibling
   mapping and ownership count. Never free a worker-local pool entry from a
   foreign worker.
6. Test the behavior and ordering boundary. Use real concurrent or lifecycle
   tests for barrier acknowledgement, lock exclusion, publication visibility,
   queue transfer, cleanup ownership, and retry behavior. Do not read source
   files or assert implementation strings as a substitute for behavior.
7. Run the focused crate tests, then `cargo fmt --all -- --check`,
   `cargo clippy --workspace --all-targets`, `cargo test --workspace`,
   `git diff --check`, and any repository allocation-contract checks required
   by the touched crate.

## Review Output

Report synchronization changes in this order:

1. State owner and VPP counterpart, with exact file/function evidence.
2. Primitive selected and why worker ownership, a barrier, an atomic, a lock,
   or a transfer queue is the narrowest correct choice.
3. Publication ordering and reclamation/lifetime proof, including every
   release/acquire or relaxed operation and what it does *not* guarantee.
4. Cross-worker transfer/reclamation and failure-atomicity behavior.
5. Tests and commands run, followed by any residual risk or intentional VPP
   divergence.

Reject a change when it uses RCU as a generic replacement for VPP's worker
ownership, publishes an object with relaxed ordering, protects barrier-owned
state with a second lock, lets a foreign worker mutate a local pool, or adds a
spin lock to the normal packet path without a bounded contention argument.

See [references/vpp-sync-map.md](references/vpp-sync-map.md) for the vendored
source map and focused evidence snippets.

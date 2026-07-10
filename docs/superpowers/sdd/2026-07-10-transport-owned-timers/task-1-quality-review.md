# Task 1 Code-Quality Review

## Verdict

FAIL. No Critical findings; three Important findings remain.

Scope reviewed: `f653c2ea..57df04a8` (`ed5b4ba9`, `59ec18a1`,
`57df04a8`) against Task 1 of
`docs/superpowers/plans/2026-07-10-transport-owned-timers.md`, the design
specification, ADR 0008, `CONTEXT.md`, and `AGENTS.md`.

The earlier spec review is accepted as PASS. This review focuses on
correctness, soundness, generation safety, lifecycle cleanup, static generic
and TLS type safety, hot-path allocation/cloning, public API scope,
maintainability, and test strength.

## Findings

### Important 1: Task 1 replaces the old TLS queue ownership with a new type-erased TLS cache

Files: `crates/hammer-service/src/transport/tcp/mod.rs:68`,
`crates/hammer-service/src/transport/tcp/mod.rs:174`,
`crates/hammer-service/src/session/node.rs:276`

`TCP_SESSION_QUEUE_RUNTIME_DATA` stores an untyped `NodeRuntimeData` pointer
behind a `(runtime_key, TypeId<C>, SessionBackend)` key. Callers then construct
`SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>`
from that untyped value and `session_queue_cell` casts it to `RefCell<Q>`.

The runtime/backend cache test proves that the current key separates the cases
it exercises, but it does not make the cast statically safe. More importantly,
this is the exact TLS queue-runtime-data ownership that Task 1 step 6 requires
removing, and it conflicts with the design/ADR prohibition on TLS and raw
pointer type erasure. The safe `SessionQueueHandle::new(NodeRuntimeData)` API
also cannot enforce the safety comment's claim that the same `Q` is recovered.

Concrete fix: remove `TCP_SESSION_QUEUE_RUNTIME_DATA` and make queue ownership
part of the worker/graph registration context. Keep the typed
`SessionQueueHandle<SessionDriverRuntime<..., Seg, ...>>` from creation through
node registration instead of returning and re-keying a raw pointer. Any
unavoidable runtime-data reconstruction must be a narrowly scoped unsafe
boundary with an explicit caller contract, not a safe generic constructor.

This cache shape is introduced by Task 1. It should not be accepted merely
because the current runtime/type/backend lookup happens to select the expected
pointer in the focused test.

### Important 2: TCP TX still deep-clones allocation-owning connection state on every batch

Files: `crates/hammer-service/src/transport/tcp/worker.rs:301`,
`crates/hammer-service/src/transport/tcp/recovery.rs:1351`,
`crates/hammer-service/src/transport/tcp/sack.rs:63`,
`crates/hammer-service/tests/vpp_session_tx_guardrails.rs:63`

`TcpWorker::tx_action` executes `connection.clone()` for every normal TX batch.
That clone is not a cheap snapshot: `TcpRecoveryState::clone` constructs new
Pool/RbTree storage, clones the scoreboard, and walks all outstanding sent
samples twice; `TcpSackState::clone` allocates and rebuilds another RbTree.
Consequently normal packetized TX performs heap-backed infrastructure
allocations and work proportional to outstanding recovery state.

The updated guardrail targets the correct new file, but it only rejects the
spellings `std::vec::Vec::with_capacity` and `std::vec!`. It passes while the
deep clone allocates through `hammer-infra` Pool/RbTree, so it does not protect
the stated hot-path property.

Concrete fix: replace whole-connection rollback with a bounded send-side
transaction/checkpoint that covers only fields mutated by `tx_segment` and
`commit_payload_tx`, or prevalidate/stage the batch so the real connection can
be updated without cloning recovery and SACK containers. Because the plan
currently explicitly prescribes the candidate clone, obtain approval for the
replacement shape. Strengthen the guardrail with an allocation-counting test
or a structural assertion that rejects `TcpConnection::clone` in `tx_action`.

The deep clone predates Task 1 (`push_header` already used it at `f653c2ea`);
Task 1 moves it into `TcpWorker::tx_action` and leaves the hot-path cost intact.

### Important 3: app-to-session events are slot-only and can target a reused generation

Files: `crates/hammer-infra/src/msg_queue.rs:8`,
`crates/hammer-service/src/session/app.rs:302`,
`crates/hammer-service/src/session/node/tests.rs:821`

`SessionEvt` carries only `session_index` (the pool slot). During drain,
`SessionAppRuntime` resolves that slot through `sessions_by_index` to whichever
`SessionId` generation is current at consumption time. A delayed or duplicate
Close/TxDeq produced by an old app session after its slot is removed and reused
can therefore be delivered to the replacement session. Pool generation checks
cannot help because the event never carried the old generation.

The new `transport_deleted_then_queued_app_close_releases_the_session_slot`
test consumes the old Close before inserting the replacement. It proves the
happy cleanup order, but not the generation boundary. It would still pass an
implementation that misdelivers a second old Close after reuse.

Concrete fix: make runtime-directed app events generation-aware (carry the
full generation-safe SessionId/handle, or validate an equivalent generation
token before dispatch). Add a test that queues two old Close events, consumes
the first to release the slot, attaches a replacement in that slot, then proves
the second old event neither closes nor schedules the replacement.

This event-format weakness predates Task 1 and is not introduced by these
commits. It is nevertheless a blocking residual generation-safety issue for
Task 1's new lifecycle cleanup and immediate slot-reuse behavior. If changing
the cross-process event ABI is intentionally out of Task 1 scope, record and
link a blocking follow-up rather than claiming generation-safe lifecycle
completion.

## Suspected Areas Re-checked

- Queue cache across runtime/type/backend: the focused test passes, but the
  cache remains an erased TLS ownership surface and is Finding 1.
- `TransportDeleted` followed by queued app close: the direct path works and
  releases the slot; delayed/duplicate events across slot reuse remain unsafe
  as described in Finding 3.
- Deep `TcpConnection` clones: confirmed in the normal TX batch path; the clone
  rebuilds allocation-owning recovery and SACK state (Finding 2).
- `vpp_session_tx_guardrails` target: it now reads `tcp/worker.rs`, but its
  assertions are too narrow to detect the confirmed allocation path.

## Verification

- `cargo test -p hammer-service --lib session:: -- --test-threads=1`: PASS,
  16 tests.
- `cargo test -p hammer-service --lib tcp_session_queue_cache_isolated_by_runtime_and_backend -- --test-threads=1`:
  PASS, 1 test.
- `cargo test -p hammer-service --test vpp_session_tx_guardrails`: PASS,
  5 tests, while still missing the deep-clone allocation.
- `cargo fmt --all -- --check`: PASS.
- `git diff --check f653c2ea..57df04a8`: PASS.

The focused builds emit numerous pre-existing warnings; no full workspace test
suite was run for this quality review.

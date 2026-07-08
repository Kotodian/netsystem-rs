# VPP-aligned session scheduling and RX enqueue locality

## Problem Statement

Hammer's current session scheduling vocabulary and implementation shape are too far from VPP's session model. `SessionReadyQueue` combines a FIFO and hash-set into a generic readiness abstraction, then reuses that abstraction for different meanings such as TX work and pending close requests. This makes the architecture look like a scheduler-owned dedup queue instead of VPP's session-owned pending flag plus worker-local handle vector.

The result is a shallow seam between Session Runtime and transport: close handling leaks through a synthetic `close_requested` boolean on ready-session dispatch, app/runtime events are not cleanly classified into work vs control lanes, and transport-facing code can accidentally inherit session scheduling concepts. The user wants this aligned with VPP now, not deferred, and wants Rust types to encode the ownership boundary without introducing speculative middle types.

## Solution

Rework Session Runtime around VPP's session semantics:

- A session-layer pending bit owns duplicate suppression for scheduled session work.
- A worker-local Session Work Batch stores session ids in append-and-drain vector form, mirroring VPP's handle-vector flushing.
- Session Control Events are separate from TX/RX session work. App/requested close becomes a disconnect control event, not a ready-session flag.
- Transport state remains isolated from session scheduling state. TCP and other transports may ask Session Runtime to schedule the current session through the session context, but they must not own, inspect, or clear the pending bit.
- `SessionReadyQueue` is removed rather than renamed or wrapped.

The result should feel like VPP semantically while remaining idiomatic Rust: small private session-layer structs, narrow public accessors, and domain names that describe actual ownership.

## User Stories

1. As a Hammer maintainer, I want session scheduling to follow VPP's pending-flag plus worker-vector model, so that the codebase has a clear semantic reference.
2. As a Hammer maintainer, I want `SessionReadyQueue` removed, so that a generic dedup queue cannot keep hiding unrelated session concepts.
3. As a Session Runtime author, I want duplicate suppression owned by the session layer, so that scheduling state is local to the session entry rather than a side hash table.
4. As a Session Runtime author, I want the worker to hold only an append-and-drain Session Work Batch, so that worker-local batching aligns with VPP vector flushing.
5. As a TCP maintainer, I want TCP connection state isolated from session scheduling state, so that transport code cannot accidentally own runtime readiness.
6. As a TCP maintainer, I want `context.mark_ready()` to remain the only transport-facing scheduling request, so that transport code does not learn the batch or pending-bit representation.
7. As a Session Runtime maintainer, I want app/runtime events classified into separate lanes, so that TX work and close control are not dispatched through the same path.
8. As an app/session boundary maintainer, I want `TxDeq` events to schedule session work, so that app TX data is packetized by Session Runtime.
9. As an app/session boundary maintainer, I want `Close` events to become disconnect control events, so that close handling follows VPP's control-event model.
10. As a transport maintainer, I want disconnect handling invoked from a control-event path, so that ready-session dispatch does not carry a fake close boolean.
11. As a Hammer maintainer, I want `pending_closes` removed from the old ready queue shape, so that close requests cannot reuse the work-batch data structure.
12. As a Session Runtime maintainer, I want a private session entry wrapper around transport state, so that Rust's type boundaries enforce session-owned scheduling facts.
13. As a transport caller, I want session accessors to continue returning transport state references, so that existing transport code is not exposed to runtime-only fields.
14. As a runtime maintainer, I want control events dispatched before scheduled work in a turn, so that a disconnect wins over stale TX work for the same session.
15. As a runtime maintainer, I want session removal to clear the pending bit, so that leftover batch entries become harmless skips.
16. As a test author, I want to test scheduling through Session Runtime dispatch seams, so that tests validate behavior instead of the old queue implementation.
17. As a performance-minded maintainer, I want to avoid hash-set membership tracking in the worker batch, so that hot-path memory footprint stays controlled.
18. As a reviewer, I want forbidden abstractions documented, so that `DedupFifo`, `ReadySession`, `Session<Ready>`, and marker-state wrappers are not reintroduced under new names.
19. As a future app/session maintainer, I want RX enqueue locality documented with VPP vocabulary, so that app notification and FIFO enqueue ownership remain in Session Runtime.
20. As a future issue implementer, I want the PRD to distinguish work-batch scheduling from control events, so that the implementation can be split without losing the architecture.

## Implementation Decisions

- Session scheduling coalescing belongs to the session layer, not transport.
- Replace the old ready queue ownership model with a private session entry that wraps transport state and carries a scheduling pending bit.
- Keep transport-facing accessors returning only transport state references; the session entry wrapper remains a Session Runtime implementation detail.
- The worker-local Session Work Batch uses vector append-and-drain semantics. It does not own a hash table or any queue-level membership state.
- Duplicate suppression is performed by the session pending bit. A session id is appended to the work batch only when its pending bit transitions from false to true.
- Remove `SessionReadyQueue` as a type and public export.
- Do not introduce `DedupFifo`, `ReadySession`, `Session<Ready>`, generic marker traits, or PhantomData-based state machines for this problem.
- Keep `context.mark_ready()` as the transport-facing scheduling API; change only the backing ownership model.
- `pending_closes` must not be preserved. Close/disconnect is a control-event lane, not a ready-session side channel.
- Remove the synthetic `close_requested` parameter from ready-session handling.
- Add a narrow current control-event shape for disconnect only. Do not pre-create placeholder shutdown/reset variants.
- Drain app/runtime events by classifying `TxDeq` as session work and `Close` as a disconnect control event.
- Dispatch order is fixed: classify app/runtime events, expire timers, dispatch control events, dispatch the Session Work Batch, then flush output frames.
- If a session has both a disconnect control event and scheduled work in the same turn, disconnect is handled first; later work for a removed session is skipped.
- Timer expiry dispatch continues to pass the exact timer token to transport. It must not scan timer kinds or use ready scheduling to discover expired work.
- RX enqueue locality remains session-owned: transport supplies buffer identity, offset, and in-order/OOO facts; Session Runtime owns FIFO enqueue, app notification, and RX capacity facts.
- `RxDelivery` should represent legal receive outcomes directly and use the existing `CoreResult` boundary for errors.
- Avoid new error enums or field-bag results for RX enqueue. Accepted-byte and OOO-span invariants should be represented by domain values.
- All new API surface should state the final result and why existing surfaces are insufficient before implementation starts.

## Testing Decisions

- Use the highest practical seam: Session Runtime dispatch with app/session events and transport protocol fakes.
- Test behavior, not the removed data structure. There should be no tests for `SessionReadyQueue`.
- Cover duplicate suppression by scheduling the same session multiple times and asserting the Session Work Batch dispatches it once until the pending bit is cleared.
- Cover rescheduling by dispatching once, then scheduling the same session again and asserting it can enter a later batch.
- Cover app event classification by asserting `TxDeq` schedules work and `Close` creates a disconnect control event.
- Cover ordering by asserting a same-turn `Close + TxDeq` processes disconnect before ready work and does not run TX work for a removed session.
- Cover transport isolation by asserting protocol fakes see only the session context methods, not pending-bit or batch internals.
- Cover timer behavior by asserting expired timers are delivered by exact token and do not imply a ready/session-work scan.
- Existing session queue dispatch tests are the closest prior art; adapt them to the new work/control lanes instead of adding low-level queue tests.

## Out of Scope

- Rewriting TCP recovery, congestion control, or output intent.
- Adding generic scheduling containers or reusable dedup queues.
- Adding placeholder control events with no current implementation path.
- Reworking the entire app/session message queue format beyond the event classification needed here.
- Introducing transport-owned RX FIFO writes, app wakeups, or payload copies.
- Changing VPP semantics into a one-to-one API clone; Hammer should match ownership and dispatch semantics, not C names or data layout.

## Further Notes

This PRD follows the confirmed ADR direction for Session RX enqueue locality and session scheduling. VPP references used during discussion include `SESSION_F_RX_EVT`, `session_to_enqueue[proto]`, `session_main_flush_enqueue_events`, and `SESSION_CTRL_EVT_DISCONNECT`-style control dispatch. The important architectural result is not a renamed queue; it is a deeper session/runtime module boundary.

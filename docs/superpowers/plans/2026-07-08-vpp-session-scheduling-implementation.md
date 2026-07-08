# VPP-aligned Session Scheduling Implementation Plan

Parent PRD: https://github.com/Kotodian/hammer-ios-rs/issues/20

## Confirmed Test Seam

Use the highest practical seam: Session Runtime dispatch with app/session events and transport protocol fakes. Tests must verify behavior through runtime/session APIs, not the internals of a queue implementation.

## Global Constraints

- Align with VPP semantics: session-owned pending fact plus worker-local handle vector/batch.
- Remove `SessionReadyQueue`; do not rename or wrap it.
- Do not introduce `DedupFifo`, `ReadySession`, `Session<Ready>`, generic marker traits, or PhantomData-based state machines for this problem.
- Scheduling coalescing belongs to the session layer, fully isolated from transport state.
- Transport may request scheduling only through the session context; transport must not own, inspect, or clear pending scheduling state.
- Worker-local Session Work Batch uses append-and-drain vector semantics and owns no hash table or membership state.
- Close/disconnect is a Session Control Event lane, not a ready/session-work fact.
- Do not preserve `pending_closes` or a synthetic `close_requested` boolean on ready-session dispatch.
- Timer expiry must dispatch the exact timer token supplied by runtime; do not scan timer kinds or use session work scheduling to discover expired timers.
- RX enqueue locality remains session/runtime-owned: transport supplies buffer identity, relative offset, and in-order/OOO facts; Session Runtime owns FIFO enqueue, app notification, and RX capacity facts.
- Use `CoreResult` for errors; do not add a new RX error enum.

## Task 1: Replace SessionReadyQueue with session-owned scheduling

### Goal

Make session scheduling match VPP's session pending flag plus worker-local vector shape. Remove the old generic queue and its hash-backed membership state.

### What to build

- Introduce a private session-layer entry around transport state that carries a scheduling pending bit.
- Store the private entry in the Session Runtime pool while keeping transport-facing accessors returning only transport state references.
- Replace the worker's ready queue with a worker-local Session Work Batch using `hammer_infra::vec::Vec<SessionId>`.
- Keep `context.mark_ready()` as the transport-facing scheduling request, backed by pending-bit transition plus vector append.
- Remove the `SessionReadyQueue` type, public export, and tests that verify that removed data structure.
- Rename step/result vocabulary away from `ready_sessions` toward scheduled/session-work terminology where the API is crate-local.

### Acceptance criteria

- Scheduling the same session multiple times before dispatch produces one scheduled work entry.
- Dispatch clears the session pending bit so the same session can be scheduled again in a later turn.
- Transport-facing code cannot access the pending bit or worker batch directly.
- There is no `SessionReadyQueue` type, export, or direct test.
- Focused tests demonstrate the scheduling behavior through Session Runtime dispatch or public runtime APIs.

### Required TDD

First add a failing behavior test for duplicate suppression and rescheduling through the Session Runtime seam. Then implement the minimal runtime changes to pass it.

## Task 2: Split app TX work from disconnect control events

### Goal

Align app/runtime event classification with VPP's split between session work and session control events.

### What to build

- Add the current narrow control-event lane for disconnect only.
- Classify `SessionEvtType::TxDeq` as session work scheduling.
- Classify `SessionEvtType::Close` as `SessionControlEvent::Disconnect`.
- Remove `pending_closes`.
- Remove the synthetic `close_requested` parameter from ready/session-work handling.
- Add a transport protocol hook for disconnect control dispatch if needed, keeping ready/session-work handling free of close booleans.
- Ensure same-turn `Close + TxDeq` dispatch handles disconnect before scheduled TX work and skips stale work after removal/close.

### Acceptance criteria

- App TX events schedule session work.
- App close events dispatch through the control-event lane.
- Ready/session-work dispatch receives no close-request boolean.
- Same-turn close plus TX work invokes close handling first and does not run TX work for a removed session.
- Focused tests demonstrate app event classification and ordering through Session Runtime dispatch.

### Required TDD

First add failing behavior tests for `TxDeq` classification, `Close` classification, and same-turn ordering. Then implement the minimal control lane to pass them.

## Task 3: Keep timer dispatch token-exact and scheduling-neutral

### Goal

Preserve exact timer-token dispatch while removing the old implicit ready scheduling from timer expiry.

### What to build

- Ensure timer expiry records and dispatches the exact timer token supplied by runtime.
- Timer expiry must not schedule session work merely because a timer expired.
- If a transport timer handler needs TX/session work, it schedules through the session context.
- Update existing timer/session queue tests to assert token delivery and scheduling neutrality.

### Acceptance criteria

- Expiring a timer produces the expected `ExpiredTimer` token and does not by itself add session work.
- A timer handler can still schedule work through `context.mark_ready()`.
- Existing TCP timer behavior remains covered by focused tests.

### Required TDD

First adjust/add a failing timer behavior test that proves timer expiry does not imply scheduled session work. Then implement the minimal change to pass it.

## Task 4: Model RX enqueue locality with RxDelivery

### Goal

Replace the old receive enqueue field bag with a transport-neutral result type that models legal RX delivery outcomes and keeps FIFO/app notification ownership in Session Runtime.

### What to build

- Replace `SessionRxEnqueue` with `RxDelivery`.
- Add `OooSpan` with a non-zero length invariant.
- Represent accepted in-order delivery, accepted out-of-order delivery, and not-accepted delivery as distinct outcomes.
- Keep errors on the existing `CoreResult` boundary.
- Keep RX FIFO enqueue, app notification, and RX capacity facts inside Session Runtime.
- Update TCP receive paths to consume `RxDelivery` for sequence, ACK, and SACK decisions without owning FIFO/app notification behavior.
- Add a size guard for the hot-path result.

### Acceptance criteria

- Accepted-byte variants cannot represent zero accepted bytes.
- OOO delivery cannot carry a zero-length OOO span.
- In-order delivery cannot carry OOO facts.
- Not-accepted delivery carries current RX capacity without adding a new error enum.
- TCP receive behavior remains covered for in-order, OOO, and full/not-accepted cases.
- Focused tests prove the legal outcome modeling and existing receive behavior.

### Required TDD

First add failing tests for the new legal outcomes, including zero-accepted/not-accepted behavior and OOO non-zero span behavior. Then implement the minimal RX result changes to pass them.

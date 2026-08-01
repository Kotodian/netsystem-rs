# VPP Feature Review: Session event lanes and private Application MQ drain

## Feature and changed surface

Issue #186 moves Application and internal Session MQ events directly into
worker-owned CTRL, new-IO, and old-IO lanes. The changed production surface is
`hammer-runtime`'s Session MQ wrapper and `hammer-service`'s Session worker,
Application worker, and session-queue dispatch path.

## Vendored VPP evidence

- `third_party/vpp/src/vnet/session/session_node.c:1937-2025` keeps separate
  `ctrl_head`, `new_head`, and `old_head` lists. `session_wrk_handle_mq`
  snapshots `svm_msg_q_size()` before dequeueing, so events produced during a
  drain remain for a later pass.
- `third_party/vpp/src/vnet/session/session_node.c:2030-2115` runs the session
  queue in this order: time updates, internal MQ, control events, new IO, old
  IO, and one TX-buffer flush.
- `third_party/vpp/src/vnet/session/session_node.c:1428-1740` moves work to the
  old list only after an attempt defers it (no send space, pacing,
  allocation/output budget, or remaining FIFO data). A newly scheduled IO event
  is not old merely because it was observed later.
- `third_party/vpp/src/vnet/session/session.c:42-70,108-134` allocates normal
  TX and rescheduled TX events on the IO/new path; explicit custom scheduling
  selects new or old.
- `third_party/vpp/src/vnet/session/application.c:374-405` drains private
  Application MQs through the same worker MQ handling and re-adds postponed
  work rather than consuming an unbounded producer stream in one pass.

## Hammer correspondence

- `SessionMsgQueue` has exactly two configured rings, `[Io, Ctrl]`. The
  descriptor's ring is retained by `dequeue_with_ring`; no `SessionEvtType`
  enumeration is used to reconstruct the producer's choice, and no impossible
  "unknown ring" error or panic is introduced.
- `AppSession` exposes separate `push_io_event*` and `push_control_event`
  operations. The producer now chooses the queue ring at the API boundary;
  `SessionEvtType` is no longer matched to guess a ring and invalid calls no
  longer reach a forced panic arm.
- `AppRxMqEntry::drain_snapshot_to`, `AppWorker::drain_tx_events_to`, and
  `SessionWorker::poll_session_events` use the initial queue length as the VPP
  snapshot boundary. The drains pass `(ring, event)` directly to staging.
- `SessionWorker::mark_ready` stages new IO. Packetized TX continuation and
  output-budget deferral use `reschedule_old`. Control scheduling uses the
  control lane explicitly.
- `dispatch_session_queue_events` takes ownership of each lane snapshot,
  processes control, then new IO, then the old snapshot, and restores any
  unconsumed snapshot entries ahead of events appended during dispatch.
  Transport-mismatched IO is retained for its matching attachment; worker
  mismatched Close/HalfClose control events retain the ADR-0010 drop behavior.
- Iterator rewrites are limited to bounded snapshots, list drains, setup
  attachment walks, and failure-atomic construction/rollback walks. Protocol
  advance budgets, FIFO-chain traversal, retry loops, and TX batch construction
  retain their explicit early-exit/state semantics.

## Verdict

**Aligned.** Hammer follows VPP's worker ownership, bounded MQ snapshot, lane
ordering, and new/old lifecycle semantics. The explicit ring-carrying dequeue
is a Hammer boundary detail: producers already select one of the two configured
rings, so the runtime preserves that fact instead of maintaining a second
event-type classification table.

## Findings

### Blocking

None found in the reviewed issue surface.

### Non-blocking

The lower-level `MultiRingMsgQueue` still stores a raw ring index because it is
generic infrastructure. `SessionMsgQueue` is the owner that fixes the layout to
two rings and is therefore the correct boundary for the typed `SessionMqRing`
value.

## Verification

- Vendored VPP source inspected under `third_party/vpp/src/vnet/session/`.
- `cargo fmt --all`
- `git diff --check`
- `cargo check -p hammer-service --tests`
- `cargo test -p hammer-service --lib` (51 passed)
- `cargo test -p hammer-runtime --test session_msg_queue` (12 passed)
- `cargo test -p hammer-runtime app::session::tests --lib` (17 passed)
- `cargo test -p hammer-service session::runtime::tests --lib` (11 passed)
- `cargo test -p hammer-service session::node::tests --lib` (10 passed)

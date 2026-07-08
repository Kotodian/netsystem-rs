# Task 2 Report: Split app TX work from disconnect control events

## Status

DONE

## Summary

Implemented the Task 2 session runtime split between app TX work and app disconnect control events.

Changes made:
- Added a narrow worker-local `SessionControlEvent::Disconnect(SessionId)` lane.
- Classified `SessionEvtType::TxDeq` as session work scheduling.
- Classified `SessionEvtType::Close` as disconnect control dispatch.
- Removed `pending_closes`.
- Removed the synthetic `close_requested` boolean from `SessionQueueProtocol::handle_ready_session`.
- Added `SessionQueueProtocol::handle_disconnect` for transport-owned disconnect handling.
- Kept TCP local-close behavior by moving the previous close-request handling out of ready work and into `handle_disconnect`.
- Ensured control events dispatch before session work, so same-turn `Close + TxDeq` skips stale work after close/removal.

## VPP reference

The change follows the VPP session split documented in local architecture context and ADR 0003: TX/RX readiness is session work, while disconnect/close is session control work. I also checked VPP session sources as the semantic reference:
- `src/vnet/session/session_node.c`: session queue node dispatch separates IO events from control events.
- `src/vnet/session/session_types.h`: session event types include separate control-event categories.

References:
- `CONTEXT.md`
- `docs/adr/0003-session-rx-enqueue-locality.md`
- https://github.com/FDio/vpp/blob/master/src/vnet/session/session_node.c
- https://github.com/FDio/vpp/blob/master/src/vnet/session/session_types.h

## TDD evidence

Added behavior tests first in `crates/hammer-service/src/session/runtime.rs`:
- `app_tx_deq_event_schedules_session_work`
- `app_close_event_dispatches_disconnect_control_event`
- `same_turn_close_dispatches_before_tx_work_and_skips_removed_session`

RED results:
- `cargo test -p hammer-service event`
  - The `TxDeq` guard passed under old behavior because the old implementation scheduled all app events as work.
  - `app_close_event_dispatches_disconnect_control_event` failed because `Close` entered ready handling (`ready_calls == 1`).
- `cargo test -p hammer-service same_turn_close_dispatches_before_tx_work_and_skips_removed_session`
  - Failed because same-turn close plus TX reached TX work (`send_params must not run`).

GREEN results:
- After implementation, the focused event and same-turn tests passed.

## Files changed

- `crates/hammer-service/src/session/app.rs`
  - Changed app TX-event drain to pass through event type.
  - Clears the TX edge only for `TxDeq`.

- `crates/hammer-service/src/session/runtime.rs`
  - Added `SessionControlEvent::Disconnect`.
  - Added control-event queue storage and dispatch before session work.
  - Removed `pending_closes` and `take_close_request`.
  - Removed `close_requested` from ready dispatch.
  - Added `handle_disconnect` to the session protocol trait.
  - Added behavior tests for classification and same-turn ordering.

- `crates/hammer-service/src/transport/tcp/mod.rs`
  - Moved app disconnect behavior into `handle_disconnect`.
  - Kept `handle_ready_session` free of close booleans.

- `crates/hammer-service/tests/session_queue_dispatch.rs`
  - Updated the test protocol implementation for the new trait method.

## Verification

Commands run:

```bash
cargo fmt --all
cargo fmt --all -- --check
cargo test -p hammer-service event
cargo test -p hammer-service same_turn_close_dispatches_before_tx_work_and_skips_removed_session
cargo test -p hammer-service session::runtime::tests
cargo test -p hammer-service
```

Results:
- `cargo fmt --all -- --check`: passed.
- `cargo test -p hammer-service event`: passed, 2 matching tests.
- `cargo test -p hammer-service same_turn_close_dispatches_before_tx_work_and_skips_removed_session`: passed, 1 matching test.
- `cargo test -p hammer-service session::runtime::tests`: passed, 14 matching tests.
- `cargo test -p hammer-service`: passed, including 137 library tests plus crate integration tests; the crate still emits pre-existing warnings.

## Self-review

Acceptance check:
- App TX events schedule session work: yes, `TxDeq` marks schedule pending and enters the Session Work Batch.
- App close events dispatch through control-event lane: yes, `Close` becomes `SessionControlEvent::Disconnect`.
- Ready/session-work dispatch receives no close-request boolean: yes, `handle_ready_session` no longer has that parameter.
- Same-turn close plus TX handles disconnect before scheduled TX work: yes, control events drain before `take_scheduled_session_work`.
- Stale work after removal/close is skipped: yes, removed sessions are skipped when the work batch drains.
- Focused tests demonstrate classification and ordering through Session Runtime dispatch: yes.

Scope check:
- Did not implement Task 3 timer-neutrality changes.
- Did not implement Task 4 `RxDelivery`.
- Did not add shutdown/reset placeholder control variants.
- Did not add TCP-specific runtime or buffer APIs.

Concerns:
- None.

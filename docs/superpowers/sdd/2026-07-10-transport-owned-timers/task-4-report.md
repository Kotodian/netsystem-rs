# Task 4 Report: Lock The Transport-Owned Timer Boundary

## Result

- Added production-source architecture guardrails for the Session and TCP timer ownership boundaries.
- Removed the Session timer wheel, tick clock, expired/pending timer queues, timer pass-through accessors, and tick-based test dispatcher.
- Removed TCP raw timer ids/count/masks, Session-wheel reconciliation, raw timer entrypoints, and all-kind refresh representation tests.
- Kept TCP timer identity, scheduling, expiry, and rearming behind the private `TcpTimerKind`, `TcpTimerState`, and `TcpTimers` seam.
- Migrated same-turn close, TX atomicity, delayed ACK, SYN retransmit, and TIME_WAIT tests to the typed transport-worker path.
- Left `CONTEXT.md`, ADR 0008, and the approved design spec unchanged because the final identifiers match their approved vocabulary.

No new type, public API, infrastructure API, dynamic dispatch, protocol enum, `TcpQueue`, or `Live` state was introduced.

## TDD Evidence

The new guardrail was run before implementation and failed as expected:

- `session_modules_do_not_own_or_dispatch_transport_timers`: `session owns forbidden TimerWheel`
- `legacy_tcp_queue_and_timer_reconciliation_surfaces_are_removed`: `TCP retains forbidden sync_all_tcp_timers`

After cleanup, both guardrails pass.

## Verification

- `cargo test -p hammer-service --test transport_worker_boundary`: 2 passed.
- `cargo test -p hammer-service --test tcp_state_machine --test vpp_session_tx_guardrails`: 11 and 5 passed.
- `cargo fmt --all -- --check`: passed.
- Session and TCP legacy-surface `rg` checks: no matches.
- `git diff --check`: passed.

Per the Task 4 instruction, the full `hammer-service` and workspace test suites were not run. Test output retains pre-existing repository deprecation and dead-code warnings.

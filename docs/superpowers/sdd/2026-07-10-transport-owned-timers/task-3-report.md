# Task 3 Report: Migrate TCP Timer Policy Into TcpWorker

## Status

Task 3 is complete. TCP timer scheduling and exact-token dispatch run through
`TcpWorker` and its private `TcpTimers`. Packet nodes pass `PoolIndex`,
`&mut TcpTimers`, and `Instant` into connection policy operations. Session
Runtime no longer dispatches raw TCP timer ids. Legacy storage and unused raw
surfaces remain for Task 4 deletion, as required by the task split.

No new type, public API, `hammer-infra` API, dynamic dispatch, protocol enum,
timer action carrier, epoch/nonce/binding/context, or payload copy was added.

## Implementation Summary

- Moved immediate per-kind timer set/update/reset decisions into typed TCP
  connection operations invoked by `TcpWorker` and TCP packet nodes.
- Made `TcpWorker::update_time` advance its private timer clock, drain exact
  generation-safe tokens, invoke only `token.kind`, emit existing TCP output,
  schedule session work, and publish transport lifecycle notifications.
- Preserved the Session FIFO ownership boundary and static transport generics.
- Migrated delayed ACK, exact-token, and TIME_WAIT legacy tests away from the
  Session wheel and through the TCP-owned timer seam.
- Added deadline isolation coverage for delayed ACK, keepalive, retransmit,
  persist, TIME_WAIT, RACK, TLP, and pacing.
- Fixed duplicate ACK policy so duplicate SACKs and nonzero send-window changes
  do not refresh RACK/TLP. The policy updates those deadlines only when an
  accepted ACK changes `snd_una`, bytes in flight, the RACK deadline, or crosses
  the zero-window boundary.
- Rejected send-window mutation from out-of-range ACKs, preventing invalid ACKs
  from switching Persist versus RACK/TLP policy.
- Narrowed typed timer methods to TCP-module visibility and removed obsolete
  timer imports without deleting Task 4 legacy definitions.

## RED History And Root Cause

The handoff recorded `cargo test -p hammer-service --lib transport::tcp::` as
138 passed / 4 failed:

- `session_tcp_delayed_ack_timer_emits_ack_after_first_clean_payload`
- `tcp_timer_dispatch_uses_exact_timer_token`
- `tcp_time_wait_expiry_closes_session`
- `typed_tcp_worker_graph_isolated_by_runtime_and_backend`

The first three still armed or synchronized the Session wheel after production
expiry ownership had moved to `TcpWorker`; no TCP token could be produced from
that wheel. The fourth failure was environmental: `Svm::default` could not
create shared memory in the sandbox (`EPERM`). On this continuation, the handed
off diff already contained the three test migrations, so the first fresh TCP
module run was 142/142 rather than reproducing the earlier state. The removed
side of the diff retains the exact original Session-wheel assertions and the
handoff count above records the observed RED phase.

Independent review found one additional policy defect. Separate RED runs
proved each case before its minimal fix:

1. Duplicate ACK: RACK was pending at the original 20 ms deadline, but TLP was
   still armed because `tlp_timeout()` returned a full interval and
   `sync_recovery_timers` called `update`.
2. Duplicate ACK plus a nonzero send-window magnitude change: TLP again missed
   the original deadline.
3. Duplicate SACK: TLP again missed the original deadline despite no new
   recovery fact.
4. Out-of-range ACK carrying a zero window: `snd_wnd` changed from 65535 to 0,
   which could cancel TLP and arm Persist.

All four focused regressions passed after policy was gated on actual accepted
ACK effects and `apply_ack` stopped mutating state for out-of-range ACKs.

## Verification Evidence

- Seven brief-listed focused timer tests: each passed individually.
- `cargo test -p hammer-service --lib tcp_unrelated_ack_does_not_move`:
  2 passed.
- `cargo test -p hammer-service --lib transport::tcp::connection::`:
  47 passed.
- `cargo test -p hammer-service --lib transport::tcp::legacy_tests::tcp_packetized_tx_rolls_back_transport_state_when_batch_fails -- --exact`:
  1 passed.
- `cargo test -p hammer-service --lib transport::tcp::` with SVM permission:
  144 passed.
- `cargo test -p hammer-service --test tcp_state_machine --test tcp_output --test tcp_session_app_boundary --test session_queue_dispatch`:
  29 passed (14 + 6 + 7 + 2).
- `cargo test -p hammer-service -- --test-threads=1` with SVM permission:
  167 library tests passed; all integration and doc suites passed; 261 total
  passed and 2 performance probes ignored.
- `cargo fmt --all -- --check`: exit 0.
- `git diff --check`: exit 0.
- Production-path search for `sync_all_tcp_timers`, raw
  `on_tcp_timer_expiry`, all-kind loops, and Session `timers_mut` in worker,
  packet-node, and SessionQueueNode paths: no matches.

The default parallel full-service run stalled for more than three minutes in
`interface_updates_run_through_configured_runtime_data_plane_barrier`. The
isolated command
`cargo test -p hammer-service --test interface_control interface_updates_run_through_configured_runtime_data_plane_barrier -- --exact --test-threads=1`
passed in 0.02 s, identifying an unrelated test-isolation issue. A sandboxed
single-thread run then reached 164 passed / 1 failed at the known SVM `EPERM`.
The exact SVM graph test passed 1/1 with elevated permission, and the final
elevated single-thread service suite passed completely.

## Review Closure

The first independent review found the TLP refresh, broad window/SACK gating,
invalid-ACK window mutation, delayed-ACK raw test dispatch, and the existing TX
connection clone. The delayed-ACK test now invokes `on_typed_timer_expiry`.
The deadline and invalid-ACK findings were resolved with RED/GREEN coverage.
A final targeted re-review reported no remaining Critical or Important finding.

The Session legacy bridge was not restored: Task 3 explicitly removes raw
timer delivery, while Task 4 owns deletion of dead wheel/tick fields and raw
legacy definitions.

## External Review Fixes

External review after commit `0b26a9a7` found two remaining transaction
semantics defects and one typed-gate cleanup. They were handled with the
explicitly approved TCP-private prevalidation scope.

### ACK-progress RTO restart

RED: `tcp_ack_progress_restarts_retransmit_deadline_at_current_rto` sent two
outstanding ranges, advanced the timer clock through half of the original RTO,
and cumulatively ACKed the first range. The assertion immediately before the
new ACK-relative deadline failed because the old send-relative Retransmit timer
had already expired.

GREEN: when `snd_una` actually advances and recovery still owns unacked data,
`receive_ack_with_timers` now calls typed `update` with the current RTO. A
duplicate or unrelated ACK still uses `set` and preserves the deadline; ACKing
all outstanding data still resets Retransmit. The exact deadline test passed.

### Post-TX timer-plan atomicity

RED: `tcp_payload_timer_sync_validation_preserves_existing_timer_transaction`
armed an original 20 ms TLP, retained unacked data, and configured the private
keepalive idle interval to 24 hours, beyond the private TCP wheel horizon.
`sync_payload_tx_timers` returned an error only after adding Retransmit to the
timer state, proving partial mutation before the later keepalive failure.

GREEN: `TcpTimers::validate_interval` now privately validates duration-to-tick
conversion against the TCP wheel's current tick and horizon. The connection
computes the complete post-TX Retransmit/RACK/TLP/Persist/Pacing/KeepAlive plan
and validates every set/update interval before the first wheel or timer-state
mutation. The failure-injection test passed and proved both the original timer
state and the 20 ms TLP deadline remain intact. Horizon and tick policy remain
private to `TcpTimers`; no public type, public API, action carrier, transaction
type, or `hammer-infra` API was added.

The live typed expiry gate now accepts `TcpTimerKind` directly. Raw `u32`
matching remains only in the explicitly named legacy compatibility gate for
Task 4 deletion.

Focused verification after these fixes:

- Exact RTO restart test: 1 passed.
- Exact post-TX atomicity failure-injection test: 1 passed.
- `cargo test -p hammer-service --lib transport::tcp::connection::`: 49 passed.
- Existing failed packetized-TX atomicity test: 1 passed.
- `cargo fmt --all -- --check`: exit 0.
- `git diff --check`: exit 0.

Per controller instruction, the full service suite was not rerun in this fix
turn; it is reserved for post-review verification.

## Concerns

`TcpWorker::tx_action` still deep-clones `TcpConnection` for the pre-existing
Task 1 batch failure-atomicity contract. The approved private interval
prevalidation closes the known wheel/live-connection incoherence path without
replacing that clone or introducing a transaction API. The existing
failed-batch test remains green.

Legacy Session timer fields, raw TCP timer constants/helpers, and their
source-level guardrail cleanup intentionally remain for Task 4.

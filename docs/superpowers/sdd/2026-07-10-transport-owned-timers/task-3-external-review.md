# Task 3 External Review

## Verdict

**Needs fixes.** No Critical findings; two Important correctness/coherence findings remain.

The review compared base `5ddfd4a6` with head `0b26a9a7` using the supplied
`task-3-review.diff`, then checked the brief, progress constraints, design, and
implementer report. I did not rerun the reported test suites.

## Critical

None.

## Important

### 1. An ACK that advances `snd_una` does not restart the retransmit deadline

**Evidence:** `crates/hammer-service/src/transport/tcp/connection.rs:1749-1771`.
The method records `snd_una_before` and computes `recovery_progress`, but the
Retransmit branch always calls `timers.set(...)` while unacked data remains.
That preserves an already armed deadline. It correctly protects duplicate or
otherwise unrelated ACKs from moving the RTO, but it also preserves the old
deadline when a cumulative ACK advances `snd_una` and leaves later bytes
outstanding.

This is a Task 3 regression from the prior reconciliation behavior and from
normal TCP RTO policy: once new data is acknowledged, the retransmission timer
must cover the new oldest unacknowledged byte. Keeping the earlier deadline can
cause a premature timeout, unnecessary retransmission, and congestion-control
backoff. The new test at
`crates/hammer-service/src/transport/tcp/connection.rs:3099` proves only the
duplicate-ACK half of the policy, so it cannot catch this case.

**Required action:** distinguish cumulative ACK progress from an unrelated ACK.
When `snd_una` advances and unacked data remains, update Retransmit using the
current RTO; when no relevant progress occurred, use set/preserve semantics;
when nothing remains unacked, reset it. Add a deadline test that ACKs the first
of multiple outstanding ranges after part of the original RTO has elapsed and
proves the timer expires relative to the advancing ACK, not the original send.

### 2. TX timer synchronization can leave the wheel and live connection incoherent on error

**Evidence:** `crates/hammer-service/src/transport/tcp/worker.rs:316-328` and
`crates/hammer-service/src/transport/tcp/connection.rs:1629-1670`.
`tx_action` clones the connection, restores the live connection's timer-state
bits into the candidate, and then calls `sync_payload_tx_timers` before
publishing the candidate. That sync performs several wheel mutations in
sequence, each with `?`. If an early mutation succeeds and a later one fails,
the private wheel retains the successful mutation while the live connection
retains its old `TcpTimerState`, because the candidate assignment has not run.
Expiry can then be discarded as apparently unarmed, or a canceled/replaced
deadline can no longer match the live state.

This violates the approved design requirement that a failed transport action
leave timer ownership coherent. It is separate from the cost of cloning
`TcpConnection`: the clone is pre-existing Task 1 behavior explicitly required
for packetized-TX failure atomicity and is not a finding. The new problem is the
fallible, externally mutating timer sequence placed between candidate mutation
and candidate commit. The existing rollback test at
`crates/hammer-service/src/transport/tcp/mod.rs:1092` fails during batch
construction, before this timer-sync path, and therefore does not cover it.

**Required action:** make timer synchronization atomic with the connection
commit using approved existing surfaces, or establish and enforce an invariant
that makes these private post-validation timer operations infallible. If that
cannot be done without a new transaction/reservation API, stop and obtain the
explicit approval required by the design rather than accepting partial wheel
mutation as residual risk. Add a failure-injection test at a timer operation
after at least one earlier operation has succeeded.

## Minor

### 1. The typed expiry path still routes its state gate through a raw timer id

**Evidence:** `crates/hammer-service/src/transport/tcp/connection.rs:2257-2269`
and `crates/hammer-service/src/transport/tcp/connection.rs:2332-2344`.
Exact dispatch itself correctly matches `TcpTimerKind`, but the live typed path
converts the kind to `u32` for `timer_dispatch_pending`, whose comment still
describes a raw runtime-supplied timer id. This does not recreate Session raw
delivery, but it leaves the production typed path coupled to the legacy raw-id
surface. Make the live gate accept `TcpTimerKind`; Task 4 can then delete the
dead raw compatibility path and its constants cleanly.

## Spec Compliance Notes

- Session raw timer delivery is removed: `SessionTransport` and
  `SessionTransports` no longer expose `handle_legacy_timer`, and the Session
  queue no longer drains `pending_timers` into transports. The dead Session
  storage/helpers that remain are consistent with the stated Task 4 split.
- Production packet and worker paths pass `hammer_infra::pool::Index`,
  `&mut TcpTimers`, and `Instant` directly and dispatch the exact private
  `TcpTimerKind`; no all-kind production scan was added.
- Static transport generics, Session/TCP ownership separation, FIFO payload
  ownership, and existing `TcpSegment` output are preserved. No new type,
  public API, `hammer-infra` API, timer-action carrier, epoch/binding/context,
  `TcpQueue`, `Live`, dynamic dispatch, or payload copy appears in the package.
- The Task 1 `TcpConnection` clone was adjudicated as required by the approved
  packetized-TX failure-atomicity contract. Replacing it with an unapproved
  snapshot/transaction API is outside Task 3.

## Named Outside-Diff Risk Check

I inspected only `crates/hammer-service/src/transport/tcp/timers.rs:236-255`
outside the supplied Task 3 diff to check one concrete risk: whether
`TcpWorker::update_time` may receive a stale token and then fail its direct
`connections.get_mut(token.index)`. `TcpTimers::take_pending` validates the
generation-bearing pool index, verifies the exact pending bit, clears it, and
skips a token when that kind has been rearmed before returning. That named risk
is resolved by the Task 2 implementation.

## Verification Assessment

The report's focused, TCP integration, full `hammer-service`, formatting, and
diff-check results are internally consistent with the package, but they do not
exercise either Important case above. Per review instructions, I treated those
results as implementer evidence and did not rerun them.

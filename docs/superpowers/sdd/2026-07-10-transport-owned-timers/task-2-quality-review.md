# Task 2 Code-Quality Review

## Verdict

**CHANGES REQUESTED.** Commit `67a741c9` is not ready for Task 3 relative to
`f54d375f`. The exact-token state model is sound in the tested cases, but two
Important issues would make the policy migration refresh deadlines incorrectly
and make transport time updates unbounded or lose elapsed-time carryover.

No Critical findings were found.

## Important Findings

### 1. `set` has `update` semantics and can move an already armed deadline

**Location:** `crates/hammer-service/src/transport/tcp/timers.rs:136`

`TcpTimers::set` delegates directly to `update`, which calls
`TimerWheel::update_timer`. Therefore a second `set` for an already armed kind
restarts that timer relative to the wheel's current tick. This erases the
semantic distinction that justified separate `set` and `update` operations.

This is not VPP-compatible. VPP's `tcp_timer_set` asserts that the wheel handle
is invalid and starts a new timer, while `tcp_timer_update` updates a live
handle or starts one if absent. A pending expiry has no live handle, so the
approved pending-plus-newly-armed rearm remains legal; an already armed timer
must not be silently refreshed by `set`.

The distinction is operationally important for Task 3. The current connection
code contains many idempotent `timer_set` calls. Mapping one of those calls to
this API can move retransmit, keepalive, persist, pacing, or recovery deadlines
when the caller only intended to ensure that work was active. That recreates,
inside the new typed engine, the unrelated-deadline-refresh defect that this
refactor is intended to remove.

**Required fix:** make `set` start only when `armed` does not contain the kind.
It may still arm when only `pending` contains the kind, preserving the approved
rearm behavior. Keep `update` as update-or-start. Prefer `arm_timer` for the new
wheel entry and a debug assertion or explicit invariant handling for an
already armed `set`; do not call `update_timer` from `set`.

Add a regression test that arms a timer, advances partway, calls `set` again,
and proves expiry remains at the original deadline. Retain the existing pending
rearm test to prove that case still creates a new deadline.

VPP reference: `src/vnet/tcp/tcp_timer.h` in `FDio/vpp`, functions
`tcp_timer_set` and `tcp_timer_update`.

### 2. Absolute-time advance is unbounded and cannot preserve budget carryover

**Locations:** `crates/hammer-service/src/transport/tcp/timers.rs:128`,
`crates/hammer-service/src/transport/tcp/timers.rs:177`, and
`crates/hammer-service/src/transport/tcp/timers.rs:242`; underlying behavior at
`crates/hammer-infra/src/timer_wheel.rs:220`

The wheel is created with `max_expirations = 0`, which means unlimited, and
`elapsed_ticks` permits up to `u32::MAX` ticks in one call. `TimerWheel::expire`
advances with `for _ in 0..ticks`; it has no empty-wheel or large-jump fast path.
At the production 10 ms resolution, a 12-hour pause causes about 4.32 million
data-plane loop iterations in one transport update. The cap itself represents
about 497 days and can request roughly 4.29 billion iterations. A barrier,
debugger stop, host suspend, or clock anchor restored after a long pause can
therefore stall the worker for an unbounded interval.

The current clock arithmetic also blocks the expiry budget required by the ADR
and design. `elapsed_ticks` advances `last_update` by every requested tick
*before* calling `wheel.expire`. When a nonzero wheel expiration budget is
introduced, `expire` may stop after fewer ticks. This behavior is proven by
`timer_wheel_max_expirations_stops_expire_call_between_ticks`: a request for 10
ticks with budget 2 advances the wheel only to tick 2. With the current
`TcpTimers` clock code, the absolute anchor would nevertheless move by all 10
ticks, discarding eight ticks of carryover and delaying or losing later expiry.

**Required fix:** bound work per transport update and advance the absolute
anchor by actual wheel progress. This is compatible with the approved
`TcpTimers` shape; private constants are sufficient.

- Define private constants for maximum wheel ticks per update and the TCP
  expiry budget, and pass the budget to `with_timer_ids`.
- Compute a bounded requested tick count without mutating `last_update`.
- Snapshot `wheel.current_tick()`, call `expire`, calculate the consumed ticks
  from the wheel's new `current_tick()`, and advance `last_update` only by that
  consumed amount. The unconsumed absolute-time delta then carries into the
  next call.
- When the wheel is empty, fast-forward the absolute anchor by the completed
  elapsed intervals without iterating wheel ticks. This does not require the
  wheel's relative tick counter to match wall time because newly armed timers
  are relative to the current wheel position and the updated anchor.

Add focused tests for zero elapsed, an elapsed value above the per-call tick
cap, an empty-wheel large jump, and budget carryover with timers on successive
ticks. Do not validate this with a literal `u32::MAX` loop.

## Minor Findings

### 1. Timer identity is duplicated across several independently maintained mappings

**Locations:** `crates/hammer-service/src/transport/tcp/timers.rs:13`,
`crates/hammer-service/src/transport/tcp/timers.rs:15`,
`crates/hammer-service/src/transport/tcp/timers.rs:34`, and
`crates/hammer-service/src/transport/tcp/timers.rs:54`

The count, implicit enum discriminants, `from_id` match, and bitflag positions
must all stay synchronized. During the Task 2 bridge, the raw constants in
`connection.rs` add another mapping. They currently agree, but reordering or
inserting an enum variant can silently make `id()` and `flag()` disagree with
`from_id` or the legacy bridge.

Use explicit discriminants (and an explicit integer representation), or make
`id`/`flag` exhaustive matches rather than relying on declaration order. Add a
small all-kinds round-trip/unique-bit test. Task 4 can remove the legacy raw
mapping. `TcpTimerSet` itself can remain private to `timers.rs`; it does not
need `pub(super)` visibility.

### 2. The focused tests omit several design-level edge cases

**Location:** `crates/hammer-service/src/transport/tcp/timers.rs:252`

The five required Task 2 tests cover exact kind, pending reset/rearm,
generation reuse before wheel expiry, and update deadline replacement. They do
not cover duplicate raw expiry handling, a queued pending token after connection
deletion/reuse, zero elapsed, large elapsed, expiry-budget carryover, or the
set-versus-update distinction. The implementation appears to reject duplicate
raw payloads through `take_expired_timer` and to reject stale queued tokens
through the pool generation lookup, but those properties are unprotected.

Add those cases before Task 3 begins using the engine. In particular, keep the
pending queue lazy-cleanup assertions: reset and connection deletion may leave
physical tokens queued, but draining must remove them without changing a new
generation or a newly armed deadline.

## Confirmed Correct Behavior

- Expiry decodes one exact timer kind and does not scan all kinds.
- `PoolIndex` generation checks reject a wheel expiry for a removed connection.
- Expiry moves only the matching kind from `armed` to `pending`.
- Reset clears both typed states, and lazy pending-token cleanup prevents its
  canceled token from dispatching.
- Rearming while pending leaves `pending + armed`; draining the old token clears
  pending and preserves the newly armed deadline.
- `take_expired_timer` clears the wheel's timer-id slot, so a duplicate raw
  payload in the same scratch batch is ignored.
- Duration-to-tick conversion uses ceiling division, enforces one tick, and
  reports wheel horizon/overflow failures through `CoreResult`.
- The new module is private to TCP and has no `SessionWorker` dependency.
- The Task 2 legacy bridge remains behavior-compatible in the focused exact
  token test; `TcpWorker::update_time` still intentionally leaves the new timer
  engine unused until Task 3.

## Verification

- `cargo test -p hammer-service --lib transport::tcp::timers::tests -- --nocapture`:
  PASS, 5 passed, 0 failed.
- `cargo test -p hammer-infra --test timer_wheel timer_wheel_max_expirations_stops_expire_call_between_ticks -- --exact --nocapture`:
  PASS, 1 passed, 0 failed. The test confirms a budgeted expire call may consume
  fewer ticks than requested.
- `cargo test -p hammer-service --lib transport::tcp::legacy_tests::tcp_timer_dispatch_uses_exact_timer_token -- --exact --nocapture`:
  PASS, 1 passed, 0 failed.
- `git diff --check f54d375f..67a741c9`: PASS.

Only focused tests were run, as requested. Existing repository warnings were
emitted; no new warning was used as a finding except the intentionally unused
Task 2 `TcpWorker::timers` field, which becomes live in Task 3.

## Task 3 Readiness

Task 3 should not start until both Important findings are fixed and their
regressions are covered. The exact-token state machine, generation safety, and
private ownership boundary are otherwise suitable foundations for Task 3.

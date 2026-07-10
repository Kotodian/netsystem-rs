# Task 2 Code-Quality Review

## Verdict

**PASS.** Fix commit `5ce6efcd` resolves the Task 2 quality findings relative
to `67a741c9`. No Critical, Important, or Minor findings remain. The private
exact TCP timer engine is ready for Task 3.

## Finding Resolution

### Set and update now preserve distinct deadline semantics

`TcpTimers::set` returns without touching the wheel when the kind is already
armed (`crates/hammer-service/src/transport/tcp/timers.rs:143`). It uses
`arm_timer` only when no armed deadline exists. This preserves the original
deadline for repeated set operations while still allowing the approved
pending-plus-newly-armed rearm case, because pending alone is not armed.

`TcpTimers::update` remains update-or-start through `update_timer`
(`crates/hammer-service/src/transport/tcp/timers.rs:177`). The distinction now
matches the VPP semantic reference: set requires no live timer handle, while
update may replace a live deadline or start an absent one.

The new `tcp_timer_repeated_set_preserves_original_deadline` regression proves
that a second set after partial elapsed time does not move expiry. The existing
pending rearm and update-deadline tests remain green.

### Time advancement is bounded and preserves actual carryover

TCP now configures a 256-expiry wheel budget and limits each non-empty-wheel
advance to 1,024 ticks (`crates/hammer-service/src/transport/tcp/timers.rs:13`
and `crates/hammer-service/src/transport/tcp/timers.rs:131`). A large absolute
time jump therefore cannot request the previous multi-million or `u32::MAX`
tick loop in one transport update.

`advance` snapshots `wheel.current_tick`, calls the budgeted wheel, measures the
actual tick delta, and advances `last_update` by only that consumed delta
(`crates/hammer-service/src/transport/tcp/timers.rs:208`). If the wheel stops
between ticks after reaching its expiry budget, the unconsumed absolute-time
delta remains available to the next call. The focused budget test proves this
with 257 successive deadlines: the first call consumes 256 ticks/tokens and the
second consumes the retained final tick.

An empty wheel takes a separate fast-forward path
(`crates/hammer-service/src/transport/tcp/timers.rs:203`). It advances the
absolute anchor by completed resolution intervals without iterating the wheel,
preserves the sub-tick remainder, and leaves the relative wheel position valid
for subsequently armed timers. The test arms a new timer after a large empty
jump and proves it expires only when the preserved remainder completes a tick.

The arithmetic is bounded by the sampled `Instant` delta: completed ticks
multiplied by the resolution cannot exceed the elapsed `Duration`, and
non-empty progress is at most 1,024 ticks. No overflow or `Instant` overshoot
path was found.

### Identity, stale-token, and duplicate-token coverage is explicit

`TcpTimerKind` now has `#[repr(u32)]` and explicit discriminants, and the count
is derived from the final discriminant (`crates/hammer-service/src/transport/tcp/timers.rs:17`).
`TcpTimerSet` visibility was narrowed to its module. The all-kinds round-trip
test proves IDs and flag bits are unique and cover exactly `0xff`.

Generation rejection is covered both before an expired wheel payload becomes a
queued token and after a token has already been queued. Reusing the pool slot
does not alter the replacement connection's timer state. A deliberately
duplicated queued token dispatches once: the first token clears pending and the
second is discarded by the pending-state check.

The exact-kind expiry, reset-while-pending, pending rearm, update replacement,
zero elapsed, large elapsed, budget carryover, generation reuse, and duplicate
token paths are all directly covered.

## Shape And Boundary Check

- `TcpTimers` retains exactly the approved fields: wheel, expired scratch,
  pending typed queue, absolute anchor, and resolution.
- `TcpTimerState` remains exactly the approved `armed` and `pending` sets.
- `TcpTimerToken` remains exactly the generation-safe pool index and exact kind.
- The fix adds only private constants, explicit enum representation, private
  methods, and tests. It adds no new type, struct field, public API, hammer-infra
  API, epoch, nonce, timer action carrier, or SessionWorker dependency.
- The TCP timer module remains private and the intentionally transitional
  legacy Session timer bridge remains unchanged for Task 2.

## Verification

- `cargo test -p hammer-service --lib transport::tcp::timers::tests -- --nocapture`:
  PASS, 13 passed, 0 failed.
- `cargo test -p hammer-service --lib transport::tcp::legacy_tests::tcp_timer_dispatch_uses_exact_timer_token -- --exact --nocapture`:
  PASS, 1 passed, 0 failed.
- `cargo test -p hammer-infra --test timer_wheel timer_wheel_max_expirations_stops_expire_call_between_ticks -- --exact --nocapture`:
  PASS, 1 passed, 0 failed.
- `git diff --check 67a741c9..5ce6efcd`: PASS.

Only focused tests were run, as requested. Existing repository warnings were
emitted; no warning is attributable to the fix. No broad crate or workspace
suite was run for this re-review.

## Task 3 Readiness

Task 3 may proceed. The engine now provides distinct set/update semantics,
bounded time work, budget carryover, empty-wheel fast-forward, exact typed
dispatch, generation safety, and stable private identity mapping without
changing the approved ownership boundary.

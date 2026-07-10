# Task 2 Spec Review

## Verdict

PASS. Commit `67a741c9` complies with Task 2 relative to `f54d375f`.
No blocking spec findings were found.

## Approved Shape And State Semantics

- The private TCP timer module defines exactly the eight approved kinds:
  retransmit, RACK, TLP, delayed ACK, persist, keepalive, TIME_WAIT, and
  pacing (`crates/hammer-service/src/transport/tcp/timers.rs:15`). The typed
  set is backed by `u16` and assigns one bit to each kind
  (`crates/hammer-service/src/transport/tcp/timers.rs:54`).
- `TcpTimerState` contains separate `armed` and `pending` typed sets
  (`crates/hammer-service/src/transport/tcp/timers.rs:68`). `is_active` and
  `active_bits` use their union (`crates/hammer-service/src/transport/tcp/timers.rs:74`).
- `advance` consumes each raw wheel expiry through `take_expired_timer`,
  reconstructs one generation-safe `PoolIndex` and one exact kind, validates
  the connection generation, and moves only that kind from armed to pending
  before queuing one typed token
  (`crates/hammer-service/src/transport/tcp/timers.rs:177`). There is no
  timer-kind scan in this path.
- Reset synchronizes the wheel and clears both armed and pending state
  (`crates/hammer-service/src/transport/tcp/timers.rs:146`). Dispatch drops a
  queued token when its pending bit was reset, its connection generation is
  stale, or the same kind has been rearmed; rearm uses the existing
  pending-plus-armed state and introduces no epoch or nonce
  (`crates/hammer-service/src/transport/tcp/timers.rs:208`).
- `TcpTimerToken` contains only the approved generation-safe index and exact
  kind, while `TcpTimers` contains only the approved wheel, raw expiry
  scratch, typed pending queue, absolute clock anchor, and resolution
  (`crates/hammer-service/src/transport/tcp/timers.rs:107`). The new module is
  private to TCP (`crates/hammer-service/src/transport/tcp/mod.rs:40`), and no
  new public type or public API is exported.

## Wheel And Time Semantics

- `set` delegates directly to `update`; `update` immediately calls the
  generation-aware wheel `update_timer` before marking the typed state armed,
  and reset immediately calls `cancel_timer`
  (`crates/hammer-service/src/transport/tcp/timers.rs:136`).
- Duration conversion uses ceiling division, enforces a minimum of one tick,
  and bounds the result to `u64` (`crates/hammer-service/src/transport/tcp/timers.rs:232`).
- `advance` accepts an absolute `Instant`. `elapsed_ticks` computes completed
  resolution intervals from the retained absolute anchor and advances that
  anchor by only the consumed whole ticks, preserving sub-tick remainder
  (`crates/hammer-service/src/transport/tcp/timers.rs:242`). A zero-tick call
  does not advance the wheel.
- Independent inspection of `TimerWheel1t2w2048sl` confirms the assumptions
  used here: `update_timer` updates relative to the wheel's current tick and
  arms when no live timer exists, `cancel_timer` checks the slot generation,
  and `take_expired_timer` returns exactly `(slot, generation, timer_id)` while
  clearing the expired timer-id slot
  (`crates/hammer-infra/src/timer_wheel.rs:640`,
  `crates/hammer-infra/src/timer_wheel.rs:660`,
  `crates/hammer-infra/src/timer_wheel.rs:682`). A reused pool slot therefore
  cannot apply an old expiry to its new generation.

## Scope Boundaries

- `TcpTimers` has no `SessionWorker` dependency. Its only connection access is
  the TCP-owned connection pool supplied to exact expiry and pending-token
  handling (`crates/hammer-service/src/transport/tcp/timers.rs:177`).
- `TcpWorker` owns the new timer engine privately
  (`crates/hammer-service/src/transport/tcp/worker.rs:27`). The pre-existing
  SessionWorker raw-wheel synchronization remains in place
  (`crates/hammer-service/src/transport/tcp/worker.rs:137`), as the plan
  explicitly requires for the compile-safe Task 2 intermediate state.
- The commit changes only `connection.rs`, `mod.rs`, `timers.rs`, and
  `worker.rs`. It does not modify session runtime or the TCP policy modules
  assigned to Task 3, so Task 3 policy migration has not been pulled forward.

## Test Coverage

All five required Task 2 tests are present with the exact approved names
(`crates/hammer-service/src/transport/tcp/timers.rs:287`). Together they prove
exact-kind expiry, reset while pending, rearm while pending without an epoch,
generation reuse rejection, and update deadline replacement.

## Verification

- `cargo test -p hammer-service --lib transport::tcp::timers::tests -- --nocapture`:
  PASS, 5 passed, 0 failed, 145 filtered out.
- `cargo test -p hammer-infra --test timer_wheel -- --nocapture`: PASS, 10
  passed, 0 failed.
- `cargo fmt --all -- --check`: PASS.
- `git diff --check f54d375f..67a741c9`: PASS.

The focused builds emitted existing deprecation, unused-code, and generated
symbol naming warnings. No full crate or workspace suite was run for this
spec review.

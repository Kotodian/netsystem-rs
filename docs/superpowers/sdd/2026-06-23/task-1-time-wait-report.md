# Task 1 Report: TIME_WAIT arm + tuple retention

## Summary
Implemented TIME_WAIT entry handling so all close paths that reach `TcpState::TimeWait` arm `TcpConnectionTimerKind::TIME_WAIT`, and added an integration test that drives a FIN/ACK close path and verifies the tuple remains routable while the connection is in TIME_WAIT.

## Files Changed
- `crates/hammer-service/src/transport/tcp/connection.rs`
- `crates/hammer-service/src/transport/tcp/mod.rs`
- `crates/hammer-service/tests/tcp_time_wait.rs`

## Behavior Changes
- `FinWait1 -> TimeWait`, `FinWait2 -> TimeWait`, and `Closing -> TimeWait` now each arm the TIME_WAIT timer.
- `tcp_timer_ticks()` now returns a fixed tick count for `TcpConnectionTimerKind::TIME_WAIT`.
- Added a small test harness in `tcp::mod` so the integration test can drive the close path and inspect the tuple route without changing runtime ownership boundaries.
- Added `tcp_fin_path_enters_time_wait_and_retains_tuple_until_expiry` to confirm the connection reaches `TimeWait` and the tuple remains indexed.

## Verification
- `cargo test -p hammer-service --test tcp_time_wait tcp_fin_path_enters_time_wait_and_retains_tuple_until_expiry -- --exact`

## Notes
- The TIME_WAIT tick constant is intentionally local to this phase and only used to satisfy the arm/tick wiring required by the task.

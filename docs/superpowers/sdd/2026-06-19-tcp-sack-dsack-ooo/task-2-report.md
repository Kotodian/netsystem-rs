# Task 2 Report

## Scope

Implemented Task 2 from `docs/superpowers/sdd/2026-06-19-tcp-sack-dsack-ooo/task-2-brief.md` on top of HEAD `349ee24f2d4f48ef41b0a5838b8564974880ad0d`.

## TDD record

### RED

1. Added `transport::tcp::recovery::tests` inside `crates/hammer-service/src/transport/tcp/recovery.rs` so recovery behavior no longer depended on an external public sent-record test fixture.
2. Added `established_receive_ack_with_sack_only_cleans_cumulative_range_once` in `crates/hammer-service/src/transport/tcp/state_machine.rs`.
3. Ran:
   - `cargo test -p hammer-service transport::tcp::recovery::tests -- --nocapture`
   - `cargo test -p hammer-service established_receive_ack_with_sack_only_cleans_cumulative_range_once -- --nocapture`
4. Observed expected failures:
   - `receive_ack` still only accepted `(acknowledgment, advertised_window)`.
   - ACK+SACK handling still produced two recovery cleanup completions for one ACK event.

### GREEN

1. Removed public `TcpSentSegment` and replaced it with private `OutstandingSegment` in `recovery.rs`.
2. Changed `TcpRecoveryState::record_sent(...)` to a primitive-parameter entrypoint.
3. Removed the `TcpSentSegment` re-export from `crates/hammer-service/src/transport/tcp/mod.rs`.
4. Updated `commit_payload_tx()` to pass primitive sent facts directly into recovery.
5. Moved ACK/SACK event handling in established/close-wait receive paths to:
   - update recovery exactly once per ACK event,
   - use `on_sack_blocks()` when SACK blocks are present,
   - use `on_ack()` otherwise.
6. Refactored recovery ACK processing so one ACK event emits one `on_end_acks(...)` completion even when cumulative ACK and SACK block cleanup both happen in that event.
7. Deleted `crates/hammer-service/tests/tcp_rack_tlp.rs` after migrating the relevant coverage into `recovery.rs`.

### REFACTOR

1. Kept sent-record ownership private to `recovery.rs`.
2. Restricted `next_tlp_probe()` to test-only visibility with `#[cfg(test)]` instead of keeping a public probe hook alive.
3. Ran `cargo fmt --all`.

## Files changed

- `crates/hammer-service/src/transport/tcp/recovery.rs`
- `crates/hammer-service/src/transport/tcp/state_machine.rs`
- `crates/hammer-service/src/transport/tcp/session.rs`
- `crates/hammer-service/src/transport/tcp/mod.rs`
- deleted `crates/hammer-service/tests/tcp_rack_tlp.rs`

## Verification

Ran successfully:

```bash
cargo test -p hammer-service transport::tcp::recovery::tests -- --nocapture
cargo test -p hammer-service established_receive_ack_with_sack_only_cleans_cumulative_range_once -- --nocapture
rg -n "TcpSentSegment" crates/hammer-service/src crates/hammer-service/tests
```

Results:

- recovery self-tests passed.
- focused ACK+SACK state-machine test passed.
- `rg` returned no `TcpSentSegment` matches.

## Boundary check

- No new public sent-record type was introduced.
- The old public sent record was not renamed and re-exposed.
- Recovery-private helpers were not promoted onto the connection public surface.
- No timer-kind scanning was added.
- No helper/carrier/range/view-style public API was introduced for sent records.

## Notes / concerns

1. Focused verification is green, but the test commands still print pre-existing unrelated `dead_code` warnings in `crates/hammer-service/src/service.rs`.
2. `session.rs` needed the smallest possible callsite update because established and close-wait packet handlers are the active ACK ingress paths today.

## Follow-up fixes after controller review

### Additional RED

Added two focused regression tests after review feedback:

1. `transport::tcp::recovery::tests::on_ack_reports_per_acked_segment_bytes_in_flight`
   - locked the per-ACK-sample `bytes_in_flight` contract across multiple cumulatively acked segments in one ACK event.
   - initial failure showed the flattened accounting bug directly: observed `[0, 0]` where the expected sequence was `[1000, 0]`.
2. `transport::tcp::state_machine::tests::established_duplicate_ack_with_sack_does_not_update_rto_sample`
   - locked the requirement that pure duplicate ACK + SACK must not update retransmit-timeout sampling state.
   - initial failure showed `rtt_variance` changing from `Some(25ms)` to `Some(18.75ms)` under a non-advancing ACK.

### Additional GREEN

1. Fixed recovery ACK accounting so each `congestion.on_ack(..., bytes_in_flight)` call sees the flight value for that specific acked segment within the ACK event, instead of the uniform final post-cleanup remainder.
2. Gated `retransmit_timeout.observe_ack_sample(...)` on cumulative ACK progress only, so duplicate ACK + SACK events still inform recovery loss handling without mutating RTT/RTO sampling state.

### Additional verification

Ran successfully after the follow-up fixes:

```bash
cargo test -p hammer-service on_ack_reports_per_acked_segment_bytes_in_flight -- --nocapture
cargo test -p hammer-service established_duplicate_ack_with_sack_does_not_update_rto_sample -- --nocapture
```

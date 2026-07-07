# Task 1 Report: Replace normal Session TX with VPP batch commit end-to-end

## Status

DONE_WITH_CONCERNS

## Summary

Replaced the external Session Runtime normal-TX seam with VPP-shaped batch send facts plus transport-owned `push_header` commit, updated the focused fake-protocol dispatch test to assert one committed batch before graph visibility, and migrated TCP normal new-data TX to the new trait shape.

## Files Changed

- `crates/hammer-service/src/session/runtime.rs`
- `crates/hammer-service/src/transport/tcp/mod.rs`
- `crates/hammer-service/tests/session_queue_dispatch.rs`

## RED Evidence

Updated the focused integration test first, then ran:

```bash
cargo test -p hammer-service --test session_queue_dispatch session_tx_dispatch_commits_batch_before_graph_visibility
```

Observed expected RED failure against the old seam:

```text
error[E0432]: unresolved imports `hammer_service::session::runtime::TransportSendFlags`, `TransportSendParams`, `TxBatchBuffer`
error[E0407]: method `send_params` is not a member of trait `SessionQueueProtocol`
error[E0407]: method `push_header` is not a member of trait `SessionQueueProtocol`
error[E0407]: method `custom_tx` is not a member of trait `SessionQueueProtocol`
error[E0046]: not all trait items implemented, missing: `tx_offset`, `tx_payload_len`, `prepare_tx`, `cancel_tx`, `commit_tx`
```

## GREEN Evidence

Focused new-test check:

```bash
cargo test -p hammer-service --test session_queue_dispatch session_tx_dispatch_commits_batch_before_graph_visibility
```

```text
running 1 test
test session_tx_dispatch_commits_batch_before_graph_visibility ... ok
test result: ok. 1 passed; 0 failed
```

Required focused suite:

```bash
cargo test -p hammer-service --test session_queue_dispatch
```

```text
running 1 test
test session_tx_dispatch_commits_batch_before_graph_visibility ... ok
test result: ok. 1 passed; 0 failed
```

```bash
cargo test -p hammer-service session_tx_does_not_call_transport_when_app_has_no_pending_send
```

```text
running 1 test
test session::runtime::tests::session_tx_does_not_call_transport_when_app_has_no_pending_send ... ok
test result: ok. 1 passed; 0 failed
```

Guardrail check:

```bash
rg -n "prepare_tx|cancel_tx|commit_tx|tx_payload_len\\(|fn tx_offset" crates/hammer-service/src/session crates/hammer-service/tests/session_queue_dispatch.rs
```

```text
no matches
```

## Implementation Notes

- Added `TransportSendFlags`, `TransportSendParams`, and `TxBatchBuffer` to the Session Runtime seam.
- Replaced `tx_offset` / `tx_payload_len` / `prepare_tx` / `cancel_tx` / `commit_tx` with `send_params` / `push_header` / `custom_tx`.
- Rewrote Session Runtime normal TX to:
  - fetch transport send facts once,
  - prepare a local frame plus local batch vector,
  - copy session-owned FIFO bytes into up to `DEFAULT_TX_DISPATCH_BUDGET` buffers,
  - call `push_header` before `enqueue_frame`,
  - make graph visibility happen only after transport commit.
- Updated TCP normal new-data TX so `push_header` owns TCP header write plus payload-TX state commit for each prepared batch buffer.

## Self-Review

- Confirmed only task-owned source files were changed.
- Confirmed the old session-runtime seam names are gone from `crates/hammer-service/src/session` and the focused dispatch test.
- Ran `git diff --check` on the changed files; it returned clean.

## Concerns

- `TransportSendFlags` is introduced end-to-end, but the real TCP `send_params` path currently returns default flags. Focused tests for Task 1 pass, but pacing/zero-window deschedule/postpone behavior is not yet exercised by this slice.

## Commit

Planned commit message:

```text
hammer-service(Refactor): replace session tx with vpp batch seam
```

## Fix After Review

Status: DONE

Changed `crates/hammer-service/src/session/runtime.rs` so the Task 1 TX loop requeues any session with remaining pending send data without consulting `TransportSendFlags::DESCHED | POSTPONE`. This keeps the approved `TransportSendFlags` interface shape in `TransportSendParams`, but removes Task 1 runtime behavior dependence on flags that Task 3 owns.

Added a focused runtime regression test, `session_tx_requeues_remaining_data_even_with_transport_desched_flag`, to prove that pending data is requeued even when a transport sets `DESCHED` while reporting zero immediate send space.

Verification:

```bash
cargo test -p hammer-service --test session_queue_dispatch
```

```text
running 1 test
test session_tx_dispatch_commits_batch_before_graph_visibility ... ok
test result: ok. 1 passed; 0 failed
```

```bash
cargo test -p hammer-service session_tx_does_not_call_transport_when_app_has_no_pending_send
```

```text
running 1 test
test session::runtime::tests::session_tx_does_not_call_transport_when_app_has_no_pending_send ... ok
test result: ok. 1 passed; 0 failed
```

```bash
rg -n "prepare_tx|cancel_tx|commit_tx|tx_payload_len\\(|fn tx_offset" crates/hammer-service/src/session crates/hammer-service/tests/session_queue_dispatch.rs
```

```text
no matches
```

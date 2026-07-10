# Task 4 Preflight: Legacy Timer Deletion Map

## Snapshot And Scope

Inspected `HEAD` (`0b26a9a7`, `hammer-service(Refactor): move TCP timer policy into transport`), the current worktree diff, `CONTEXT.md`, ADR 0008, and the Task 4 brief. The uncommitted `connection.rs` changes are Task 3 follow-up work and are only accounted for when naming current symbols; this preflight does not assess them.

## Session Production Deletion Map

`crates/hammer-service/src/session/protocol.rs` is already clean. It only re-exports the typed session/transport seam and has no timer legacy to delete.

Delete from `crates/hammer-service/src/session/runtime.rs`:

- Import: `hammer_infra::timer_wheel::TimerWheel1t2w2048sl`.
- Tick-only import member: `Duration` from `std::time::{Duration, Instant}`; retain `Instant`.
- Constants: `DEFAULT_SESSION_TIMER_TICK` and `SESSION_TIMER_KIND_COUNT`.
- Record: `ExpiredTimer { session_id, timer_id }`.
- `SessionQueueStep::expired_timers`; no caller reads it.
- `SessionWorker` fields: `timers`, `expired_timers`, `pending_timers`, `timer_tick_duration`, and `last_timer_tick`, plus their constructor initialization.
- Both `timers_mut` pass-throughs (`SessionWorker::timers_mut` and `SessionDriverRuntime::timers_mut`). Their only remaining consumer is a legacy unit-test helper in `connection.rs`.
- `SessionWorker::elapsed_timer_ticks` and `SessionWorker::expire_legacy_timers`.
- Test-only `dispatch_session_queue_for_ticks`; replace test calls with a tickless helper that samples/accepts `Instant` and calls the normal pending dispatcher.
- The `timer_ticks: u32` argument and `let _ = timer_ticks` in `dispatch_session_queue_pending`.
- In `dispatch_registered_session_queue_once_at`, delete the `elapsed_timer_ticks(now)` call and invoke the pending dispatcher with `now` only.

`FifoQueue` and `PoolIndex` imports remain required by control events and the default transport-index type respectively.

Production-path status: `expire_legacy_timers`, both `timers_mut` methods, and `dispatch_session_queue_for_ticks` have no non-test caller. `elapsed_timer_ticks` does still run on the production SessionQueueNode path, but its result is passed to an ignored parameter. It is behaviorally inert, not unreachable.

## TCP Production Deletion Map

### Direct legacy definitions and exports

Delete from `crates/hammer-service/src/transport/tcp/connection.rs`:

- Raw ids: `TCP_TIMER_RETRANSMIT`, `TCP_TIMER_RACK`, `TCP_TIMER_TLP`, `TCP_TIMER_DELAYED_ACK`, `TCP_TIMER_PERSIST`, `TCP_TIMER_KEEP_ALIVE`, `TCP_TIMER_TIME_WAIT`, and `TCP_TIMER_PACING`.
- Raw count/mask machinery: `TCP_TIMER_COUNT`, `timer_bit`, and the `u16` timer-mask return values in legacy connection methods.
- Tick representation left outside `TcpTimers`: `TCP_DELAYED_ACK_TICKS`, `TCP_TIME_WAIT_TICKS`, and `timer_ticks`. Keep the `Duration` constants `TCP_DELAYED_ACK_INTERVAL` and `TCP_TIME_WAIT_INTERVAL`.
- Session-wheel reconciliation: `sync_tcp_timer` and `sync_all_tcp_timers`.
- Raw-id helpers: `timer_is_supported`, `timer_is_active(u32)`, `active_timer_mask`, `timer_set(u32)`, `timer_reset(u32)`, `on_tcp_timer_expiry(u32, ...)`, and `legacy_timer_dispatch_pending`.
- Legacy-only policy entrypoints once their typed callers are made self-contained: `receive_open_reply`, `receive_final_ack`, `receive_ack`, and `receive_established`.

After removing `sync_tcp_timer`, remove the now-unused `super::TcpNodeError` import from `connection.rs`.

Delete from `crates/hammer-service/src/transport/tcp/mod.rs`:

- `pub(crate) use connection::sync_all_tcp_timers`.
- The raw `TCP_TIMER_*`/`TCP_TIMER_COUNT` members from the public `connection` re-export list.

`established.rs` and `rcv_process.rs` are already free of raw timer imports and all-kind reconciliation. `type TcpQueue`, `trait SessionQueueProtocol`, and optional `fn custom_tx` do not exist in current production source.

### Production-reachable conversions required before deletion

The external Session-wheel bridge is dead in production, but not every raw helper is. Convert these paths before deleting the raw definitions:

- `dispatch_registered_session_queue_once_at -> elapsed_timer_ticks`: remove the inert Session tick calculation.
- `TcpWorker::tx_action -> TcpConnection::commit_payload_tx`: the returned raw `u16` mask is ignored, but `commit_payload_tx` still sets/resets raw ids before `sync_payload_tx_timers`. Make the commit state-only/typed and return `CoreResult<()>`.
- `receive_open_reply_with_timers -> receive_open_reply` and `receive_final_ack_with_timers -> receive_final_ack`: fold the state transition into each typed method so raw reset/activity calls disappear.
- `on_tcp_ready_with_timers -> on_tcp_ready`: fold the output/state operation into the typed method; replace raw `timer_is_active` with `TcpTimerState::is_active(TcpTimerKind::Retransmit)`.
- `TcpWorker::disconnect -> on_session_close`: pass `PoolIndex`/`&mut TcpTimers` or otherwise perform the typed KeepAlive/Pacing resets at the owning boundary.
- `on_clean_in_order_payload`: replace `timer_is_active(TCP_TIMER_DELAYED_ACK)` with the private typed state query.
- `on_typed_timer_expiry`: replace the raw retransmit support/active checks and remove raw rearming from the shared retransmit, RACK, TLP, Persist, KeepAlive, and Pacing handlers. `TcpTimers` must remain the only wheel/state synchronizer.
- `record_activity`: it is reached by `commit_payload_tx`; reduce it to non-timer activity state or use `observe_activity(index, timers, now)` at the typed boundary.

Verified no non-test production caller remains for `sync_tcp_timer`, `sync_all_tcp_timers`, raw `on_tcp_timer_expiry`, `timer_ticks`, Session `timers_mut`, Session `expire_legacy_timers`, or `dispatch_session_queue_for_ticks`. The production-reachable conversions above are the exceptions to any broader claim that all legacy-shaped code is already dead.

## Test Disposition

Delete as legacy-representation guardrails:

- `tests/tcp_state_machine.rs::established_node_delegates_timer_refresh_to_shared_helper`.
- `tests/tcp_state_machine.rs::timer_refresh_loops_consolidated_into_shared_helper`.
- `tests/tcp_state_machine.rs::tcp_timer_dispatch_is_owned_by_connection`; exact-token behavior is already covered through `TcpWorker`.
- `connection.rs::time_wait_ticks_is_60s`; the 60-second behavior is covered at the typed timer seam.

Migrate rather than delete:

- Same-turn ordering/close: `session/node/tests.rs::session_queue_updates_all_static_transports_before_control_and_io` and `transport_deleted_then_queued_app_close_releases_the_session_slot`. Preserve the one sampled `Instant` and time-update -> control -> I/O ordering while replacing tick-based helpers.
- TX ordering/atomicity: `session_tx_dispatch_commits_batch_before_graph_visibility`, `failed_session_packetized_tx_action_keeps_fifo_and_graph_unchanged`, and TCP's `tcp_packetized_tx_rolls_back_transport_state_when_batch_fails`. Replace tick/raw timer assertions only.
- Delayed ACK: `tcp_timer_dispatch_uses_exact_timer_token`, `session_tcp_delayed_ack_timer_emits_ack_after_first_clean_payload`, and `connection.rs::tcp_delayed_ack_expiry_emits_one_ack_without_moving_keepalive_deadline`. Query `TcpTimerState` with `TcpTimerKind` or observe emitted output; do not retain raw ids.
- TIME_WAIT: `tcp_time_wait_expiry_closes_session`, `tcp_time_wait_duplicate_fin_rearms_only_time_wait`, `tcp_fin_path_enters_time_wait_and_retains_session_until_expiry`, and `tcp_time_wait_duplicate_fin_reacks_and_rearms_timer`. Advance by `Duration::from_secs(60)` through `TcpWorker`; remove the Session-wheel `drive_fin_ack_to_time_wait` reconciliation.
- SYN/keepalive/pacing/persist behavior currently using raw expiry helpers: `tcp_syn_sent_timer_expiry_updates_connection_state_in_connection`, `tcp_syn_data_retransmit_preserves_payload_len_and_cookie`, `tcp_established_supports_pacing_and_keepalive_timers`, `tcp_pacing_timer_expiry_rearms_and_requests_tx_dispatch`, `tcp_keepalive_timer_expiry_probes_then_closes_idle_connection`, and `persist_timer_backoff_doubles_interval_each_attempt`. Migrate to typed timers or retain only if not duplicated by the Task 3 typed deadline tests.
- Rename `tcp_custom_tx_handles_special_output_without_normal_packetization` to describe control TX; its behavior is valid, but `custom_tx` is obsolete vocabulary.

All current `session/node/tests.rs` calls to `dispatch_session_queue_for_ticks` are behavior tests and should switch to a tickless test dispatcher, not be removed wholesale.

## Exact Architecture Guardrails

Create `crates/hammer-service/tests/transport_worker_boundary.rs` and scan production source text only.

Session source set:

```text
src/session/protocol.rs
src/session/runtime.rs
```

Exact forbidden strings:

```text
TimerWheel
timer_wheel
ExpiredTimer
pending_timers
handle_expired_timer
handle_legacy_timer
poll_once_for_ticks
TcpConnection
TcpTimer
```

TCP source set:

```text
src/transport/tcp/mod.rs
src/transport/tcp/connection.rs
src/transport/tcp/established.rs
src/transport/tcp/rcv_process.rs
```

Exact forbidden strings:

```text
type TcpQueue
SessionQueueProtocol
sync_all_tcp_timers
sync_tcp_timer
TCP_TIMER_COUNT
pub const TCP_TIMER_
active_timer_mask
fn custom_tx
```

Do not weaken these strings to accommodate tests embedded in production modules; migrate/rename those tests instead.

## Documentation Alignment

No vocabulary correction is currently required:

- `CONTEXT.md` already defines `Session Runtime`, `Transport Timer Policy`, `Transport Worker State`, and `Timer Token` with the required ownership boundary and exact-token semantics.
- ADR 0008 already uses `SessionQueueNode`, `SessionTransport<Index>`, `SessionWorker<Index, Seg>`, `TcpWorker`, private `TcpTimerKind`, `TcpTimers`, and `TcpTimerToken`, explicitly rejects raw ids/counts/masks and `TcpQueue`, and assigns tick conversion to each transport.
- The design spec already says to remove the Session timer wheel/ticks/raw delivery, delete full-timer reconciliation and raw ids/count/masks, and preserve exact-token behavior.

Only update these documents if final code identifiers differ from that approved vocabulary; no such mismatch is present in the inspected snapshot.

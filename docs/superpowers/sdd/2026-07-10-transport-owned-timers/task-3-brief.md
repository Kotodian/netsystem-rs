### Task 3: Migrate All TCP Timer Policy Into TcpWorker

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/{timers,worker,connection,mod,established,rcv_process,syn_sent,listen}.rs`
- Modify: `crates/hammer-service/src/session/{protocol,runtime}.rs`
- Test: existing TCP unit and integration suites

- [ ] **Step 1: Add failing deadline and per-kind behavior tests**

Add behavior tests proving:

```rust
tcp_delayed_ack_expiry_emits_one_ack_without_moving_keepalive_deadline
tcp_unrelated_ack_does_not_move_retransmit_deadline
tcp_keepalive_activity_updates_only_keepalive_deadline
tcp_persist_window_reopen_cancels_pending_probe
tcp_time_wait_duplicate_fin_rearms_only_time_wait
tcp_rack_and_tlp_expiry_schedule_exact_recovery_work
tcp_pacing_expiry_schedules_only_when_pacing_is_active
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
cargo test -p hammer-service --lib transport::tcp::tests::tcp_delayed_ack_expiry_emits_one_ack_without_moving_keepalive_deadline -- --exact
cargo test -p hammer-service --lib transport::tcp::tests::tcp_unrelated_ack_does_not_move_retransmit_deadline -- --exact
```

Expected: FAIL because `sync_all_tcp_timers` restarts unrelated active relative timers.

- [ ] **Step 3: Pass TcpTimers directly through connection policy operations**

Split `TcpWorker` fields before borrowing:

```rust
let TcpWorker { connections, timers, .. } = self;
let connection = connections.get_mut(index).ok_or(TcpNodeError::SessionMissing)?;
connection.receive_ack(index, timers, packet, now)?;
```

Connection methods that decide timers accept `PoolIndex`, `&mut TcpTimers`, and `Instant`, compute the one affected duration, and immediately call typed set/update/reset. Do not return timer masks, scan kinds, use raw pointers, or introduce a timer action/effect carrier.

- [ ] **Step 4: Migrate timer policy in risk order**

Migrate each group completely before the next:

1. DelayedAck, TimeWait, KeepAlive.
2. Persist, Pacing.
3. Retransmit, Rack, Tlp.

Intervals remain behavior-compatible: delayed ACK 10 ms; TIME_WAIT 60 s; keepalive idle/probe policy; persist capped exponential RTO; pacing controller delay; retransmit current RTO; RACK/TLP remaining recovery deadlines. Use `Duration` at the connection/worker boundary and ticks only inside `TcpTimers`.

- [ ] **Step 5: Dispatch exact tokens inside TcpWorker**

`SessionQueueNode` calls transport-set `update_time_all` before app/control/I/O. TCP advances its private clock, drains exact tokens up to its private budget, validates connection generation and pending state, clears pending, skips if rearmed, and matches only `token.kind`. Handlers may emit existing `TcpSegment`, schedule the owning SessionId, perform ACK cleanup, or issue transport-neutral lifecycle notifications.

Session Runtime never receives a kind, token, tick, or wheel reference.

- [ ] **Step 6: Run TCP behavior suites**

Run:

```bash
cargo test -p hammer-service --lib transport::tcp::
cargo test -p hammer-service --test tcp_state_machine --test tcp_output --test tcp_session_app_boundary --test session_queue_dispatch
cargo test -p hammer-service
```

Expected: PASS, including all eight timer kinds, pending reset/rearm, generation reuse, and deadline non-refresh regressions.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/session crates/hammer-service/src/transport/tcp crates/hammer-service/tests
git commit -m "hammer-service(Refactor): move TCP timer policy into transport"
```


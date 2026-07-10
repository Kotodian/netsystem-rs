### Task 4: Delete Legacy Timer Surfaces And Lock The Boundary

**Files:**
- Create: `crates/hammer-service/tests/transport_worker_boundary.rs`
- Modify: `crates/hammer-service/src/session/{protocol,runtime}.rs`
- Modify: `crates/hammer-service/src/transport/tcp/{mod,connection,established,rcv_process}.rs`
- Modify: `crates/hammer-service/tests/{tcp_state_machine,vpp_session_tx_guardrails}.rs`
- Modify: `CONTEXT.md`
- Modify: `docs/adr/0008-transport-workers-own-transport-state-and-timers.md`
- Modify: `docs/superpowers/specs/2026-07-10-transport-owned-timers-design.md`

- [ ] **Step 1: Write failing architecture guardrails**

Add tests using production source text only:

```rust
#[test]
fn session_modules_do_not_own_or_dispatch_transport_timers() {
    let source = [
        include_str!("../src/session/protocol.rs"),
        include_str!("../src/session/runtime.rs"),
    ]
    .join("\n");
    for forbidden in [
        "TimerWheel", "timer_wheel", "ExpiredTimer", "pending_timers",
        "handle_expired_timer", "handle_legacy_timer", "poll_once_for_ticks",
        "TcpConnection", "TcpTimer",
    ] {
        assert!(!source.contains(forbidden), "session owns forbidden {forbidden}");
    }
}

#[test]
fn legacy_tcp_queue_and_timer_reconciliation_surfaces_are_removed() {
    let source = [
        include_str!("../src/transport/tcp/mod.rs"),
        include_str!("../src/transport/tcp/connection.rs"),
        include_str!("../src/transport/tcp/established.rs"),
        include_str!("../src/transport/tcp/rcv_process.rs"),
    ]
    .join("\n");
    for forbidden in [
        "type TcpQueue", "SessionQueueProtocol", "sync_all_tcp_timers",
        "sync_tcp_timer", "TCP_TIMER_COUNT", "pub const TCP_TIMER_",
        "active_timer_mask", "fn custom_tx",
    ] {
        assert!(!source.contains(forbidden), "TCP retains forbidden {forbidden}");
    }
}
```

Forbid production session references to `TimerWheel`, `timer_wheel`, `ExpiredTimer`, `pending_timers`, `handle_expired_timer`, `poll_once_for_ticks`, TCP timer kinds/counts/masks, and `TcpConnection`. Forbid legacy `TcpQueue`, `SessionQueueProtocol`, `sync_all_tcp_timers`, `sync_tcp_timer`, `TCP_TIMER_COUNT`, public raw `TCP_TIMER_*`, active masks, and optional `custom_tx`.

- [ ] **Step 2: Run guardrails and verify RED**

Run:

```bash
cargo test -p hammer-service --test transport_worker_boundary -- --nocapture
```

Expected: FAIL and name the remaining legacy surfaces.

- [ ] **Step 3: Delete obsolete implementation and tests**

Remove Session timer wheel/clock/tick conversion/expired queues, raw timer control context, tick-based polling, the per-connection `SessionQueueProtocol`, raw timer ids/count/masks, `sync_tcp_timer`, `sync_all_tcp_timers`, `TcpQueue`, and tests that specifically required all-kind refresh helpers. Preserve and migrate behavior assertions for same-turn close, TX ordering, delayed ACK, and TIME_WAIT.

- [ ] **Step 4: Verify documentation and source consistency**

Run:

```bash
rg -n "poll_once_for_ticks|timer_wheel|ExpiredTimer|pending_timers|handle_legacy_timer" crates/hammer-service/src/session
rg -n "TcpQueue|SessionQueueProtocol|sync_all_tcp_timers|sync_tcp_timer|TCP_TIMER_COUNT|pub const TCP_TIMER_" crates/hammer-service/src/transport/tcp
git diff --check
```

Expected: both `rg` commands return no legacy production matches; `git diff --check` exits 0. Update docs only for final identifiers that differ from the approved vocabulary; do not weaken the ownership contract.

- [ ] **Step 5: Run guardrails and focused regressions**

Run:

```bash
cargo test -p hammer-service --test transport_worker_boundary
cargo test -p hammer-service --test tcp_state_machine --test vpp_session_tx_guardrails
cargo test -p hammer-service
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add CONTEXT.md docs/adr docs/superpowers crates/hammer-service
git commit -m "hammer-service(Test): lock transport-owned timer boundary"
```


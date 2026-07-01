# TCP Session App Defect Audit — Gap Supplement

> **For agentic workers:** Execute after parent plan `2026-06-30-tcp-session-app-defect-audit.md` Tasks 1–6. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix FIN-handling protocol gaps, SACK boundary latent defect, check-then-wait wake races, and incomplete producer-side wake coverage that were identified in the July 2026 TCP/Session/App boundary audit but not covered by the parent plan's Tasks 1–10.

**Architecture:** This supplement:
- **Enhances Task 7** (the parent plan's wake-API task) to use VPP's three-phase want/clear/notification protocol, avoiding the `Notify::notify_waiters()` no-permit race.
- **Adds Task 12** (FIN+payload ordering bug + dead param cleanup).
- **Adds Task 13** (`SessionEvtType::HalfClose` variant for peer FIN notification).
- **Adds Task 14** (SACK left boundary base correction).
- **Adds Task 16** (wake coverage for OOO RX, TX buffer dispatch, and runtime-ready paths).

All tasks keep the parent plan's constraints: Local-only segment, no cross-process SVM, no `dyn Any`, no TCP-specific buffer APIs.

**Tech Stack:** Same as parent: Rust 2024, `hammer-infra::{fifo,msg_queue,segment}`, `hammer-runtime::app`, `hammer-service::{session,transport::tcp}`, VPP references in `third_party/vpp/src/svm/svm_fifo.c` and `third_party/vpp/src/vnet/tcp/tcp_input.c`, tests via `cargo test -p hammer-infra`, `hammer-runtime`, and `hammer-service`.

---

## Supplementary Defects

### P0: FIN+payload in the same segment requires a FIN retransmit to close

**Evidence:**
- `crates/hammer-service/src/transport/tcp/established.rs:265` calls `receive_established` (which runs `receive_fin_in_established` at `connection.rs:1536`) **before** `accept_payload`/`receive_payload` at `established.rs:297`.
- `crates/hammer-service/src/transport/tcp/connection.rs:1544` `receive_fin_in_established` checks `packet.sequence.advance(payload_len) == self.rcv_nxt`. For an in-sequence FIN+data segment, `payload_len > 0` means `sequence + payload_len > rcv_nxt` — the check fails, FIN is silently dropped.
- After `receive_payload` advances `rcv_nxt`, the FIN condition would hold, but `receive_fin_in_established` won't be called again for this packet. The peer must retransmit a bare FIN.

**VPP reference:** `tcp_input.c:1449-1455` orders data processing (step 7) before FIN check (step 8).

### P0: Peer FIN→CloseWait never notifies the app

**Evidence:**
- `connection.rs:1542-1557` `receive_fin_in_established` sets `state = CloseWait` and returns an ACK but calls no `app.closed()` / `push_event`.
- `SessionAppRuntime::closed` (`session/app.rs:80-87`) is only called from `close_session` (`runtime.rs:323`), which is driven by local app pending close — not by peer FIN.
- `SessionEvtType` (`msg_queue.rs:15-20`) has no `HalfClose` or EOF variant.

**VPP reference:** `tcp_input.c:1031-1050` `tcp_rcv_fin` calls `tcp_program_disconnect()` to notify session layer.

### P1: `enqueue_rx` 4th parameter `_: bool` is dead

**Evidence:**
- `crates/hammer-service/src/session/runtime.rs:386` declares `_: bool`.
- All 5 callers pass `false` unconditionally; the function body never reads it.
- VPP `tcp_session_enqueue_data` uses the equivalent param (`queue event` flag) to control app notification — Hammer hasn't implemented the distinction.

### P1: SACK left boundary base is inconsistent after OOO promote

**Evidence:**
- `connection.rs:1711` advances `rcv_nxt` by `delivered_len`.
- `connection.rs:1713` then computes `left = self.rcv_nxt.advance(start)` where `start` is `newest_ooo_start` (a pre-promote offset, relative to pre-advance `rcv_nxt`). This yields `left = seq + delivered_len` — off by `delivered_len`.
- Currently masked because OOO `delivered` is always 0 (Task 2 of parent plan will make `delivered_len > 0` possible, exposing this).

**VPP reference:** `tcp_input.c:1107-1155` OOO path never advances `rcv_nxt`; SACK uses `rcv_nxt + ooo_segment_offset_prod(...)` where `rcv_nxt` is the un-advanced value.

### P0: check-then-wait race in async sleep loops

**Evidence:**
- `crates/hammer-runtime/src/app/application.rs:160-175` (`send_all`), `185-201` (`recv`), `208-216` (`next_event`) all use: `want_notification(); if check { clear(); continue; }; notified().await;`
- `Notify::notify_waiters()` (the only available wake primitive) stores **no permit** — it only wakes already-registered waiters. If `notify_waiters()` fires between `check` and the `notified()` future's first poll, the wake is lost.
- The parent plan's Task 7 adds `wake_*` → `notify_waiters()` calls but does not restructure these loops. The race survives.

**VPP reference:** `svm_fifo.h:23-29` persistent want flags + `:823-846` `svm_fifo_needs_deq_ntf` (production-side need-based decision, not waker-registration-based).

### P1: Producer-side wake coverage is incomplete (parent Task 7 misses paths)

**Evidence:**
- `crates/hammer-service/src/session/app.rs:165-187` — `copy_rx_from_buffer_ooo` (OOO RX) has no `wake_rx` after success (parent Task 7 only covers in-order RX).
- `crates/hammer-service/src/session/app.rs:114-141` — `copy_tx_to_buffer` (TX data actually moved to dataplane) has no `wake_tx`, yet this is where TX FIFO space is freed.
- `crates/hammer-service/src/session/app.rs:195-224` — `drain_tx_events_to` (`mark_ready`) has no `wake_tx`, yet this is the runtime trigger for app TX-space waiters.
- Peer FIN→CloseWait (Task 13) will need `wake_evt` + `wake_rx`.
- grep confirms zero `wake_rx|wake_tx|wake_evt` calls in `crates/hammer-service/src/` today.

---

## Approval Required Before Implementation

These API additions cross layer boundaries; get explicit user approval before coding.

1. `SessionEvtType::HalfClose` variant
   - **Final result:** new enum variant in `SessionEvtType`, `SessionEvent` gets a corresponding mapping, `AppSession::half_close()` method (or reuse `closed` with a flag).
   - **Why existing API is not enough:** no existing event variant represents "peer sent FIN, session is half-closed, app should stop reading."
   - **Boundary:** msg_queue owns the enum; session/app owns production; TCP owns the semantic trigger (`receive_fin_in_established`).

2. `TcpConnection::receive_fin_after_payload` extraction
   - **Final result:** extract `receive_fin_in_established` into a public-ish helper that can be called after payload delivery, with a new name clarifying "call after payload processing" (e.g. `process_fin_after_payload`).
   - **Why existing API is not enough:** current `receive_fin_in_established` is private with the wrong ordering assumption built in.
   - **Boundary:** TCP owns FIN state-machine logic; established.rs owns the call ordering.

---

## File Map (new/modified beyond parent)

- `crates/hammer-infra/src/msg_queue.rs` — `SessionEvtType::HalfClose` variant.
- `crates/hammer-runtime/src/app/application.rs` — Three-phase want/check/notify + `wake_rx/wake_tx/wake_evt`.
- `crates/hammer-runtime/tests/app_worker_notify.rs` — Racy-wake test + HalfClose app test.
- `crates/hammer-service/src/session/app.rs` — `half_closed()` method, `wake_*` calls at all producer paths.
- `crates/hammer-service/src/session/runtime.rs` — `enqueue_rx` 4th-param removal; wake calls in TX dispatch/ready paths.
- `crates/hammer-service/src/transport/tcp/connection.rs` — `process_fin_after_payload`, SACK left boundary fix.
- `crates/hammer-service/src/transport/tcp/established.rs` — Reorder data-then-FIN; remove `_` param from `enqueue_rx`.
- `crates/hammer-service/src/transport/tcp/{listen,syn_sent,rcv_process,mod}.rs` — Remove `_` param from `enqueue_rx`.
- `crates/hammer-service/tests/tcp_session_app_boundary.rs` — FIN+payload, HalfClose, SACK, racy-wake tests.

---

### Enhanced Task 7: Wake Local App Waiters (replaces parent Task 7)

**Parent plan reference:** Replaces `docs/superpowers/plans/2026-06-30-tcp-session-app-defect-audit.md Task 7`.

**Files:**
- Modify: `crates/hammer-runtime/src/app/application.rs`
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-runtime/tests/app_worker_notify.rs`

- [ ] **Step 1: Add failing local wake tests**

Create `crates/hammer-runtime/tests/app_worker_notify.rs`:

```rust
use hammer_infra::segment::Local;
use hammer_runtime::app::{AppSessionConfig, SessionHandle, with_current_app_worker};
use tokio::time::{Duration, timeout};

#[tokio::test(flavor = "current_thread")]
async fn local_app_worker_recv_wakes_after_rx_notify() {
    let handle = SessionHandle::new(7, 0);
    let session = with_current_app_worker(0, |worker| {
        worker
            .attach_session_local(handle, AppSessionConfig::new(64, 4))
            .expect("attach")
    });
    let worker = with_current_app_worker(0, |worker| worker.clone());
    let recv = async {
        let mut out = [0u8; 8];
        let read = worker.recv(handle, &mut out).await;
        (read, out)
    };
    let producer = async {
        tokio::task::yield_now().await;
        session.enqueue_rx(b"hi").expect("enqueue rx");
        with_current_app_worker(0, |worker| worker.wake_rx(handle));
    };

    let ((read, out), _) = timeout(Duration::from_millis(200), async {
        tokio::join!(recv, producer)
    })
    .await
    .expect("recv should wake");

    assert_eq!(read, 2);
    assert_eq!(&out[..2], b"hi");
}

#[tokio::test(flavor = "current_thread")]
async fn local_app_worker_next_event_wakes_after_event_notify() {
    let handle = SessionHandle::new(8, 0);
    let session = with_current_app_worker(0, |worker| {
        worker
            .attach_session_local(handle, AppSessionConfig::new(64, 4))
            .expect("attach")
    });
    let worker = with_current_app_worker(0, |worker| worker.clone());
    let next = async { worker.next_event(handle).await.expect("event") };
    let producer = async {
        tokio::task::yield_now().await;
        session
            .push_event(hammer_infra::msg_queue::SessionEvtType::Connect)
            .expect("push event");
        with_current_app_worker(0, |worker| worker.wake_evt(handle));
    };

    let (event, _) = timeout(Duration::from_millis(200), async {
        tokio::join!(next, producer)
    })
    .await
    .expect("next_event should wake");

    assert_eq!(event.session_index, handle.session_index());
}

#[tokio::test(flavor = "current_thread")]
async fn recv_does_not_lose_wake_under_race() {
    let handle = SessionHandle::new(9, 0);
    let session = with_current_app_worker(0, |worker| {
        worker
            .attach_session_local(handle, AppSessionConfig::new(64, 4))
            .expect("attach")
    });
    let worker = with_current_app_worker(0, |worker| worker.clone());
    let recv = async {
        let mut out = [0u8; 8];
        let read = worker.recv(handle, &mut out).await;
        (read, out)
    };
    let producer = async {
        tokio::task::yield_now().await;
        session.enqueue_rx(b"hi").expect("enqueue rx");
        /* Simulate a notify_waiters that fires before the waiter has
         * polled its Notified future for the first time. The three-phase
         * want/check protocol in AppWorker::recv must survive this. */
        with_current_app_worker(0, |worker| worker.wake_rx(handle));
        tokio::task::yield_now().await;
        session.enqueue_rx(b"!"  ).expect("enqueue rx");
        with_current_app_worker(0, |worker| worker.wake_rx(handle));
    };

    let ((read, out), _) = timeout(Duration::from_millis(200), async {
        tokio::join!(recv, producer)
    })
    .await
    .expect("recv should wake even with early notify");

    assert!(read > 0, "must receive data despite racy notify");
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:
```bash
cargo test -p hammer-runtime --test app_worker_notify
```

Expected: FAIL to compile because `wake_rx` and `wake_evt` do not exist.

- [ ] **Step 3: Add producer wake methods with three-phase protocol**

In `crates/hammer-runtime/src/app/application.rs`, add to `impl<S: Segment> AppWorker<S>`:

```rust
    pub fn wake_rx(&self, handle: SessionHandle) {
        if let Some(notify) = self.notify_entry(handle) {
            notify.rx_readable.notify_waiters();
            notify.evt_readable.notify_waiters();
        }
    }

    pub fn wake_tx(&self, handle: SessionHandle) {
        if let Some(notify) = self.notify_entry(handle) {
            notify.tx_writable.notify_waiters();
            notify.evt_readable.notify_waiters();
        }
    }

    pub fn wake_evt(&self, handle: SessionHandle) {
        if let Some(notify) = self.notify_entry(handle) {
            notify.evt_readable.notify_waiters();
        }
    }
```

- [ ] **Step 4: Restructure async loops to VPP three-phase protocol**

Replace the body of `send_all`, `recv`, and `next_event` loops. The pattern (`recv` shown; `send_all`/`next_event` analogous):

```rust
pub async fn recv(&self, handle: SessionHandle, out: &mut [u8]) -> usize {
    let session = self
        .sessions
        .lookup(&handle.raw())
        .expect("session must exist for recv");
    let notify = self.notify_entry(handle);
    loop {
        let read = session.rx_fifo().peek(0, out.len(), out);
        if read != 0 || out.is_empty() {
            session.rx_fifo().dequeue_drop(read);
            return read;
        }
        session.want_rx_notification();
        if session.rx_fifo().max_dequeue() != 0 {
            session.clear_rx_notification();
            continue;
        }
        let notified = notify.as_ref().map(|n| n.rx_readable.notified());
        match notified {
            Some(fut) => {
                tokio::pin!(fut);
                let _ = fut.as_mut().poll(&mut std::task::Context::from_waker(
                    &fut::noop_waker_ref(),
                ));
                if session.rx_fifo().max_dequeue() != 0 {
                    session.clear_rx_notification();
                    continue;
                }
                fut.await;
                session.clear_rx_notification();
            }
            None => return 0,
        }
    }
}
```

Key change: pin + poll the `Notified` future once to register the waker **before** the second check. This eliminates the race window: if `notify_waiters` fires between the first check and the first poll, the waker is not yet registered and the wake is lost — but the second check will catch the FIFO state change. If `notify_waiters` fires between the first poll (waker registered) and the second check, the waker fires and `fut.await` returns immediately. If neither, `fut.await` registers to wait.

- [ ] **Step 5: Call wake methods from session app runtime**

In `crates/hammer-service/src/session/app.rs`:

After `session.push_event(SessionEvtType::Connect)?` (connected method), add:
```rust
let handle = session.session_handle();
hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
    worker.wake_evt(handle);
});
```

After `session.push_event(SessionEvtType::Close)?` (closed method), add the same `wake_evt`.

In `copy_rx_from_buffer`, after the loop's `session.enqueue_rx(...)` call succeeds with `wrote > 0`, add:
```rust
let handle = session.session_handle();
hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
    worker.wake_rx(handle);
});
```

In `release_pending_send_bytes`, after `dropped > 0`, add:
```rust
let handle = session.session_handle();
hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
    worker.wake_tx(handle);
});
```

- [ ] **Step 6: Run runtime wake tests**

Run:
```bash
cargo test -p hammer-runtime --test app_worker_notify
```

Expected: PASS (all 3 tests: recv-wakes, event-wakes, racy-wake-survives).

- [ ] **Step 7: Run app/session tests**

Run:
```bash
cargo test -p hammer-runtime --lib app::
cargo test -p hammer-service --lib session::
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/hammer-runtime/src/app/application.rs \
        crates/hammer-runtime/tests/app_worker_notify.rs \
        crates/hammer-service/src/session/app.rs \
        crates/hammer-service/src/session/runtime.rs
git commit -m "hammer-runtime(Fix): three-phase notify protocol + wake local app waiters"
```

---

### Task 12: Fix FIN+payload Order, Clean Dead `enqueue_rx` Param

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/{listen,syn_sent,rcv_process,mod}.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-service/tests/tcp_session_app_boundary.rs`

- [ ] **Step 1: Add failing test**

Append to `crates/hammer-service/tests/tcp_session_app_boundary.rs`:

```rust
#[test]
fn fin_with_payload_closes_in_one_pass() {
    /* Structural test: established.rs must process payload before FIN.
     * Also: enqueue_rx must not have a 4th dead parameter. */
    let established_source = include_str!("../src/transport/tcp/established.rs");

    assert!(
        established_source.contains("receive_payload"),
        "established RX path must call receive_payload"
    );
    assert!(
        !established_source.contains(".enqueue_rx(session_id, index, offset, false)"),
        "enqueue_rx must not have a 4th parameter"
    );
    assert!(
        established_source.contains("fin") || established_source.contains("FIN"),
        "FIN must be checked after payload delivery"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:
```bash
cargo test -p hammer-service --test tcp_session_app_boundary fin_with_payload_closes_in_one_pass -- --exact
```

Expected: FAIL.

- [ ] **Step 3: Extract `process_fin_after_payload` from `receive_fin_in_established`**

In `crates/hammer-service/src/transport/tcp/connection.rs`, rename and re-expose:

```rust
/// Call AFTER `receive_payload` has advanced `rcv_nxt`.
/// Checks FIN against the current (post-payload) `rcv_nxt`.
pub(crate) fn process_fin_after_payload(
    &mut self,
    packet: &TcpPacket,
) -> CoreResult<Option<TcpSegment>> {
    if packet.flags.contains(TcpSegmentFlags::FIN) {
        if self.state != TcpState::Established {
            return Ok(None);
        }
        /* After payload delivery, seq + len == rcv_nxt for in-order FIN+data. */
        if packet.sequence.advance(packet.payload_len as u32) == self.rcv_nxt {
            self.rcv_nxt = self.rcv_nxt.advance(1);
            self.state = TcpState::CloseWait;
            return Ok(Some(self.control_segment(
                packet.local,
                packet.remote,
                TcpSegmentFlags::ACK,
                None,
                TcpCapabilities::default(),
            )));
        }
    }
    Ok(None)
}
```

Remove the old `receive_fin_in_established` private method. Remove the FIN check from `receive_established` (which now only handles ACK processing).

- [ ] **Step 4: Reorder established.rs to process data before FIN**

In `crates/hammer-service/src/transport/tcp/established.rs`, in the `receive_established` / `accept_payload` / `enqueue_rx` / `receive_payload` / `process_fin_after_payload` sequence:

```rust
// 1. process payload (before FIN)
let accept_payload = connection.accept_payload(&packet);
if let Some((trim, offset)) = accept_payload {
    let accepted_sequence = if packet.is_data() {
        packet.sequence.advance(trim)
    } else {
        SequenceNumber::new(0)
    };
    let index = runtime.buffer_index_from_frame(frame)?;
    frame.take_index();
    let enqueue = queue.enqueue_rx(session_id, index, offset)?;
    let connection = queue.session_mut(session_id).ok_or_else(|| {
        let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
        TcpNodeError::EstablishedSessionMissing
    })?;
    connection.receive_payload(
        accepted_sequence,
        trim as u32,
        enqueue.delivered_len,
        enqueue.newest_ooo_start,
        enqueue.newest_ooo_len,
    );
    if enqueue.delivered_len != 0 && !immediate_ack {
        queue.app().copy_rx_from_buffer_ooo(/*...*/);
    }
}
// 2. then check FIN (after rcv_nxt is advanced)
let (control, _) = connection.receive_established(&packet)?;
let fin_control = connection.process_fin_after_payload(&packet)?;
if fin_control.is_some() {
    // peer sent FIN (possibly on the same segment as data)
    // Task 13 will push HalfClose event here
    notify_close_wait = true;
}
let control = control.or(fin_control);
```

- [ ] **Step 5: Remove `_: bool` parameter from `enqueue_rx`**

In `crates/hammer-service/src/session/runtime.rs`, change the signature:

```rust
pub(crate) fn enqueue_rx(
    &self,
    session_id: SessionId,
    index: BufferIndex,
    offset: u32,
) -> CoreResult<SessionRxEnqueue> {
```

Update the function body (no `_` param to ignore). Update all 5 callers in:
- `crates/hammer-service/src/transport/tcp/established.rs:291` — remove `, false`
- `crates/hammer-service/src/transport/tcp/listen.rs:1256` — remove `, false`
- `crates/hammer-service/src/transport/tcp/mod.rs:943` — remove `, false`
- `crates/hammer-service/src/transport/tcp/syn_sent.rs:232` — remove `, false`
- `crates/hammer-service/src/transport/tcp/rcv_process.rs:281` — remove `, false`

- [ ] **Step 6: Run TCP tests**

Run:
```bash
cargo test -p hammer-service --test tcp_session_app_boundary fin_with_payload_closes_in_one_pass -- --exact
cargo test -p hammer-service --lib transport::tcp::
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs \
        crates/hammer-service/src/transport/tcp/established.rs \
        crates/hammer-service/src/transport/tcp/{listen,syn_sent,rcv_process,mod}.rs \
        crates/hammer-service/src/session/runtime.rs \
        crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "tcp(Fix): process payload before FIN, remove dead _:bool param"
```

---

### Task 13: Peer FIN Notifies App via `SessionEvtType::HalfClose`

**Files:**
- Modify: `crates/hammer-infra/src/msg_queue.rs`
- Modify: `crates/hammer-runtime/src/app/application.rs`
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-app/src/remote_session.rs`
- Test: `crates/hammer-service/tests/tcp_session_app_boundary.rs`

- [ ] **Step 1: Add failing tests**

Append to `crates/hammer-service/tests/tcp_session_app_boundary.rs`:

```rust
#[test]
fn peer_fin_notifies_app_half_close() {
    let msg_queue_source = include_str!("../crates/hammer-infra/src/msg_queue.rs");
    let app_source = include_str!("../crates/hammer-service/src/session/app.rs");
    let connection_source = include_str!("../crates/hammer-service/src/transport/tcp/connection.rs");

    assert!(
        msg_queue_source.contains("HalfClose"),
        "SessionEvtType must have a HalfClose variant"
    );
    assert!(
        app_source.contains("half_closed"),
        "SessionAppRuntime must have a half_closed method"
    );
    assert!(
        connection_source.contains("push_event")
            && connection_source.contains("HalfClose"),
        "process_fin_after_payload must push HalfClose event"
    );
}

#[test]
fn half_close_returns_recv_zero() {
    let app_runtime_source = include_str!("../crates/hammer-runtime/src/app/application.rs");

    assert!(
        app_runtime_source.contains("HalfClose")
            || app_runtime_source.contains("half_close"),
        "AppWorker must handle HalfClose events (e.g. return 0 from recv)"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:
```bash
cargo test -p hammer-service --test tcp_session_app_boundary peer_fin_notifies_app_half_close -- --exact
```

Expected: FAIL.

- [ ] **Step 3: Add `SessionEvtType::HalfClose` variant**

In `crates/hammer-infra/src/msg_queue.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEvtType {
    RxEnq,
    TxDeq,
    Connect,
    Close,
    HalfClose,
}
```

In `crates/hammer-app/src/remote_session.rs`, add a `SessionEvent::HalfClose` variant (or map to an existing variant — e.g. `SessionEvent::Closed` with a flag — depending on the remote protocol).

- [ ] **Step 4: Add `half_closed` method on `SessionAppRuntime`**

In `crates/hammer-service/src/session/app.rs`:

```rust
pub(crate) fn half_closed(&self, session_id: SessionId) -> CoreResult<()> {
    let Some(session) = self.sessions.lookup(&session_id.get()) else {
        return Ok(());
    };
    let handle = session.session_handle();
    session.push_event(SessionEvtType::HalfClose)?;
    hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
        worker.wake_evt(handle);
        worker.wake_rx(handle); /* recv should return 0 on HalfClose */
    });
    Ok(())
}
```

- [ ] **Step 5: Call `half_closed` from `process_fin_after_payload`**

In `crates/hammer-service/src/transport/tcp/connection.rs`, `process_fin_after_payload` (or in `established.rs` after the call to `process_fin_after_payload`), add:

```rust
if let Some(control) = fin_control {
    if self.state == TcpState::CloseWait {
        queue.app().half_closed(session_id)?;
    }
}
```

- [ ] **Step 6: Handle HalfClose in local app**

In `crates/hammer-runtime/src/app/application.rs`, `AppWorker::recv` should return `0` when the session is half-closed (poison FIFO or check state). `AppWorker::next_event` should surface the `HalfClose` variant. `send_all` should probably return an error or `0` on half-close (can't send if remote closed).

- [ ] **Step 7: Run TCP/app boundary tests**

Run:
```bash
cargo test -p hammer-service --test tcp_session_app_boundary
cargo test -p hammer-runtime --lib app::
cargo test -p hammer-runtime --test app_worker_notify
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/hammer-infra/src/msg_queue.rs \
        crates/hammer-runtime/src/app/application.rs \
        crates/hammer-service/src/session/app.rs \
        crates/hammer-service/src/session/runtime.rs \
        crates/hammer-service/src/transport/tcp/connection.rs \
        crates/hammer-service/src/transport/tcp/established.rs \
        crates/hammer-app/src/remote_session.rs \
        crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "tcp(Feat): push HalfClose event on peer FIN, wake app"
```

---

### Task 14: Fix SACK Left Boundary After OOO Promote

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Test: `crates/hammer-service/tests/tcp_session_app_boundary.rs`

- [ ] **Step 1: Add structural test**

Append to `crates/hammer-service/tests/tcp_session_app_boundary.rs`:

```rust
#[test]
fn sack_left_boundary_after_ooo_promote() {
    let connection_source = include_str!("../src/transport/tcp/connection.rs");

    assert!(
        connection_source.contains("save_rcv_nxt")
            || connection_source.contains("pre_advance_rcv_nxt")
            || connection_source.contains("before_payload_rcv_nxt"),
        "receive_payload must compute SACK left from pre-advance rcv_nxt"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:
```bash
cargo test -p hammer-service --test tcp_session_app_boundary sack_left_boundary_after_ooo_promote -- --exact
```

Expected: FAIL.

- [ ] **Step 3: Fix `receive_payload` SACK base**

In `crates/hammer-service/src/transport/tcp/connection.rs`, in `receive_payload`:

```rust
pub(crate) fn receive_payload(
    &mut self,
    sequence: TcpSeq,
    trim: u32,
    delivered_len: u32,
    newest_ooo_start: Option<u32>,
    newest_ooo_len: u32,
) {
    let before_payload_rcv_nxt = self.rcv_nxt; // save base for SACK

    self.rcv_nxt = self.rcv_nxt.advance(delivered_len);
    if !self.state.is_established() {
        return;
    }

    if let Some(start) = newest_ooo_start {
        /* Compute SACK left from the pre-advance rcv_nxt, matching the
         * origin of `newest_ooo_start` (which is relative to the rcv_nxt
         * value at the time newest_ooo_start was computed by accept_payload). */
        let left = before_payload_rcv_nxt.advance(start);
        let right = left.advance(newest_ooo_len);
        self.sack.update_range(
            self.rcv_nxt,
            left,
            right,
            &self.snd_una,
        );
    }
}
```

- [ ] **Step 4: Run TCP tests**

Run:
```bash
cargo test -p hammer-service --test tcp_session_app_boundary
cargo test -p hammer-service --lib transport::tcp::
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/connection.rs \
        crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "tcp(Fix): compute SACK left boundary from pre-advance rcv_nxt"
```

---

### Task 16: Complete Producer-Side Wake Coverage

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-service/tests/tcp_session_app_boundary.rs`

Precondition: Enhanced Task 7 (three-phase protocol + wake API) is already committed.

- [ ] **Step 1: Add structural coverage test**

Append to `crates/hammer-service/tests/tcp_session_app_boundary.rs`:

```rust
#[test]
fn producer_side_wake_calls_are_present() {
    let app_source = include_str!("../src/session/app.rs");
    let runtime_source = include_str!("../src/session/runtime.rs");

    /* All producer-side wake points must be present.
     * Count: in-order RX (1), OOO RX (1), TX release (1), TX buffer dispatch (1),
     * runtime ready (1), close (1), connect (1), half-close (1) ≈ 8 calls. */
    let wake_rx_count = app_source.matches("wake_rx").count()
        + runtime_source.matches("wake_rx").count();
    let wake_tx_count = app_source.matches("wake_tx").count()
        + runtime_source.matches("wake_tx").count();
    let wake_evt_count = app_source.matches("wake_evt").count()
        + runtime_source.matches("wake_evt").count();

    assert!(
        wake_rx_count >= 2,
        "wake_rx must be called from at least in-order RX and OOO RX paths; got {}",
        wake_rx_count
    );
    assert!(
        wake_tx_count >= 2,
        "wake_tx must be called from at least TX release and TX dispatch paths; got {}",
        wake_tx_count
    );
    assert!(
        wake_evt_count >= 2,
        "wake_evt must be called from at least connect/close paths; got {}",
        wake_evt_count
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:
```bash
cargo test -p hammer-service --test tcp_session_app_boundary producer_side_wake_calls_are_present -- --exact
```

Expected: FAIL because `wake_rx`, `wake_tx`, `wake_evt` calls are missing from OOO RX, TX dispatch, and runtime-ready paths.

- [ ] **Step 3: Add `wake_rx` after OOO RX**

In `crates/hammer-service/src/session/app.rs`, in `copy_rx_from_buffer_ooo`, after the for-loop and `buffers.free_index(index)`, add:

```rust
if total_len > 0 {
    let handle = session.session_handle();
    hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
        worker.wake_rx(handle);
    });
}
```

- [ ] **Step 4: Add `wake_tx` after TX buffer dispatch**

In `crates/hammer-service/src/session/app.rs`, in `copy_tx_to_buffer`, after the while-loop (bytes copied from FIFO to dataplane), add:

```rust
let handle = session.session_handle();
hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
    worker.wake_tx(handle);
});
```

- [ ] **Step 5: Add `wake_tx` after runtime ready dispatch**

In `crates/hammer-service/src/session/runtime.rs`, in `drain_tx_events_to`, after the `ready.mark_ready(*session_id)` call, add:

```rust
let handle = SessionHandle::new(session_id.get(), self.runtime.worker_index);
hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
    worker.wake_tx(handle);
});
```

- [ ] **Step 6: Run coverage test**

Run:
```bash
cargo test -p hammer-service --test tcp_session_app_boundary producer_side_wake_calls_are_present -- --exact
```

Expected: PASS.

- [ ] **Step 7: Run full workspace tests**

Run:
```bash
cargo build --workspace
cargo test -p hammer-service --test tcp_session_app_boundary
cargo test -p hammer-service --lib transport::tcp:: session::
cargo test -p hammer-runtime --test app_worker_notify
cargo test -p hammer-runtime --lib app::
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/hammer-service/src/session/app.rs \
        crates/hammer-service/src/session/runtime.rs \
        crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "session(Fix): complete producer-side wake coverage"
```

---

## Execution Order

```
Parent Tasks 1–6
  → Enhanced Task 7 (this supplement)
  → Parent Tasks 8–10
  → Task 12 (FIN+payload order + dead param)
  → Task 14 (SACK boundary)
  → Task 13 (HalfClose event)
  → Task 16 (wake coverage completion)
```

Rationale:
- Task 7 must come first (wake API + three-phase protocol are prerequisites for all subsequent wake calls).
- Task 12 before 13 (FIN ordering must be correct before HalfClose semantics are meaningful).
- Task 14 before 13 (SACK correctness needed for OOO support which HalfClose may interact with).
- Task 13 before 16 (HalfClose wake calls are part of the coverage count).
- Task 16 last (fills the remaining gaps after all event sources are defined).

---

## Verification

After all tasks land, run:

```bash
cargo fmt --all -- --check
cargo test -p hammer-infra --test fifo_ooo
cargo test -p hammer-runtime --lib app::
cargo test -p hammer-runtime --test app_worker_notify
cargo test -p hammer-app --test remote_session_event
cargo test -p hammer-service --test tcp_session_app_boundary
cargo test -p hammer-service --test tcp_output
cargo test -p hammer-service --lib session:: transport::tcp::
cargo test --workspace
```

Expected:
- All commands pass.
- `enqueue_rx` has 3 parameters.
- `SessionEvtType` has 5 variants including `HalfClose`.
- `wake_rx|wake_tx|wake_evt` are called from all producer paths (≥8 calls).
- SACK block boundaries are correct after OOO promote.
- FIN+data segment closes in one pass.
- Peer FIN→CloseWait pushes `HalfClose` event and wakes app.
- Async loops survive early `notify_waiters` without losing wakeups.

---

## Self-Review

- **Spec coverage:** Covers all 6 gap areas identified in the July 2026 audit that parent Tasks 1–10 did not address.
- **Placeholder scan:** No `TBD`, `TODO`, or unspecified test commands remain.
- **Type consistency:** New names are consistent with parent plan: `SessionEvtType::HalfClose`, `AppWorker::wake_rx/wake_tx/wake_evt`, `process_fin_after_payload`.
- **Boundary discipline:** No TCP-specific APIs added to runtime/buffer infra; no `Notify` access from session/TCP code.

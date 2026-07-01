# TCP Session App Defect Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the remaining correctness gaps between TCP, L5 session, and app-facing FIFOs/events so Hammer matches VPP-style `tcp_session_enqueue_*`, `svm_fifo`, and app wake semantics.

**Architecture:** Repair the receive path from the bottom up: first make `hammer-infra::Fifo` OOO writes invisible until contiguous, then make session RX report exact enqueue outcomes, then make TCP update ACK/SACK/window state only from those outcomes. In parallel, reconnect local app async wakeups to existing FIFO/message-queue event production, and keep cross-process SVM integration as a separately approved follow-up because the current service path is still Local-only.

**Tech Stack:** Rust 2024, `hammer-infra::{fifo,msg_queue,segment}`, `hammer-runtime::app`, `hammer-service::{session,transport::tcp}`, local VPP references in `third_party/vpp/src/svm/svm_fifo.c` and `third_party/vpp/src/vnet/tcp/tcp_input.c`, tests via `cargo test -p hammer-infra`, `cargo test -p hammer-runtime`, and `cargo test -p hammer-service`.

---

## Current Defects

### P0: OOO RX is not enabled for production app sessions

Evidence:
- `crates/hammer-runtime/src/app/session.rs:41-64` creates `rx_fifo` with `Fifo::new(...)` but never calls `enable_ooo()`.
- `crates/hammer-service/src/session/app.rs:182-185` calls `session.rx_fifo().enqueue_ooo(...)` for TCP out-of-order payload.
- `crates/hammer-infra/src/fifo.rs:682-684` returns `Err(())` when OOO bookkeeping is absent.

Impact:
- TCP out-of-order payload in `tcp-established` returns `CoreError("ooo enqueue failed")`, the input buffer goes to drop, and `TcpConnection::receive_payload` is not called. SACK/dup-ACK behavior is therefore broken for real OOO receive.

### P0: OOO FIFO writes expose future bytes to app before the gap is filled

Evidence:
- `crates/hammer-infra/src/fifo.rs:682-687` calls `enqueue_at(abs_pos, src)`.
- `crates/hammer-infra/src/fifo.rs:488-490` makes `enqueue_at` store `tail = max(tail, offset + written)`.
- `crates/hammer-runtime/src/app/application.rs:185-189` reads app-visible RX bytes from `rx_fifo.peek(0, ...)` and advances with `dequeue_drop`.

VPP reference:
- `third_party/vpp/src/svm/svm_fifo.c:892-927` `svm_fifo_enqueue_with_offset` copies future bytes but does not store a new tail.
- `third_party/vpp/src/svm/svm_fifo.c:833-884` in-order enqueue later collects OOO data and only then advances tail.

Impact:
- If OOO is enabled without fixing `Fifo`, app reads can observe gaps/future data as contiguous stream bytes. This corrupts byte-stream semantics.

### P0: OOO RX creates an intermediate payload Vec

Evidence:
- `crates/hammer-service/src/session/app.rs:176-180` builds a `hammer_infra::vec::Vec<u8>` from the buffer chain before enqueuing OOO bytes.

Impact:
- Violates AGENTS.md TCP rule: after app/session ownership, session/TCP/recovery/output/buffer/runtime code must not create intermediate payload `Vec`s or private payload copies.
- Adds an extra copy on the receive hot path.

### P0: OOO writes are not capacity-accounted

Evidence:
- `crates/hammer-infra/src/fifo.rs:682-687` writes OOO bytes without checking `offset + len` against FIFO free space.
- `crates/hammer-infra/src/fifo.rs:504-510` computes `max_enqueue()` from visible `tail - head` only.

VPP reference:
- `third_party/vpp/src/svm/svm_fifo.c:904-911` rejects future-offset writes when `len + offset > free_count`.

Impact:
- Once OOO writes stop advancing visible tail, the FIFO must still reserve capacity for future bytes. Without that, receive-window reporting can over-advertise and later writes can overrun or overwrite future data.

### P0: RX partial enqueue can acknowledge bytes that were not accepted

Evidence:
- `crates/hammer-service/src/session/app.rs:143-162` returns only the number of bytes written by `session.enqueue_rx`.
- `crates/hammer-service/src/session/runtime.rs:389-399` converts that into `SessionRxEnqueue { delivered_len: wrote }`.
- `crates/hammer-service/src/transport/tcp/connection.rs:1711` advances `rcv_nxt` by `delivered_len` and has no error/status for partial in-order enqueue.
- `crates/hammer-service/src/transport/tcp/established.rs:313-315` marks the session ready when `delivered_len != 0`, but does not signal receive-window collapse or retry policy for the unwritten suffix.

VPP reference:
- `third_party/vpp/src/vnet/tcp/tcp_input.c:1054-1102` distinguishes full enqueue, partial enqueue, FIFO full, and zero receive-window cases.

Impact:
- If RX FIFO is nearly full, Hammer can accept only a prefix while dropping the original packet buffer. The peer may retransmit the suffix, but Hammer has already advanced `rcv_nxt` for the prefix and does not explicitly shrink/reopen receive window based on FIFO space.

### P0: Advertised receive window is static, not app/FIFO backed

Evidence:
- `crates/hammer-service/src/transport/tcp/connection.rs:360-362` initializes `rcv_wnd` to `DEFAULT_TCP_WINDOW`.
- `crates/hammer-service/src/transport/tcp/connection.rs:941`, `1262`, and `1295` advertise `self.advertised_receive_window(self.rcv_wnd)`.
- No production code updates `rcv_wnd` from `session.rx_fifo().max_enqueue()` or app consumption.

Impact:
- App backpressure is not reflected to TCP peers. When the app stops reading, Hammer can keep advertising a stale receive window until FIFO writes fail.

### P0: Local app async waiters are never notified

Evidence:
- `crates/hammer-runtime/src/app/application.rs:20-27` defines per-session `Notify`s.
- `crates/hammer-runtime/src/app/application.rs:171-173`, `196-198`, and `212-214` wait on those notifies.
- `crates/hammer-runtime/src/app/session.rs:178-209` only enqueues `SessionEvt`s; no producer calls `Notify::notify_waiters`.
- `rg "notify_|wake_" crates/hammer-runtime/src crates/hammer-service/src` shows no producer-side wake API.

Impact:
- Local async `AppWorker::recv`, `send_all`, and `next_event` can sleep forever even though FIFOs/message queues have changed.

### P1: Remote app event wait consumes one event

Evidence:
- `crates/hammer-app/src/remote_session.rs:91-97` `next_event()` first checks `evt_q.dequeue()`.
- `crates/hammer-app/src/remote_session.rs:103-122` `wait_for_event()` also calls `self.session.evt_q().dequeue().is_some()` and discards that event.

Impact:
- The first event that wakes a remote app can be lost, so `next_event()` may miss `Connect`, `RxEnq`, `TxDeq`, or `Close`.

### P1: SVM backend is configured but not wired into service session creation

Evidence:
- `crates/hammer-service/src/transport/mod.rs:92-114` dispatches `SessionBackend::{Local,Svm}` through `with_segment!`.
- `crates/hammer-service/src/session/runtime.rs:427-485` implements `insert_session_with_id` only for `SessionDriverRuntime<St, Local>`.
- `crates/hammer-runtime/src/attach.rs` and `crates/hammer-app/src/attach.rs` exist but are not used by TCP listen/active-open session creation.

Impact:
- Config can select SVM, and attach assets exist, but TCP session creation remains Local-only. This is misleading API surface and incomplete VPP-style cross-process app/session integration.

### P2: Session queue dispatch still relies on raw-pointer split borrows

Evidence:
- `crates/hammer-service/src/session/runtime.rs:560-573` uses raw pointers in `with_session_state`.
- `crates/hammer-service/src/session/runtime.rs:587-721` repeatedly re-enters that helper for timer, ready, prepare, commit, and cancel paths.

Impact:
- Current code encapsulates the unsafe blocks, but future session/TCP changes can accidentally widen aliasing assumptions. This is a maintainability risk after the correctness fixes.

---

## Approval Required Before Implementation

These API changes are small but cross layer boundaries; get explicit user approval before coding them.

1. `hammer-runtime::app::AppWorker` producer wake methods
   - **Final result:** add `wake_rx(handle)`, `wake_tx(handle)`, and `wake_evt(handle)` or an equivalent generic method that wakes existing per-session `Notify`s after FIFO/message queue events are produced.
   - **Why existing API is not enough:** waiters exist but there is no producer-side API, and `AppSession` intentionally no longer owns `Notify`.
   - **Boundary:** app runtime only; session/TCP call through app/session boundary, not by touching `Notify` directly.

2. TCP receive-window refresh surface
   - **Final result:** a narrow method such as `TcpConnection::set_receive_window(bytes: u32)` or `TcpConnection::refresh_receive_window(bytes: u32)` called from session runtime after RX enqueue/app consumption observations.
   - **Why existing API is not enough:** `rcv_wnd` is private TCP state and currently only exposed through `rcv_wnd()`. Session must not write TCP fields directly.
   - **Boundary:** TCP owns advertised-window policy; session supplies FIFO free-space facts only.

---

## File Map

- `crates/hammer-infra/src/fifo.rs`
  - Owns SVM-style FIFO storage, visible head/tail, OOO bookkeeping, and event bits.
- `crates/hammer-infra/tests/fifo_ooo.rs`
  - Tests future-offset writes, promotion, overlap/trim, and app-visible byte counts.
- `crates/hammer-runtime/src/app/session.rs`
  - Owns app-facing per-session FIFOs and message queue operations.
- `crates/hammer-runtime/src/app/application.rs`
  - Owns local app worker registry and async waiter notification.
- `crates/hammer-runtime/tests/app_worker_notify.rs`
  - New tests for Local async wakeups.
- `crates/hammer-app/src/remote_session.rs`
  - Owns cross-process async waiting over event fd.
- `crates/hammer-app/tests/remote_session_event.rs`
  - New test for remote event preservation.
- `crates/hammer-service/src/session/app.rs`
  - Copies buffer-chain bytes into app/session FIFOs and posts app events.
- `crates/hammer-service/src/session/runtime.rs`
  - Dispatches session queue, RX enqueue result reporting, TX queue polling, timers, and close cleanup.
- `crates/hammer-service/src/transport/tcp/connection.rs`
  - Owns TCP sequence, SACK/DSACK, receive-window, and ACK output facts.
- `crates/hammer-service/src/transport/tcp/established.rs`
  - Applies RX enqueue outcomes to established TCP packets.
- `crates/hammer-service/tests/tcp_session_app_boundary.rs`
  - New integration tests for OOO RX, partial RX enqueue, receive-window update, and app wake behavior.

---

### Task 1: Lock Down FIFO OOO Visibility

**Files:**
- Modify: `crates/hammer-infra/tests/fifo_ooo.rs`

- [ ] **Step 1: Add failing tests for future bytes not visible and gap collection**

Append to `crates/hammer-infra/tests/fifo_ooo.rs`:

```rust
#[test]
fn ooo_enqueue_does_not_advance_visible_tail_before_gap_fills() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    let result = f.enqueue_ooo(5, b"world").expect("ooo enqueue");

    assert_eq!(result.delivered, 0);
    assert_eq!(f.max_dequeue(), 0);
    assert_eq!(f.ooo_enqueued(), 1);
    let mut out = [0u8; 16];
    assert_eq!(f.peek(0, out.len(), &mut out), 0);
}

#[test]
fn in_order_enqueue_collects_contiguous_ooo_bytes() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    f.enqueue_ooo(5, b"world").expect("ooo enqueue");
    assert_eq!(f.enqueue(b"hello"), 10);

    let mut out = [0u8; 16];
    assert_eq!(f.peek(0, out.len(), &mut out), 10);
    assert_eq!(&out[..10], b"helloworld");
    assert_eq!(f.ooo_enqueued(), 0);
}

#[test]
fn ooo_enqueue_rejects_future_write_beyond_fifo_capacity() {
    let mut f = fifo(64);
    f.enable_ooo();

    assert!(f.enqueue_ooo(60, b"12345").is_err());
    assert_eq!(f.max_dequeue(), 0);
    assert_eq!(f.ooo_enqueued(), 0);
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test -p hammer-infra --test fifo_ooo
```

Expected:
- `ooo_enqueue_does_not_advance_visible_tail_before_gap_fills` fails because `max_dequeue()` becomes `10`.
- `in_order_enqueue_collects_contiguous_ooo_bytes` fails or returns the wrong visible bytes because `enqueue_ooo` already advanced tail.
- `ooo_enqueue_rejects_future_write_beyond_fifo_capacity` fails because current `enqueue_ooo` does not reject `offset + len > free_count`.

- [ ] **Step 3: Commit red tests**

```bash
git add crates/hammer-infra/tests/fifo_ooo.rs
git commit -m "hammer-infra(Test): capture fifo OOO visibility semantics"
```

### Task 2: Make Fifo OOO Match VPP Tail Semantics

**Files:**
- Modify: `crates/hammer-infra/src/fifo.rs`
- Test: `crates/hammer-infra/tests/fifo_ooo.rs`

- [ ] **Step 1: Add internal future-write helper that does not store visible tail**

In `crates/hammer-infra/src/fifo.rs`, replace the body of `enqueue_at` with a wrapper around a new private helper:

```rust
    fn write_at_without_tail_store(&self, offset: u32, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let chunk_data_size = (*hdr).min_alloc as usize;
            let mut chunk_off = (*hdr).head_chunk;
            let mut prev_off = 0u64;
            let mut written = 0usize;
            let mut remaining_offset = offset;
            let mut remaining_src = src;
            loop {
                let found = if chunk_off != 0 {
                    let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                    remaining_offset >= chunk.start_byte
                        && remaining_offset < chunk.start_byte + chunk_data_size as u32
                } else {
                    false
                };
                if found {
                    let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                    let data_ptr = self.base.add(chunk_off as usize + CHUNK_HEADER_SIZE);
                    let data_off = (remaining_offset - chunk.start_byte) as usize;
                    let chunk_avail = chunk_data_size - data_off;
                    let to_write = remaining_src.len().min(chunk_avail);
                    std::ptr::copy_nonoverlapping(
                        remaining_src.as_ptr(),
                        data_ptr.add(data_off),
                        to_write,
                    );
                    let new_used = data_off + to_write;
                    if new_used > chunk.length as usize {
                        chunk.length = new_used as u32;
                    }
                    written += to_write;
                    if to_write == remaining_src.len() {
                        break;
                    }
                    remaining_offset += to_write as u32;
                    remaining_src = &remaining_src[to_write..];
                    prev_off = chunk_off;
                    chunk_off = chunk.next.load(Ordering::Acquire);
                } else if chunk_off == 0 {
                    let new_off = self.seg.alloc(CHUNK_HEADER_SIZE + chunk_data_size, 64);
                    let start_byte = if prev_off != 0 {
                        let prev = &mut *(self.base.add(prev_off as usize) as *mut Chunk);
                        let prev_end = prev.start_byte + prev.length;
                        if prev_off == (*hdr).end_chunk {
                            (*hdr).end_chunk = new_off;
                            prev.next.store(new_off, Ordering::Release);
                        }
                        remaining_offset.max(prev_end)
                    } else {
                        (*hdr).start_chunk = new_off;
                        (*hdr).end_chunk = new_off;
                        (*hdr).head_chunk = new_off;
                        (*hdr).tail_chunk = new_off;
                        remaining_offset
                    };
                    let new_chunk = &mut *(self.base.add(new_off as usize) as *mut Chunk);
                    std::ptr::write(
                        new_chunk,
                        Chunk {
                            start_byte,
                            length: 0,
                            next: AtomicU64::new(0),
                            refcount: AtomicU32::new(1),
                        },
                    );
                    chunk_off = new_off;
                } else {
                    prev_off = chunk_off;
                    chunk_off = {
                        let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                        chunk.next.load(Ordering::Acquire)
                    };
                }
            }
            written
        }
    }

    pub fn enqueue_at(&self, offset: u32, src: &[u8]) -> usize {
        let written = self.write_at_without_tail_store(offset, src);
        if written == 0 {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let cur_tail = (*hdr).tail.load(Ordering::Relaxed);
            let new_tail = cur_tail.max(offset + written as u32);
            (*hdr).tail.store(new_tail, Ordering::Release);
        }
        written
    }
```

- [ ] **Step 2: Add tail-synchronized OOO promotion helper**

In `crates/hammer-infra/src/fifo.rs`, add this private helper in `impl<S: Segment> Fifo<S>`:

```rust
    fn promote_contiguous_from(&self, base: u32) -> u32 {
        let ooo = unsafe { &mut *self.ooo.get() };
        match ooo.as_mut() {
            Some(bk) => {
                bk.base = base;
                Self::promote_contiguous_inner(bk)
            }
            None => 0,
        }
    }
```

Then replace `promote_contiguous` with:

```rust
    pub fn promote_contiguous(&self) -> u32 {
        let base = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
        self.promote_contiguous_from(base)
    }
```

- [ ] **Step 3: Change `enqueue_ooo` to be tail-relative and capacity-accounted**

In `crates/hammer-infra/src/fifo.rs`, replace the first part of `enqueue_ooo`:

```rust
let abs_pos = bk.base.wrapping_add(offset);
let written = self.enqueue_at(abs_pos, src);
```

with:

```rust
let total_len = u32::try_from(src.len()).map_err(|_| ())?;
let end_offset = offset.checked_add(total_len).ok_or(())?;
if end_offset as usize > self.max_enqueue() {
    return Err(());
}
let tail = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
bk.base = tail;
let abs_pos = tail.wrapping_add(offset);
let written = self.write_at_without_tail_store(abs_pos, src);
```

- [ ] **Step 4: Make normal enqueue write at visible tail and collect OOO**

In `Fifo::enqueue`, replace the current chunk-append loop body after the `to_write == 0` check with:

```rust
let tail = (*hdr).tail.load(Ordering::Relaxed);
let written = self.write_at_without_tail_store(tail, &src[..to_write]);
if written == 0 {
    return 0;
}
let new_tail = tail.wrapping_add(written as u32);
(*hdr).tail.store(new_tail, Ordering::Release);
let collected = self.promote_contiguous_from(new_tail);
(*hdr)
    .tail
    .store(new_tail.wrapping_add(collected), Ordering::Release);
written + collected as usize
```

Keep the existing free-space calculation before this block:

```rust
let head = (*hdr).head.load(Ordering::Acquire);
let tail = (*hdr).tail.load(Ordering::Relaxed);
let used = tail.wrapping_sub(head);
let free = ((*hdr).size - used) as usize;
let to_write = src.len().min(free);
```

- [ ] **Step 5: Run FIFO OOO tests**

Run:

```bash
cargo test -p hammer-infra --test fifo_ooo
```

Expected: all `fifo_ooo` tests pass.

- [ ] **Step 6: Run FIFO unit tests**

Run:

```bash
cargo test -p hammer-infra --lib fifo::
```

Expected: all FIFO unit tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-infra/src/fifo.rs crates/hammer-infra/tests/fifo_ooo.rs
git commit -m "hammer-infra(Fix): keep fifo OOO bytes invisible until contiguous"
```

### Task 3: Enable OOO for App RX FIFO

**Files:**
- Modify: `crates/hammer-runtime/src/app/session.rs`
- Test: `crates/hammer-runtime/src/app/session.rs`

- [ ] **Step 1: Add failing test**

In `crates/hammer-runtime/src/app/session.rs` test module, add:

```rust
#[test]
fn app_session_rx_fifo_supports_ooo_enqueue() {
    let session = new_session(AppSessionConfig::new(64, 4), 1);

    let result = session
        .rx_fifo()
        .enqueue_ooo(5, b"world")
        .expect("rx fifo should support ooo");

    assert_eq!(result.delivered, 0);
    assert_eq!(session.rx_fifo().max_dequeue(), 0);
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p hammer-runtime --lib app::session::tests::app_session_rx_fifo_supports_ooo_enqueue -- --exact
```

Expected: FAIL with `rx fifo should support ooo`.

- [ ] **Step 3: Enable OOO during app session construction**

In `crates/hammer-runtime/src/app/session.rs`, change `new_in_segment`:

```rust
let rx_fifo = Arc::new(
    Fifo::<S>::new(seg.clone(), config.fifo_capacity)
        .map_err(|_| HammerError::internal("invalid rx fifo capacity"))?,
);
```

to:

```rust
let mut rx_fifo = Fifo::<S>::new(seg.clone(), config.fifo_capacity)
    .map_err(|_| HammerError::internal("invalid rx fifo capacity"))?;
rx_fifo.enable_ooo();
let rx_fifo = Arc::new(rx_fifo);
```

- [ ] **Step 4: Run runtime app tests**

Run:

```bash
cargo test -p hammer-runtime --lib app::
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-runtime/src/app/session.rs
git commit -m "hammer-runtime(Fix): enable OOO bookkeeping on app rx fifo"
```

### Task 4: Remove OOO RX Temporary Payload Vec

**Files:**
- Modify: `crates/hammer-infra/src/fifo.rs`
- Modify: `crates/hammer-service/src/session/app.rs`
- Test: `crates/hammer-service/tests/tcp_session_app_boundary.rs`

- [ ] **Step 1: Add failing structural test**

Create `crates/hammer-service/tests/tcp_session_app_boundary.rs`:

```rust
#[test]
fn session_ooo_rx_path_does_not_allocate_payload_vec() {
    let source = include_str!("../src/session/app.rs");

    assert!(
        !source.contains("let mut bytes = Vec::new()"),
        "OOO RX must stream buffer-chain slices into the session FIFO without a payload Vec"
    );
    assert!(
        !source.contains("bytes.extend_from_slice"),
        "OOO RX must not gather payload bytes before FIFO enqueue"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p hammer-service --test tcp_session_app_boundary session_ooo_rx_path_does_not_allocate_payload_vec -- --exact
```

Expected: FAIL because `copy_rx_from_buffer_ooo` builds `Vec`.

- [ ] **Step 3: Confirm existing FIFO API can stream each chain segment**

No new public FIFO API is needed for this task. Use repeated `Fifo::enqueue_ooo(relative_offset, current_slice)` calls while each `Ref<Buffer>` is still alive. This keeps payload bytes borrowed directly from the data-plane buffer and avoids both payload allocation and descriptor allocation.

- [ ] **Step 4: Update session OOO copy path**

In `crates/hammer-service/src/session/app.rs`, replace `copy_rx_from_buffer_ooo` with:

```rust
    pub(crate) fn copy_rx_from_buffer_ooo(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
        offset: u32,
    ) -> CoreResult<(u32, Option<u32>, u32)> {
        let Some(session) = self.sessions.lookup(&session_id.get()) else {
            buffers.free_index(index);
            return Ok((0, None, 0));
        };
        let mut total_len = 0u32;
        let mut delivered = 0u32;
        for buf in buffers.chain(index) {
            let buf = buf?;
            let current = buf.current();
            let chunk_offset = offset
                .checked_add(total_len)
                .ok_or_else(|| CoreError::internal("ooo rx offset overflow"))?;
            let result = session
                .rx_fifo()
                .enqueue_ooo(chunk_offset, current)
                .map_err(|_| CoreError::internal("ooo enqueue failed"))?;
            delivered = delivered.wrapping_add(result.delivered);
            total_len = total_len
                .checked_add(current.len() as u32)
                .ok_or_else(|| CoreError::internal("ooo rx buffer length overflow"))?;
        }
        buffers.free_index(index);
        Ok((delivered, Some(offset), total_len))
    }
```

The per-slice loop preserves borrowed buffer lifetimes correctly and avoids storing `&[u8]` past the `Ref<Buffer>` guard. It also keeps the API generic and avoids adding a transport-specific helper.

- [ ] **Step 5: Run boundary test**

Run:

```bash
cargo test -p hammer-service --test tcp_session_app_boundary session_ooo_rx_path_does_not_allocate_payload_vec -- --exact
```

Expected: PASS.

- [ ] **Step 6: Run infra and service tests**

Run:

```bash
cargo test -p hammer-infra --test fifo_ooo
cargo test -p hammer-service --test tcp_session_app_boundary
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-infra/src/fifo.rs \
        crates/hammer-service/src/session/app.rs \
        crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "session(Fix): stream OOO rx slices into fifo without payload Vec"
```

### Task 5: Report RX Enqueue Outcomes Precisely

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Test: `crates/hammer-service/tests/tcp_session_app_boundary.rs`

- [ ] **Step 1: Extend boundary tests for partial enqueue**

Append to `crates/hammer-service/tests/tcp_session_app_boundary.rs`:

```rust
#[test]
fn session_rx_enqueue_reports_partial_delivery_without_claiming_full_accept() {
    let runtime_source = include_str!("../src/session/runtime.rs");
    let tcp_source = include_str!("../src/transport/tcp/established.rs");

    assert!(
        runtime_source.contains("accepted_len"),
        "SessionRxEnqueue should report accepted_len separately from delivered_len"
    );
    assert!(
        tcp_source.contains("enqueue.accepted_len"),
        "TCP established path must branch on exact accepted_len"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p hammer-service --test tcp_session_app_boundary session_rx_enqueue_reports_partial_delivery_without_claiming_full_accept -- --exact
```

Expected: FAIL because `SessionRxEnqueue` does not expose `accepted_len`.

- [ ] **Step 3: Add accepted/full/error fields**

In `crates/hammer-service/src/session/runtime.rs`, change `SessionRxEnqueue` to:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionRxEnqueue {
    pub(crate) accepted_len: u32,
    pub(crate) delivered_len: u32,
    pub(crate) newest_ooo_start: Option<u32>,
    pub(crate) newest_ooo_len: u32,
    pub(crate) fifo_full: bool,
}
```

- [ ] **Step 4: Return accepted length from in-order RX copy**

In `crates/hammer-service/src/session/app.rs`, keep `copy_rx_from_buffer` returning bytes written. In `crates/hammer-service/src/session/runtime.rs`, set:

```rust
Ok(SessionRxEnqueue {
    accepted_len: wrote as u32,
    delivered_len: wrote as u32,
    newest_ooo_start: None,
    newest_ooo_len: 0,
    fifo_full: wrote == 0,
})
```

For OOO:

```rust
Ok(SessionRxEnqueue {
    accepted_len: ooo_len,
    delivered_len: delivered,
    newest_ooo_start: ooo_start,
    newest_ooo_len: ooo_len,
    fifo_full: ooo_len == 0,
})
```

- [ ] **Step 5: Make established TCP react to partial/FIFO-full enqueue**

In `crates/hammer-service/src/transport/tcp/established.rs`, after `queue.enqueue_rx(...)`, add:

```rust
if enqueue.accepted_len != accepted_len as u32 {
    let connection = queue.session_mut(session_id).ok_or_else(|| {
        let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
        TcpNodeError::EstablishedSessionMissing
    })?;
    connection.note_receive_window_from_session(0);
    immediate_ack = true;
}
```

Use the approved receive-window method name from Task 6. If Task 6 has not landed, keep this task on a branch after Task 6.

- [ ] **Step 6: Run focused tests**

Run:

```bash
cargo test -p hammer-service --test tcp_session_app_boundary
cargo test -p hammer-service --lib transport::tcp::
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs \
        crates/hammer-service/src/session/app.rs \
        crates/hammer-service/src/transport/tcp/established.rs \
        crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "tcp(Fix): preserve exact session rx enqueue outcomes"
```

### Task 6: Connect TCP Receive Window to Session RX FIFO Capacity

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Test: `crates/hammer-service/tests/tcp_session_app_boundary.rs`

- [ ] **Step 1: Add failing receive-window structural test**

Append to `crates/hammer-service/tests/tcp_session_app_boundary.rs`:

```rust
#[test]
fn tcp_receive_window_is_refreshed_from_session_rx_fifo_capacity() {
    let connection_source = include_str!("../src/transport/tcp/connection.rs");
    let established_source = include_str!("../src/transport/tcp/established.rs");

    assert!(
        connection_source.contains("note_receive_window_from_session"),
        "TcpConnection needs a narrow API for session-provided RX capacity facts"
    );
    assert!(
        established_source.contains("rx_available_len"),
        "established RX path must refresh advertised window from session RX capacity"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p hammer-service --test tcp_session_app_boundary tcp_receive_window_is_refreshed_from_session_rx_fifo_capacity -- --exact
```

Expected: FAIL.

- [ ] **Step 3: Add session RX capacity accessor**

In `crates/hammer-service/src/session/app.rs`, add:

```rust
    pub(crate) fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        self.sessions
            .lookup(&session_id.get())
            .map(|session| session.rx_fifo().max_enqueue())
    }
```

In `crates/hammer-service/src/session/runtime.rs`, add:

```rust
    pub(crate) fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        self.app_state.app.rx_available_len(session_id)
    }
```

- [ ] **Step 4: Add TCP receive-window API after approval**

In `crates/hammer-service/src/transport/tcp/connection.rs`, add:

```rust
    #[inline]
    pub(crate) fn note_receive_window_from_session(&mut self, available: usize) {
        self.rcv_wnd = u32::try_from(available).unwrap_or(u32::MAX);
    }
```

- [ ] **Step 5: Refresh window in established RX path before output**

In `crates/hammer-service/src/transport/tcp/established.rs`, after RX enqueue handling and before constructing any immediate ACK:

```rust
if let Some(available) = queue.rx_available_len(session_id) {
    let connection = queue.session_mut(session_id).ok_or_else(|| {
        let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
        TcpNodeError::EstablishedSessionMissing
    })?;
    connection.note_receive_window_from_session(available);
}
```

- [ ] **Step 6: Run TCP/session tests**

Run:

```bash
cargo test -p hammer-service --test tcp_session_app_boundary
cargo test -p hammer-service --test tcp_output
cargo test -p hammer-service --lib transport::tcp::
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/session/app.rs \
        crates/hammer-service/src/session/runtime.rs \
        crates/hammer-service/src/transport/tcp/connection.rs \
        crates/hammer-service/src/transport/tcp/established.rs \
        crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "tcp(Fix): derive receive window from session rx fifo space"
```

### Task 7: Wake Local App Waiters

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
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test -p hammer-runtime --test app_worker_notify
```

Expected: FAIL to compile because `wake_rx` and `wake_evt` do not exist.

- [ ] **Step 3: Add producer wake methods after approval**

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

- [ ] **Step 4: Call wake methods from session app runtime**

In `crates/hammer-service/src/session/app.rs`, after `session.push_event(SessionEvtType::Connect)?`, add:

```rust
let handle = session.session_handle();
hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
    worker.wake_evt(handle);
});
```

After `session.push_event(SessionEvtType::Close)?`, call the same `wake_evt`.

After `copy_rx_from_buffer` writes `wrote > 0`, call:

```rust
let handle = session.session_handle();
hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
    worker.wake_rx(handle);
});
```

After `release_pending_send_bytes` drops bytes, call `wake_tx(handle)`.

- [ ] **Step 5: Run runtime wake tests**

Run:

```bash
cargo test -p hammer-runtime --test app_worker_notify
```

Expected: PASS.

- [ ] **Step 6: Run app/session tests**

Run:

```bash
cargo test -p hammer-runtime --lib app::
cargo test -p hammer-service --lib session::
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-runtime/src/app/application.rs \
        crates/hammer-runtime/tests/app_worker_notify.rs \
        crates/hammer-service/src/session/app.rs \
        crates/hammer-service/src/session/runtime.rs
git commit -m "hammer-runtime(Fix): wake local app waiters from session events"
```

### Task 8: Preserve RemoteAppSession Events

**Files:**
- Modify: `crates/hammer-app/src/remote_session.rs`
- Test: `crates/hammer-app/tests/remote_session_event.rs`

- [ ] **Step 1: Add structural regression test**

Create `crates/hammer-app/tests/remote_session_event.rs`:

```rust
#[test]
fn remote_wait_for_event_does_not_dequeue_session_events() {
    let source = include_str!("../src/remote_session.rs");

    assert!(
        !source.contains("if self.session.evt_q().dequeue().is_some()"),
        "RemoteAppSession::wait_for_event must not consume the event that woke the app"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p hammer-app --test remote_session_event remote_wait_for_event_does_not_dequeue_session_events -- --exact
```

Expected: FAIL.

- [ ] **Step 3: Change wait loop to inspect queue size without dequeue**

First add a non-consuming `MsgQueue::is_empty()` API in `crates/hammer-infra/src/msg_queue.rs`:

```rust
    pub fn is_empty(&self) -> bool {
        let head = unsafe { (*self.hdr).head.load(Ordering::Acquire) };
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
        head == tail
    }
```

Then in `crates/hammer-app/src/remote_session.rs`, replace:

```rust
if self.session.evt_q().dequeue().is_some() {
    return;
}
```

with:

```rust
if !self.session.evt_q().is_empty() {
    return;
}
```

- [ ] **Step 4: Run remote event test**

Run:

```bash
cargo test -p hammer-app --test remote_session_event
```

Expected: PASS.

- [ ] **Step 5: Run msg queue tests**

Run:

```bash
cargo test -p hammer-infra --lib msg_queue::
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-app/src/remote_session.rs \
        crates/hammer-app/tests/remote_session_event.rs \
        crates/hammer-infra/src/msg_queue.rs
git commit -m "hammer-app(Fix): preserve remote session events while waiting"
```

### Task 9: Decide SVM Backend Scope

**Files:**
- Modify: `docs/superpowers/plans/2026-06-30-tcp-session-app-defect-audit.md`
- Later plan: `docs/superpowers/plans/YYYY-MM-DD-svm-session-backend-wiring.md`

- [ ] **Step 1: Ask for explicit scope decision**

Use this exact question before coding SVM work:

```text
SVM backend is currently exposed in config and attach helpers but not wired into TCP session creation. Should I (A) hide/reject SessionBackend::Svm until a full attach loop exists, or (B) write a separate implementation plan to wire AttachServer/AttachClient into service session creation?
```

- [ ] **Step 2: If option A, add config guard plan**

Prepare a small follow-up plan that makes `transport::init` reject `SessionBackend::Svm` with a clear `CoreError` until the backend is implemented.

- [ ] **Step 3: If option B, write a separate SVM backend plan**

The separate plan must cover:
- attach server lifecycle and Unix socket ownership;
- session id/handle allocation from TCP accept/open path;
- `SessionDriverRuntime<St, Svm>` session insertion without `with_current_app_worker<Local>`;
- fd passing tests with `AttachClient`;
- remote wake semantics through event fds.

No code is changed in this audit task.

### Task 10: Replace Raw-Pointer Session Dispatch Borrowing

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-service/tests/tcp_session_app_boundary.rs`

- [ ] **Step 1: Add structural test**

Append to `crates/hammer-service/tests/tcp_session_app_boundary.rs`:

```rust
#[test]
fn session_dispatch_does_not_repeat_raw_pointer_state_borrows() {
    let source = include_str!("../src/session/runtime.rs");

    assert!(
        !source.contains("unsafe fn with_session_state"),
        "session dispatch should use a safe split-borrow work handle"
    );
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cargo test -p hammer-service --test tcp_session_app_boundary session_dispatch_does_not_repeat_raw_pointer_state_borrows -- --exact
```

Expected: FAIL.

- [ ] **Step 3: Introduce private work handle**

In `crates/hammer-service/src/session/runtime.rs`, add:

```rust
struct SessionDispatchWork<'a, St, Seg: Segment> {
    state: &'a mut St,
    timers: &'a mut TimerWheel1t2w2048sl<u32>,
    ready: &'a mut SessionReadyQueue,
    buffers: &'a DataPlaneBuffers,
    app: &'a SessionAppRuntime<Seg>,
    session_id: SessionId,
}

impl<'a, St, Seg: Segment> SessionDispatchWork<'a, St, Seg> {
    fn context(&mut self) -> SessionQueueControlContext {
        SessionQueueControlContext::new(
            self.timers as *mut _,
            self.ready as *mut _,
            self.buffers as *const _,
            self.session_id,
            self.app.has_pending_send(self.session_id),
        )
    }
}
```

- [ ] **Step 4: Refactor dispatch call sites**

Replace each `with_session_state(...)` call in `dispatch_session_queue_pending` by a scoped borrow that builds `SessionDispatchWork` from disjoint fields. Keep `SessionQueueControlContext` pointer fields unchanged in this task; only remove repeated raw-pointer derivation from dispatch.

- [ ] **Step 5: Run service session/TCP tests**

Run:

```bash
cargo test -p hammer-service --lib session:: transport::tcp::
cargo test -p hammer-service --test tcp_session_app_boundary
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs \
        crates/hammer-service/tests/tcp_session_app_boundary.rs
git commit -m "session(Refactor): use dispatch work handle for session queue borrows"
```

---

## Verification

After all approved tasks land, run:

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
- No OOO RX path creates payload `Vec`s.
- App-visible RX bytes never include future OOO bytes before gaps close.
- Local app async receive/event/TX-space waiters wake.
- TCP ACK/window output reflects session RX FIFO capacity.

---

## Self-Review

- **Spec coverage:** Covers VPP FIFO OOO semantics and capacity accounting, TCP/session/app ownership, Local app wakeups, remote event preservation, receive-window/app backpressure, and SVM configuration mismatch.
- **Placeholder scan:** No `TBD`, `TODO`, or unspecified test commands remain.
- **Type consistency:** New names are consistent across tasks: `SessionRxEnqueue::accepted_len`, `AppWorker::wake_rx/wake_tx/wake_evt`, and `TcpConnection::note_receive_window_from_session`.

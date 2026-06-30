# svm_fifo Alignment + AppSession Unification

> **For agentic workers:** Execute tasks sequentially.

**Goal:** Align `Fifo` (svm_fifo) with VPP semantics and eliminate unnecessary local/remote AppSession distinction.

**Architecture:** Changes span `hammer-infra` (Fifo OOO, chunk refcount, signal cleanup), `hammer-runtime` (Notify → AppWorker, unified constructors), and `hammer-service` (remove SessionRxQueue). No `Svm` segment, no cross-process — only in-process `Local`.

**Tech Stack:** Rust 2024, `hammer-infra::{fifo, rb_tree, pool, segment}`, `hammer-runtime::app`, `tokio::sync::Notify`.

---

## Key Decisions

1. No `Svm`/cross-process — only `Local` segment. `AppWorkerRegistry` stays `AppWorker<Local>`. No `dyn Any`, no generic thread_local. The unification is at the `AppSession<S: Segment>` type level only.
2. `Fifo` OOO uses the same `RbTree<u32, Pool<OooSegment>>` pattern as current `SessionRxQueue`, but operates on byte-offsets within fifo data, not `BufferIndex` references.
3. `MsgQueue::new(cross_process: bool)` → `MsgQueue::new(seg, capacity)` — always `AtomicBool` signal (no pipe fds). Pipes live only in the cross-process attach path which we don't build.
4. `with_capacity` stays as `impl Fifo<Local>` / `impl MsgQueue<Local>` convenience methods — they are harmless and the only segment type in use.

## Files to Modify

| File | Change |
|---|---|
| `crates/hammer-infra/src/fifo.rs` | Chunk refcount + `clear()` fix + `deq_thresh`/`has_deq_ntf` read + OOO support |
| `crates/hammer-infra/src/msg_queue.rs` | Remove `cross_process: bool` from `new()` |
| `crates/hammer-runtime/src/app/session.rs` | Remove `Notify` fields; remove `impl AppSession<Local>`; unify constructors into `impl<S: Segment> AppSession<S>` |
| `crates/hammer-runtime/src/app/application.rs` | `AppWorker` gains `SessionNotify` table; `send_all`/`recv`/`next_event` move here |
| `crates/hammer-runtime/src/app/context.rs` | Simplify (only `Local` segment) |
| `crates/hammer-service/src/session/runtime.rs` | Delete `SessionRxQueue`/`SessionRxBuffer`/`SessionRxEnqueue`; `enqueue_rx` uses `Fifo::enqueue_ooo` |
| `crates/hammer-service/src/session/app.rs` | Adjust to new `AppSession` API |

---

### Task 1: Chunk refcount + clear() fix + deq_thresh/has_deq_ntf

**Files:** `crates/hammer-infra/src/fifo.rs`

- Add `refcount: AtomicU32` to `Chunk` struct, initialize to 1 in all constructors.
- `dequeue_drop`: before freeing a chunk, decrement refcount; free only when refcount reaches 0.
- `clear()`: walk `head_chunk→next` chain, de-ref each chunk (decref + free if 0), reset header fields.
- Add `pub fn deq_threshold(&self) -> u32` — reads the (still-unused) `deq_thresh` field.
- Add `pub fn has_deq_notification(&self) -> bool` — reads `has_deq_ntf` (coalesce check).
- Add `pub fn clear_deq_notification_flag(&self)` — atomically clears the coalesce flag.
- Adjust `FifoHeader` padding if `Chunk` size changes (keep 64-byte cacheline alignment).

### Task 2: OOO support in Fifo

**Files:** `crates/hammer-infra/src/fifo.rs`

- New types:
```rust
struct OooSegment {
    offset: u32,
    len: u32,
    prev: Option<usize>,
    next: Option<usize>,
}

struct OooBookkeeping {
    base: u32,
    entries: Pool<OooSegment>,
    index: RbTree<u32, PoolIndex>,
}
```
- `FifoHeader` gets `ooo_pool_offset: u64` (offset to the `OooHeader` struct in the segment) — only allocated when OOO mode is enabled. `0` means OOO disabled.
- `Fifo<S>` gets `ooo: Option<Box<OooBookkeeping>>>` (lazy, for the in-process `Arc<Fifo<S>>` only).
- `Fifo::enable_ooo(&mut self)` — initializes `OooBookkeeping`.
- `Fifo::enqueue_ooo(&self, offset: u32, src: &[u8]) -> Result<OooResult, ()>`:
  - Calls internal `enqueue_at(offset, src)` to write bytes into chunk chain.
  - Creates `OooSegment` with the absolute offset, inserts into index.
  - Handles predecessor overlap (trim left) and successor overlap (trim right / remove covered).
  - Returns `OooResult { delivered: u32 }` — bytes now contiguous at head.
- `Fifo::promote_contiguous(&self) -> u32`:
  - Checks `ooo_index.first()` offset vs current `head`. If contiguous, advances `head` and removes the segment.
  - Repeats until a gap is found or OOO list is empty.
  - Returns total bytes newly contiguous.
- `Fifo::ooo_head(&self) -> Option<(u32, u32)>` — first OOO entry relative to head.
- `Fifo::ooo_enqueued(&self) -> usize` — number of OOO segments.

Note: `enqueue_ooo` returns `OooResult` not `BufferIndex` — the caller is responsible for freeing buffers that get fully covered during overlap trimming. `Fifo::enqueue_ooo` returns the offset of any covered segment that was dropped; the caller can then look up the associated buffer to free.

### Task 3: Remove MsgQueue cross_process flag

**Files:** `crates/hammer-infra/src/msg_queue.rs`

- `MsgQueue::new(seg: S, capacity: usize)` — removes `cross_process: bool`.
- Always sets `signal_read = None`, `signal_write = None` (AtomicBool only).
- All callers updated: `MsgQueue::new(seg, capacity, false)` → `MsgQueue::new(seg, capacity)`.

### Task 4: Notify out of AppSession → into AppWorker

**Files:** `crates/hammer-runtime/src/app/session.rs`, `crates/hammer-runtime/src/app/application.rs`

#### session.rs changes:
- Remove `rx_readable: Notify`, `tx_writable: Notify`, `evt_readable: Notify` fields from `AppSession<S>`.
- Remove `use tokio::sync::Notify;`
- `enqueue_rx`: remove `self.rx_readable.notify_waiters()` — caller (AppWorker) handles wakeup.
- `drop_tx_acked`: remove `self.tx_writable.notify_waiters()`.
- `push_event`: remove `self.evt_readable.notify_waiters()`.
- `clear`: remove three `notify_waiters()` calls.
- Remove async methods: `send_all`, `recv`, `next_event`.
- All remaining methods stay (`send_bytes`, `recv_bytes`, `consume_rx`, `poll_events`, `enqueue_rx`, `drop_tx_acked`, etc.).

#### application.rs changes:
- Add `use tokio::sync::Notify;`
- New struct `SessionNotify { rx_readable: Notify, tx_writable: Notify, evt_readable: Notify }`
- `AppWorker<S>` gains `notifies: FlatHashTable<u64, SessionNotify>`
- `attach_session()` variants create and insert `SessionNotify`.
- `detach_session()` removes the notify entry.
- New methods:
  - `pub fn notify_rx(&self, handle: SessionHandle) -> &Notify`
  - `pub fn notify_tx(&self, handle: SessionHandle) -> &Notify`
  - `pub fn notify_evt(&self, handle: SessionHandle) -> &Notify`
  - `pub fn wake_rx(&self, session_id: u64)` — sets edge-triggered bits and notifies.
  - `pub fn wake_tx(&self, session_id: u64)`
  - `pub fn wake_evt(&self, session_id: u64)`
- Port `send_all`, `recv`, `next_event` from `AppSession` to `AppWorker<S>`:
  - `AppWorker::send_all(&self, handle: SessionHandle, bytes: &[u8]) -> impl Future<Output = HammerResult<usize>>`
  - `AppWorker::recv(&self, handle: SessionHandle, buf: &mut [u8]) -> impl Future<Output = usize>`
  - `AppWorker::next_event(&self, handle: SessionHandle) -> impl Future<Output = SessionEvt>`
  - These get `session` from `self.sessions`, get notify from `self.notifies`.

### Task 5: Unify AppSession constructors

**Files:** `crates/hammer-runtime/src/app/session.rs`

- Delete `impl AppSession<Local>` block.
- In `impl<S: Segment> AppSession<S>`:
```rust
pub fn new_in_segment(
    seg: S,
    config: AppSessionConfig,
    handle: SessionHandle,
    tx_evt_q: Arc<MsgQueue<S>>,
) -> HammerResult<Self>
```
Uses `Fifo::new(seg.clone(), config.fifo_capacity)` for rx/tx, `MsgQueue::new(seg, ring_size)` for evt_q, and the provided `tx_evt_q`.

- Client code (`AppWorker::attach_session`) now allocates its segment and calls `AppSession::new_in_segment` directly. `local()`/`local_with_runtime_tx()` removed.
- `AppSessionConfig` gains `tx_evt_q_capacity: usize` field (default 64) to let callers control the shared queue or create a per-session one.

### Task 6: Delete SessionRxQueue, use Fifo OOO

**Files:** `crates/hammer-service/src/session/runtime.rs`, `crates/hammer-service/src/session/app.rs`

#### runtime.rs:
- Delete `SessionRxBuffer`, `SessionRxEnqueue`, `SessionRxQueue` types.
- `SessionDriverRuntime` no longer holds `rx: Pool<SessionRxQueue>` or `rx_index: FlatHashTable<u64, PoolIndex>`.
- `enqueue_rx(session_id, index, offset, fin) → CoreResult<()>`:
  - If offset == 0 and rx_fifo has space: fast-path — copy bytes from buffer chain to `session.rx_fifo()`, free buffer.
  - If offset > 0 or rx_fifo full: call `session.rx_fifo.enqueue_ooo(offset, chunk_bytes)`, free buffer after copy.
  - After each enqueue, call `session.rx_fifo.promote_contiguous()` to check if OOO data is now in-order.
- For the OOO path: we need to copy bytes from buffer to the fifo at the given offset. This is the same one-copy model — bytes go into the fifo directly. The `OooBookkeeping` tracks what's where.
- `release_session_rx` can be simplified (no SessionRxQueue to clean up).
- Update `SessionRxEnqueue` return — no longer needed. Return `()`. Update all callers that consume `SessionRxEnqueue` (`established.rs`, `listen.rs`, etc.).

#### app.rs:
- `SessionAppRuntime::copy_rx_from_buffer` — adjusts to write directly to `session.rx_fifo.enqueue_ooo` or `session.rx_fifo.enqueue`.
- `SessionAppRuntime::enqueue_rx` — simplified (no SessionRxQueue layer).

### Task 7: Update callers + tests

**Files:** various

- `established.rs:290` `queue.enqueue_rx(session_id, index, offset, fin)` → adapt to new return type `()` and new semantics.
- `listen.rs:1263`, `syn_sent.rs:231`, `rcv_process.rs:280` — same adaptation.
- `reset.rs` test — update any hardcoded references.
- `session_queue_dispatch.rs`, `session_rx_flush.rs` — update for new API.
- Add: `crates/hammer-infra/tests/fifo_ooo.rs` — test OOO insert, trim, overlap, promote, clear.
- Add: `crates/hammer-runtime/tests/app_worker_notify.rs` — test Notify on AppWorker.

### Task 8: Build + test + commit

- `cargo build -p hammer-infra -p hammer-runtime -p hammer-service`
- `cargo test -p hammer-infra --lib fifo::`
- `cargo test -p hammer-service --lib session:: transport::tcp::`
- `cargo test -p hammer-runtime --lib app::`
- `cargo fmt --all`
- `git add -A && git commit`

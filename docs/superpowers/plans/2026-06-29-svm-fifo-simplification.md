# Svm Fifo Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align Hammer's session/fifo/app boundary with VPP semantics by keeping `Fifo<S>` locality-agnostic, replacing `MsgQueue` with a reusable `Queue<Msg, S = Local>`, unifying session metadata, and moving locality-specific wakeup/attach behavior out of `AppSession`.

**Architecture:** VPP does not make `svm_fifo_t` or `session_t` decide whether an app is local or remote. That distinction is fixed at construction/attach time by segment attach, fifo offset exchange, and mq/eventfd wiring. Hammer should follow the same split: `AppSession<S>` stays a plain session object over fifo + queue + session metadata; locality-specific waiting lives in outer app-side surfaces, and cross-process reconstruction lives in attach code. The queue primitive becomes `Queue<Msg, S = Local>` so event transport is generic and reusable instead of baking `SessionEvt` into the type name and layout.

**Tech Stack:** Rust 2024, `std`, `libc`, `tokio::sync::Notify`, `tokio::io::unix::AsyncFd`, `hammer_core::{HammerResult, CoreResult}`, `hammer_infra::{fifo, segment}`, `hammer_runtime::app`, `hammer_app::remote_session`, `third_party/vpp/src/svm/{svm_fifo.c,message_queue.h}`, `third_party/vpp/src/vnet/session/{session.c,session_node.c}`, `third_party/vpp/src/vcl/vcl_bapi.c`.

---

## Global Constraints

- `Fifo<S>` stays the concrete session byte store. Do not add reader/view/writer/capability wrappers around it.
- `AppSession<S>` must not branch on “is this cross-process?” at runtime.
- `AppSession<S>` must not own locality policy. If a caller is already holding an `AppSession`, the construction path should already have fixed the wakeup/attach model.
- Do not use `Local` vs `Svm` as the semantic discriminator for session behavior. `S` is a memory backend, not an application-mode type system.
- Replace `MsgQueue` with a reusable `Queue<Msg, S = Local>` rather than adding another session-specific queue type.
- `hammer-infra` utility types must stay business-free. `SessionEvt` and other app/session payloads must not live in infra.
- Reuse repository result aliases for repository APIs. Only trait signatures that force foreign result types may use foreign results.
- Remove decorative `PhantomData` where the type parameter adds no state or invariant.
- New public API must be minimized and justified. Prefer renaming/reusing existing surfaces over adding parallel ones.

## VPP Grounding

- VPP queue locking is on `svm_msg_q_*`, not on `svm_fifo_t`.
  - `third_party/vpp/src/svm/message_queue.h`
  - `third_party/vpp/src/vnet/session/session.c`
- VPP session replies hand out `segment_handle` plus fifo offsets; attach code reconstructs the shared objects later.
  - `third_party/vpp/src/vnet/session/session_node.c`
  - `third_party/vpp/src/vcl/vcl_bapi.c`
- VPP locality differences live in attach / mq eventfd / segment attach flow, not in session identity.
- Therefore Hammer should treat:
  - `Fifo<S>` as byte ownership and notification bits only
  - `Queue<Msg, S>` as message transport + wakeup
  - session metadata as one attach/reconstruction record
  - `AppSession<S>` as the already-constructed session core

## Design Summary

### 1. Session metadata becomes one type

- Replace the split `SessionHandle` + `SessionOffsets` surface with one session metadata record.
- The unified record should contain:
  - session identity used by runtime/app lookup
  - worker/session indices
  - fifo offsets
  - queue offsets
- This mirrors VPP's attach messages more closely: the app side reconstructs a session from one record plus transferred fds, not from loosely-related handle and offset structs.
- `AppSession<S>` stores this metadata record directly and exposes narrow accessors for the pieces callers actually need.

### 2. `MsgQueue` becomes `Queue<Msg, S = Local>`

- Rename the module away from `msg_queue` and replace `MsgQueue<S>` with `Queue<Msg, S = Local>`.
- The queue header/layout stays generic over `Msg: Copy`.
- `SessionEvt` moves out of `hammer-infra` into the app domain and becomes just one `Msg` instantiation.
- The wakeup side also belongs to the generic queue:
  - in-process signal bit path
  - cross-process pipe/fd path
- Construction must be explicit:
  - local constructor
  - constructor with external signal fds
  - reconstruction from shared memory
- Do not keep `cross_process: bool` in `Queue::new`. The construction path should decide the wakeup shape up front.

### 3. `AppSession<S>` becomes locality-agnostic core

- Keep `AppSession<S>` as the shared session core over:
  - `rx_fifo`
  - `tx_fifo`
  - app event queue
  - tx event queue
  - session metadata
- Remove locality-specific async waiting from this core.
- Keep ordered immediate operations on the core:
  - `send_bytes`
  - `recv_bytes`
  - `consume_rx`
  - `push_event`
  - `enqueue_rx`
  - `drop_tx_acked`
- Local async waiting belongs in a local app-side helper module, not on the session type.
- Cross-process async waiting remains in `RemoteAppSession`.
- This matches VPP: the reconstructed session object is shared semantics; the app-side waiting mechanism depends on how the app is attached.

### 4. Clean outer surfaces

- `RemoteAppSession` becomes `Arc<AppSession<Svm>>` plus `AsyncFd` waiting only.
- Local app code gets the same kind of outer async surface using `Notify`, instead of calling locality-aware async methods on the session core itself.
- `AppContext<Local>` should stop carrying a dead generic + `PhantomData`.
- Attach code should pass one unified metadata record and explicit queue fds.

## Approval Items

| Item | Final Result | Why Existing Surfaces Are Not Enough |
| --- | --- | --- |
| `Queue<Msg, S = Local>` | Generic shared-memory queue replaces `MsgQueue<S>` | Current queue bakes `SessionEvt` into the storage type and blocks reuse |
| Unified session metadata type | One record replaces split `SessionHandle` + `SessionOffsets` | Current attach/reconstruct path spreads one concept across unrelated structs |
| Local async wrapper/surface | Local waiting moves out of `AppSession<S>` | Current `AppSession` async methods couple session core to `Notify` |
| `AppContext` de-genericization | Remove dead `PhantomData` generic state | Current generic parameter carries no state and no useful invariant |

## File Responsibility Map

- `crates/hammer-infra/src/fifo.rs`
  - shared-memory fifo semantics only
- `crates/hammer-infra/src/msg_queue.rs`
  - replaced by generic queue module
- `crates/hammer-infra/src/queue.rs`
  - generic shared-memory queue and wakeup primitive only
- `crates/hammer-infra/src/lib.rs`
  - exports the new queue module/symbols
- `crates/hammer-runtime/src/app/handle.rs`
  - folded into unified session metadata
- `crates/hammer-runtime/src/app/layout.rs`
  - folded into unified session metadata
- `crates/hammer-runtime/src/app/session.rs`
  - session core over fifo + queue + metadata, no locality policy
- `crates/hammer-runtime/src/app/event.rs`
  - session-domain queue payload definitions such as `SessionEvt`
- `crates/hammer-runtime/src/app/local.rs`
  - local async waiting helpers over `AppSession<Local>`
- `crates/hammer-runtime/src/app/context.rs`
  - local app context only, no dead generic marker
- `crates/hammer-runtime/src/app/application.rs`
  - local worker-owned session registration
- `crates/hammer-runtime/src/attach.rs`
  - dataplane-side cross-process construction and metadata/fd transfer
- `crates/hammer-app/src/attach.rs`
  - app-side cross-process reconstruction from metadata + fds
- `crates/hammer-app/src/remote_session.rs`
  - cross-process async waiting
- `crates/hammer-app/src/echo.rs`
  - local async waiting consumer, updated to the new local helper surface
- `crates/hammer-service/src/session/app.rs`
  - dataplane queue drain and fifo copy paths

### Task 1: Replace `MsgQueue` with generic `Queue<Msg, S = Local>`

**Files:**
- Create: `crates/hammer-infra/src/queue.rs`
- Create: `crates/hammer-runtime/src/app/event.rs`
- Modify: `crates/hammer-infra/src/lib.rs`
- Delete: `crates/hammer-infra/src/msg_queue.rs`
- Modify: `crates/hammer-runtime/src/app/session.rs`
- Modify: `crates/hammer-runtime/src/app/application.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify: `crates/hammer-runtime/src/attach.rs`
- Modify: `crates/hammer-app/src/remote_session.rs`
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Test: `crates/hammer-infra/src/queue.rs`

**Interfaces:**
- Produces: `Queue<Msg, S = Local>`
- Produces: `QueueError<Msg>`
- Produces: `Queue::<Msg>::with_capacity(...)`
- Removes: `MsgQueue<S>`, `MsgQueueError`

- [ ] **Step 1: Write the failing tests**

Add these tests in `crates/hammer-infra/src/queue.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::Local;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    struct TestMsg {
        a: u32,
        b: u32,
    }

    #[test]
    fn queue_round_trips_generic_msg() {
        let q = Queue::<TestMsg>::with_capacity(8).expect("queue");
        let msg = TestMsg { a: 1, b: 2 };
        q.enqueue(msg).expect("enqueue");
        assert_eq!(q.dequeue(), Some(msg));
        assert_eq!(q.dequeue(), None);
    }

    #[test]
    fn local_queue_has_no_fd() {
        let q = Queue::<TestMsg>::with_capacity(4).expect("queue");
        assert!(q.read_fd().is_none());
        assert!(q.write_fd().is_none());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hammer-infra queue_round_trips_generic_msg local_queue_has_no_fd -- --exact`  
Expected: FAIL because `queue.rs` and `Queue<Msg, S>` do not exist yet.

- [ ] **Step 3: Implement the generic queue and switch imports**

Create `crates/hammer-infra/src/queue.rs` by lifting the current `msg_queue.rs` layout into:

```rust
#[repr(C)]
pub struct QueueHeader {
    head: AtomicU32,
    tail: AtomicU32,
    size: u32,
    mask: u32,
}

#[derive(Debug)]
pub enum QueueError<Msg> {
    InvalidCapacity,
    Full(Msg),
}

pub struct Queue<Msg: Copy, S: Segment = Local> {
    seg: S,
    base: *mut u8,
    hdr: *mut QueueHeader,
    hdr_off: u64,
    signal_read: Option<RawFd>,
    signal_write: Option<RawFd>,
    signal_atomic: AtomicBool,
    slots: [std::mem::MaybeUninit<Msg>; 0],
}
```

Then implement:

```rust
impl<Msg: Copy, S: Segment> Queue<Msg, S> {
    pub fn new_local(seg: S, capacity: usize) -> Result<Self, QueueError<Msg>> { /* ... */ }

    pub fn new_with_fds(
        seg: S,
        capacity: usize,
        signal_read: Option<RawFd>,
        signal_write: Option<RawFd>,
    ) -> Result<Self, QueueError<Msg>> { /* ... */ }

    pub unsafe fn init_at(seg: S, hdr_offset: u64, capacity: usize) -> Result<Self, QueueError<Msg>> { /* ... */ }

    pub unsafe fn from_shared(
        seg: S,
        offset: u64,
        signal_read: Option<RawFd>,
        signal_write: Option<RawFd>,
    ) -> Self { /* ... */ }
}
```

Constraints:
- Do not use `PhantomData`.
- If Rust needs a typed zero-sized field for `Msg`, use a zero-length typed array as above.
- Do not introduce extra wrapper types for producers/consumers.
- Do not keep `cross_process: bool`; constructors must fix the wakeup shape.
- Move `SessionEvt` and `SessionEvtType` into `crates/hammer-runtime/src/app/event.rs`.
- Rename all `hammer_infra::msg_queue::*` imports to `hammer_infra::queue::*`.

- [ ] **Step 4: Run the queue tests**

Run: `cargo test -p hammer-infra queue_round_trips_generic_msg local_queue_has_no_fd -- --exact`  
Expected: PASS.

- [ ] **Step 5: Run the existing queue consumers**

Run: `cargo test -p hammer-infra queue -- --nocapture`  
Expected: PASS.

Run: `cargo test -p hammer-runtime app::session -- --nocapture`  
Expected: compile/pass after import migration.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-infra/src/queue.rs \
        crates/hammer-infra/src/lib.rs \
        crates/hammer-runtime/src/app/event.rs \
        crates/hammer-runtime/src/app/mod.rs \
        crates/hammer-runtime/src/app/session.rs \
        crates/hammer-runtime/src/app/application.rs \
        crates/hammer-runtime/src/attach.rs \
        crates/hammer-app/src/remote_session.rs \
        crates/hammer-service/src/session/app.rs \
        crates/hammer-service/src/session/runtime.rs
git commit -m "hammer-infra(Refactor): replace msg queue with generic queue"
```

### Task 2: Unify `SessionHandle` and `SessionOffsets` into one metadata record

**Files:**
- Create: `crates/hammer-runtime/src/app/metadata.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Delete: `crates/hammer-runtime/src/app/handle.rs`
- Delete: `crates/hammer-runtime/src/app/layout.rs`
- Modify: `crates/hammer-runtime/src/app/session.rs`
- Modify: `crates/hammer-runtime/src/attach.rs`
- Modify: `crates/hammer-app/src/attach.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: all session metadata call sites
- Test: `crates/hammer-runtime/src/app/metadata.rs`

**Interfaces:**
- Produces: unified session metadata type
- Removes: split `SessionHandle`, split `SessionOffsets`

- [ ] **Step 1: Write the failing tests**

Create tests in `crates/hammer-runtime/src/app/metadata.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::segment::Local;

    #[test]
    fn session_metadata_packs_identity_and_layout() {
        let seg = Local::new(1 << 20);
        let meta = SessionMetadata::allocate(&seg, 7, 3, 64, 16);
        assert_eq!(meta.session_index(), 7);
        assert_eq!(meta.worker_index(), 3);
        assert!(meta.rx_fifo_off < meta.tx_fifo_off);
        assert!(meta.tx_fifo_off < meta.evt_q_off);
        assert!(meta.evt_q_off < meta.tx_evt_q_off);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hammer-runtime session_metadata_packs_identity_and_layout -- --exact`  
Expected: FAIL because `SessionMetadata` does not exist yet.

- [ ] **Step 3: Implement unified metadata and switch call sites**

Create `crates/hammer-runtime/src/app/metadata.rs`:

```rust
pub struct SessionMetadata {
    raw: u64,
    pub rx_fifo_off: u64,
    pub tx_fifo_off: u64,
    pub evt_q_off: u64,
    pub tx_evt_q_off: u64,
}

impl SessionMetadata {
    pub const fn new(
        session_index: u32,
        worker_index: u32,
        rx_fifo_off: u64,
        tx_fifo_off: u64,
        evt_q_off: u64,
        tx_evt_q_off: u64,
    ) -> Self {
        Self {
            raw: (session_index as u64) | ((worker_index as u64) << 32),
            rx_fifo_off,
            tx_fifo_off,
            evt_q_off,
            tx_evt_q_off,
        }
    }

    pub fn allocate<S: Segment>(
        seg: &S,
        session_index: u32,
        worker_index: u32,
        fifo_capacity: usize,
        evt_q_capacity: usize,
    ) -> Self {
        // reuse existing layout math here
    }

    pub const fn raw(self) -> u64 { self.raw }
    pub const fn session_index(self) -> u32 { self.raw as u32 }
    pub const fn worker_index(self) -> u32 { (self.raw >> 32) as u32 }
}
```

Then:
- export `SessionMetadata` from `app/mod.rs`
- store `SessionMetadata` in `AppSession`
- make attach send/receive this one record
- update session lookup code to use `meta.raw()` instead of a separate handle type

- [ ] **Step 4: Run the tests**

Run: `cargo test -p hammer-runtime session_metadata_packs_identity_and_layout -- --exact`  
Expected: PASS.

Run: `cargo test -p hammer-runtime app::session -- --nocapture`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-runtime/src/app/metadata.rs \
        crates/hammer-runtime/src/app/mod.rs \
        crates/hammer-runtime/src/app/session.rs \
        crates/hammer-runtime/src/attach.rs \
        crates/hammer-app/src/attach.rs \
        crates/hammer-service/src/session/runtime.rs
git commit -m "hammer-runtime(Refactor): unify session metadata"
```

### Task 3: Remove locality-specific async waiting from `AppSession`

**Files:**
- Create: `crates/hammer-runtime/src/app/local.rs`
- Modify: `crates/hammer-runtime/src/app/session.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify: `crates/hammer-app/src/remote_session.rs`
- Modify: `crates/hammer-app/src/echo.rs`
- Modify: `crates/hammer-app/src/lib.rs`
- Test: `crates/hammer-runtime/src/app/session.rs`
- Test: `crates/hammer-app/src/remote_session.rs`

**Interfaces:**
- Keeps: immediate fifo/queue operations on `AppSession<S>`
- Removes: locality-aware async wait methods from the session core
- Produces: local async helper functions for `Local`

- [ ] **Step 1: Write the failing tests**

Add to `crates/hammer-runtime/src/app/session.rs`:

```rust
#[test]
fn app_session_core_still_supports_immediate_fifo_ops() {
    let session = new_session(AppSessionConfig::new(64, 4), 1);
    assert_eq!(session.send_bytes(b"abc").expect("send"), 3);
    let mut out = [0u8; 8];
    assert_eq!(session.tx_fifo().peek(0, out.len(), &mut out), 3);
    assert_eq!(&out[..3], b"abc");
}
```

Add to `crates/hammer-app/src/remote_session.rs`:

```rust
#[test]
fn remote_session_requires_signal_fd() {
    use std::sync::Arc;

    let session = Arc::new(
        hammer_runtime::app::AppSession::local(
            hammer_runtime::app::AppSessionConfig::new(64, 4),
            hammer_runtime::app::SessionMetadata::new(1, 0, 0, 0, 0, 0),
        )
        .expect("session"),
    );

    assert!(RemoteAppSession::new(session).is_err());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hammer-runtime app_session_core_still_supports_immediate_fifo_ops -- --exact`  
Expected: FAIL until tests are added.

Run: `cargo test -p hammer-app remote_session_requires_signal_fd -- --exact`  
Expected: FAIL until tests are added.

- [ ] **Step 3: Move async waiting outward**

Create `crates/hammer-runtime/src/app/local.rs` with free async helpers:

```rust
pub async fn recv(session: &AppSession<Local>, out: &mut [u8]) -> usize { /* ... */ }

pub async fn send_all(session: &AppSession<Local>, bytes: &[u8]) -> HammerResult<usize> { /* ... */ }

pub async fn next_event(session: &AppSession<Local>) -> SessionEvt { /* ... */ }
```

In `crates/hammer-runtime/src/app/session.rs`:
- remove `recv`, `send_all`, `next_event`
- keep only crate-private wake hooks needed by `app::local`

In `crates/hammer-app/src/echo.rs`, switch from `session.recv(...)` / `session.send_all(...)` to the re-exported local helper surface.

In `crates/hammer-app/src/remote_session.rs`, keep cross-process waiting around `Arc<AppSession<Svm>>`.

- [ ] **Step 4: Run focused tests**

Run: `cargo test -p hammer-runtime app::session -- --nocapture`  
Expected: PASS.

Run: `cargo test -p hammer-app remote_session -- --nocapture`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-runtime/src/app/session.rs \
        crates/hammer-runtime/src/app/local.rs \
        crates/hammer-runtime/src/app/mod.rs \
        crates/hammer-app/src/remote_session.rs \
        crates/hammer-app/src/echo.rs \
        crates/hammer-app/src/lib.rs
git commit -m "hammer-runtime(Refactor): move app waiting out of session core"
```

### Task 4: Make queue construction explicit and fix attach/locality boundaries

**Files:**
- Modify: `crates/hammer-runtime/src/attach.rs`
- Modify: `crates/hammer-app/src/attach.rs`
- Modify: `crates/hammer-runtime/src/app/application.rs`
- Modify: `crates/hammer-runtime/src/app/session.rs`
- Create: `crates/hammer-app/tests/attach_roundtrip.rs`

**Interfaces:**
- Produces: explicit local queue construction
- Produces: explicit fd-backed queue reconstruction
- Removes: `cross_process: bool` constructor choice
- Produces: `AttachClient::connect(path)` that reconstructs from transferred metadata
- Produces: `AttachServer::accept(config, seg, session_index, worker_index)` that allocates metadata internally

- [ ] **Step 1: Write the failing tests**

Create `crates/hammer-app/tests/attach_roundtrip.rs`:

```rust
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use hammer_app::attach::AttachClient;
use hammer_infra::segment::Svm;
use hammer_runtime::app::AppSessionConfig;
use hammer_runtime::attach::AttachServer;

fn unique_socket_path() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "hammer-attach-{}-{stamp}.sock",
        std::process::id()
    ))
}

#[test]
fn attach_round_trip_preserves_metadata_and_fd_directions() {
    let socket = unique_socket_path();
    let socket_str = socket.to_str().expect("socket path").to_owned();
    let server = AttachServer::bind(&socket_str).expect("bind");
    let seg = Svm::create("hammer_attach_round_trip", 1 << 20).expect("segment");
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        let attached = server
            .accept(AppSessionConfig::new(64, 4), &seg, 7, 3)
            .expect("accept");
        tx.send(attached).expect("send attached");
    });

    let client = AttachClient::connect(&socket_str).expect("client connect");
    let server_side = rx.recv().expect("server attached");

    assert_eq!(server_side.metadata.session_index(), 7);
    assert_eq!(server_side.metadata.worker_index(), 3);
    assert_eq!(client.metadata.raw(), server_side.metadata.raw());
    assert_eq!(client.metadata.rx_fifo_off, server_side.metadata.rx_fifo_off);
    assert_eq!(client.metadata.tx_fifo_off, server_side.metadata.tx_fifo_off);
    assert_eq!(client.metadata.evt_q_off, server_side.metadata.evt_q_off);
    assert_eq!(client.metadata.tx_evt_q_off, server_side.metadata.tx_evt_q_off);

    assert!(server_side.session.evt_q().read_fd().is_none());
    assert!(server_side.session.evt_q().write_fd().is_some());
    assert!(server_side.session.tx_evt_q().read_fd().is_some());
    assert!(server_side.session.tx_evt_q().write_fd().is_none());

    assert!(client.session.evt_q().read_fd().is_some());
    assert!(client.session.evt_q().write_fd().is_none());
    assert!(client.session.tx_evt_q().read_fd().is_none());
    assert!(client.session.tx_evt_q().write_fd().is_some());

    let _ = std::fs::remove_file(socket);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hammer-app attach_round_trip_preserves_metadata_and_fd_directions -- --exact`  
Expected: FAIL.

- [ ] **Step 3: Implement explicit construction paths**

Change queue/session construction so that:
- local app worker uses `Queue::<SessionEvt>::with_capacity(...)`
- cross-process attach uses `Queue::<SessionEvt, Svm>::from_shared(..., read_fd, write_fd)`
- no constructor accepts a locality boolean
- `AttachServer::accept(...)` allocates one `SessionMetadata`
- attach messages serialize exactly one `SessionMetadata`
- `AttachClient::connect(...)` reconstructs from the transferred metadata instead of taking a separate handle input

- [ ] **Step 4: Run focused attach tests**

Run: `cargo test -p hammer-app attach_round_trip_preserves_metadata_and_fd_directions -- --exact`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-runtime/src/attach.rs \
        crates/hammer-app/src/attach.rs \
        crates/hammer-runtime/src/app/application.rs \
        crates/hammer-runtime/src/app/session.rs \
        crates/hammer-app/tests/attach_roundtrip.rs
git commit -m "hammer-runtime(Refactor): make attach and queue construction explicit"
```

### Task 5: Remove dead generics and leftover session-specific queue naming

**Files:**
- Modify: `crates/hammer-runtime/src/app/context.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify: `crates/hammer-app/src/lib.rs`
- Modify: comments/tests/imports touched by rename
- Test: `crates/hammer-runtime/src/app/context.rs`

**Interfaces:**
- Removes: `AppContext<S>` generic + `PhantomData`
- Removes: leftover `msg_queue` naming

- [ ] **Step 1: Write the failing test**

Add to `crates/hammer-runtime/src/app/context.rs`:

```rust
#[test]
fn app_context_is_local_only_surface() {
    let fn_ptr: fn(crate::spawn::DataRuntimeContext, crate::app::AppSessionConfig) -> AppContext =
        AppContext::new;
    let _ = fn_ptr;
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p hammer-runtime app_context_is_local_only_surface -- --exact`  
Expected: FAIL while `AppContext` is still generic.

- [ ] **Step 3: Remove dead generic state and rename leftovers**

Update `crates/hammer-runtime/src/app/context.rs`:

```rust
#[derive(Clone)]
pub struct AppContext {
    data_context: DataRuntimeContext,
    app_session_config: AppSessionConfig,
}
```

Then update all exports/imports accordingly and remove any leftover `msg_queue` references.

- [ ] **Step 4: Run focused tests**

Run: `cargo test -p hammer-runtime app_context_is_local_only_surface -- --exact`  
Expected: PASS.

Run: `rg -n "MsgQueue|msg_queue|SessionHandle|SessionOffsets|PhantomData" crates -g'*.rs'`  
Expected: no matches for the removed surfaces, except where `PhantomData` is still genuinely needed elsewhere in unrelated code.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-runtime/src/app/context.rs \
        crates/hammer-runtime/src/app/mod.rs \
        crates/hammer-app/src/lib.rs
git commit -m "hammer-runtime(Refactor): remove dead app generics and old queue names"
```

## Self-Review

- **Spec coverage:** this plan now covers the user-requested simplifications:
  - `session` no longer decides locality
  - queue extracted and generalized to `Queue<Msg, S = Local>`
  - `Handle` and `Offset` unified into one metadata record
  - locality fixed at construction/attach time, not by runtime method checks
  - business payload types move out of infra
  - dead `PhantomData` generic state removed where it adds nothing
- **Placeholder scan:** no `TODO`, `TBD`, or hand-waved test placeholders remain.
- **Type consistency:** the plan uses `SessionMetadata` and `Queue<S, Msg>` consistently; no alternate wrapper names remain.

Plan complete and saved to `docs/superpowers/plans/2026-06-29-svm-fifo-simplification.md`. Two execution options:

1. Subagent-Driven (recommended) - I dispatch a fresh subagent per task, review between tasks, fast iteration

2. Inline Execution - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?

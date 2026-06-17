# TCP Module 1 Session TX And App Send Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Module 1 so app `Send` SQEs become session-owned TX work, are copied into existing packet buffers by the generic session runtime, receive transport headers from TCP, and are emitted through the assembly-provided TCP output next.

**Architecture:** Follow VPP's session/transport split. Hammer session runtime owns app TX scheduling, packet-buffer allocation, payload copy, frame enqueue, and rescheduling; TCP owns only transport capacity and established segment header mutation. Data structures come from `hammer-infra` by default; if infra is missing a small primitive, add it to `hammer-infra` instead of falling back to `std`.

**Tech Stack:** Rust 2024, `hammer-infra`, `hammer-runtime` app ring/data area, `hammer-service` session runtime and typed TCP connections, `hammer-adapter` packet buffers and frames, VPP `session_node.c`, VPP `session.c`, VPP `transport.h`, VPP `tcp.c`, and VPP `tcp_output.c`.

---

## Scope Check

This plan implements only normal established TCP payload TX from app `Send`. It does not implement FIN close sends, app send completion CQEs, retransmit payload storage, SACK, delayed ACK, congestion-control expansion, RACK/TLP, persist, keepalive, or TIME-WAIT work.

## VPP Reference Mapping

VPP keeps app data in session-owned FIFOs and lets the session node schedule transport output. Hammer maps that to app ring descriptors plus session-owned TX progress entries:

```text
App SQE Send
  -> SessionDriverRuntime::poll_app drains app descriptors
  -> SessionAppRuntime stores a private SessionAppTxProgress
  -> SessionDriverRuntime marks affected sessions ready
  -> SessionDriverRuntime flushes ready session TX
  -> SessionAppRuntime::has_pending_send(session_id)
  -> SessionQueueProtocol::transport_tx_capacity(state, session_id)
  -> DataPlaneBuffers::alloc_index / get_buffer_mut / append
  -> SessionQueueProtocol::write_transport_segment_header(state, index, payload_len, now)
  -> SessionQueueOutput enqueue using SessionQueueNext supplied by service/TCP assembly
  -> SessionQueueProtocol::commit_transport_tx(state, update)
  -> SessionAppRuntime::commit_pending_send_bytes(session_id, payload_len)
  -> TcpOutputNode routes the packet onward
```

## Layer Isolation Contract

This module must be isolated by Rust visibility and trait boundaries, not by convention.

| Layer / file area | May depend on | Must not depend on | Only crossing API |
| --- | --- | --- | --- |
| `hammer-infra` | nothing project-domain-specific | app/session/TCP/dataplane business types | generic containers/utilities only |
| `hammer-runtime/src/app` | `hammer-core`, `hammer-infra`, app data/ring internals | `hammer-service`, session IDs, TCP, `DataPlaneBuffers` TX enqueue policy, graph output frames | `AppSendData::len`, `AppSendData::copy_range`, `TryFrom<AppSend>` |
| `hammer-service/src/session/app.rs` | app ring/data transfer types, `SessionId`, `hammer-infra` collections | TCP modules, output nodes, `SessionQueueOutput`, transport state-machine types | `SessionAppRuntime` behavior methods only |
| `hammer-service/src/session/runtime.rs` | session app runtime, packet buffers, generic `SessionQueueProtocol` trait | concrete TCP modules, TCP output node, app ring internals beyond `SessionAppRuntime` | `SessionQueueProtocol::{transport_tx_capacity, write_transport_segment_header, commit_transport_tx}` |
| `hammer-service/src/session/protocol.rs` | narrow session control context over state/timers/buffers | app runtime internals, app send progress, ready/timer drain ownership, concrete transport modules | `SessionQueueControlContext` |
| `hammer-service/src/session/node.rs` | session queue handles, dispatch fn, `SessionQueueOutput` | app ring internals, concrete TCP state-machine internals | `SessionQueueDispatchFn` |
| `hammer-service/src/transport/tcp/state_machine.rs` | TCP packet/header primitives, TCP connection state, adapter buffer type only for header fill | app/session runtime, `SessionId`, `SessionQueueOutput`, app ring/data transfer types | typed TCP methods on `TcpConnection<Established, C>` |
| `hammer-service/src/transport/tcp/session.rs` | TCP state machine, session protocol trait, session IDs | `SessionAppTxProgress`, app ring internals, output scheduling ownership | implements `SessionQueueProtocol<TcpServiceConnectionState>` |
| `hammer-service/src/service.rs` | node assembly types | app/TCP private internals | graph assembly such as `SessionQueueNext::from_node(tcp_output)` |

Visibility requirements:

- `SessionAppTxProgress` is private to `session/app.rs`; no `pub`, no `pub(crate)`, no re-export.
- `SessionTransportTxCapacity` and `SessionQueueProtocol` are `pub(crate)` because they are service-internal extension points, not public API. Transport-specific `TxUpdate` associated types stay private to their transport module.
- TCP established TX helpers are typed methods on `TcpConnection<Established, C>`; session code cannot inspect erased TCP state for app TX decisions.
- Session runtime calls TCP only through `SessionQueueProtocol`; TCP does not call app TX APIs.
- `SessionQueueOutput` is owned by `session/node.rs` in production and by session runtime test helpers in tests; TCP session code may receive `&mut SessionQueueOutput` but must not construct or schedule it.
- Transport protocol methods must not receive `&mut SessionDriverRuntime<S>`. Session runtime owns app state, buffers, timers, and the session map; transport receives only `&S`/`&mut S`, explicit buffer handles, and explicit scalar inputs.
- Timer/control hooks receive `SessionQueueControlContext`, not `SessionDriverRuntime`. This context must not expose `app`, `app_mut`, pending sends, ready-session drains, timer-expiry drains, session removal, or direct entry-map mutation.
- Transport sequence state must not advance while only preparing a packet. `write_transport_segment_header` returns the protocol's opaque `TxUpdate`; `commit_transport_tx` applies it only after output enqueue succeeds.

## Non-Negotiable Constraints

- Do not add app-layer packet-buffer append helpers. App data may expose checked reads; session TX must call `DataPlaneBuffers` allocation and append APIs directly.
- Do not expose buffer-owned APIs upward merely for convenience. If a helper belongs to packet buffers, keep it on the buffer side.
- Do not keep app send progress as an unnamed tuple. The private session-app type is `SessionAppTxProgress`, because it names the business state: one session-owned app TX progress record over one `AppSendData`.
- Do not store `AppOpId` in `SessionAppTxProgress` until send completion CQEs are implemented. The app op is used only to map an SQE descriptor to a `SessionId` in this module.
- Do not expose `SessionAppTxProgress` from `session/app.rs`. Upper layers call behavior methods on `SessionAppRuntime`.
- Do not expose `AppSendData::data` publicly. App send internals may offer checked behavior such as `len` and `copy_range`; callers outside `hammer-runtime::app` must not depend on `AppDataAddr` for app-send progress.
- Do not name business state as `Cursor`, `Helper`, `Util`, or similar tool nouns. Use those only for generic utility types such as `Cursor<T>`, and prefer Rust standard-library or `hammer-infra` utilities before adding one.
- Do not add TCP-owned app write queues. There is no TCP app-send queue.
- Do not make generic session code mention TCP, `TcpOutputNode`, `tcp_output`, or `transport::tcp`.
- Do not put app ring types or session runtime types in `transport/tcp/state_machine.rs`.
- Do not re-export private app TX progress or transport implementation details through `session/mod.rs`.
- Do not create `SessionQueueOutput::default()` or call `.schedule(runtime)` from `transport/tcp/session.rs`; output ownership is in `session/node.rs` and test helpers.
- Use `TryFrom<AppSend> for AppSendData` for ownership transfer.
- Do not use `std::vec::Vec<u8>` for the app/session TX byte path. Add missing infra APIs and return `hammer_infra::vec::Vec<u8>`.
- Do not introduce underscore-prefixed variable names such as `_value`. If a parameter or pattern slot must exist but is intentionally unused, use the bare `_` pattern. If a local binding is unused, delete it and the work that produced it.

## File Responsibility Map

- Modify `crates/hammer-infra/src/vec.rs` and `crates/hammer-infra/tests/vec.rs`
  - Add and test a small initialized-vector constructor needed by app-data reads.

- Modify `crates/hammer-runtime/src/app/data.rs`
  - Add checked `AppDataAddr + Range<usize>` subrange selection.
  - Keep write/release validation strict.
  - Add read-only subrange validation.
  - Return `hammer_infra::vec::Vec<u8>` from `AppDataArea::read`.

- Modify `crates/hammer-runtime/src/app/ring.rs`
  - Replace crate-private send transfer with `TryFrom<AppSend> for AppSendData`.
  - Keep `AppSendData::data` private.
  - Add `AppSendData::len`.
  - Add `AppSendData::copy_range` returning `hammer_infra::vec::Vec<u8>`.
  - Update existing app read methods that expose copied bytes to return infra Vec.

- Modify `crates/hammer-runtime/src/app/context.rs`
  - Use `try_into()?` for app-send transfer.
  - Return infra Vec from `read_data`.

- Modify `crates/hammer-runtime/src/app/mod.rs`
  - Re-export `AppSendData`.

- Modify `crates/hammer-app/src/ring.rs` and `crates/hammer-app/src/lib.rs`
  - Keep façade `read_data` returning `std::vec::Vec<u8>` for external compatibility by converting from runtime infra Vec at this explicit boundary.

- Modify `crates/hammer-service/src/session/app.rs`
  - Delete the old app send submission type.
  - Add private `SessionAppTxProgress`; it must be `struct SessionAppTxProgress`, not `pub` or `pub(crate)`.
  - Store pending sends as `hammer_infra::vec::Vec<SessionAppTxProgress>`.
  - Expose behavior methods on `SessionAppRuntime`: push, copy, commit, inspect by `SessionId`.

- Modify `crates/hammer-service/src/session/mod.rs`
  - Re-export only `SessionAppRuntime` publicly.
  - Keep close submissions crate-local.
  - Do not re-export `SessionAppTxProgress`, `SessionTransportTxCapacity`, or transport provider details.

- Modify `crates/hammer-service/src/session/protocol.rs`
  - Split queue-time control access into `SessionQueueControlContext`.
  - Keep app-ring binding APIs only on the connection/open path context where they are already required.
  - Remove raw `app`/`app_mut` access from protocol contexts; transports must not receive `SessionAppRuntime`.
  - Do not expose app TX or app runtime access from queue dispatch hooks.

- Modify `crates/hammer-service/src/session/runtime.rs`
  - Add generic transport TX capacity/header methods to `SessionQueueProtocol`.
  - Add `SessionTransportTxCapacity`.
  - Add a `SessionQueueProtocol::TxUpdate` associated type for transport-private commit state.
  - Add `SessionDriverRuntime::poll_app`.
  - Add generic TX flushing using existing `DataPlaneBuffers`.

- Modify `crates/hammer-service/src/session/node.rs`
  - Let the node own one `SessionQueueOutput` per process call and schedule it once.

- Modify `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - Add `TcpEstablishedTxCapacity`.
  - Add typed established-payload header writing that reuses `hammer_core::protocol::tcp::write_tcp_segment_header` directly.

- Modify `crates/hammer-service/src/transport/tcp/session.rs`
  - Implement generic transport TX hooks for TCP.
  - Keep timer/control output separate from app payload TX.

## Task 1: Infra Vec And App Data Reads

**Files:**
- Modify: `crates/hammer-infra/src/vec.rs`
- Modify: `crates/hammer-infra/tests/vec.rs`
- Modify: `crates/hammer-runtime/src/app/data.rs`
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-runtime/src/app/context.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify: `crates/hammer-app/src/ring.rs`
- Modify: `crates/hammer-app/src/lib.rs`

- [ ] **Step 1: Add failing infra and app tests**

Add this test to `crates/hammer-infra/tests/vec.rs`:

```rust
#[test]
fn from_elem_copy_initializes_aligned_vec() {
    let values = Vec::from_elem_copy(4, 7_u8);

    assert_eq!(values.as_slice(), &[7, 7, 7, 7]);
    assert_eq!(values.len(), 4);
}
```

Add these tests to the existing or new `#[cfg(test)] mod tests` in `crates/hammer-runtime/src/app/ring.rs`:

```rust
#[test]
fn app_send_into_app_send_data_moves_ownership() {
    let ring = AppRingHandle::with_data_area(4, 4, 256, 4).expect("ring");
    let data = ring.alloc_data_for_bytes(b"abcdef").expect("data");
    let transfer: AppSendData = ring
        .send_from_data(data)
        .try_into()
        .expect("transfer");

    assert_eq!(transfer.len().expect("len"), 6);
    let copied = transfer.copy_range(0, 6).expect("copy");
    assert_eq!(copied.as_slice(), b"abcdef");

    transfer.release();
}

#[test]
fn app_send_data_copies_checked_range_as_infra_vec() {
    let ring = AppRingHandle::with_data_area(4, 4, 256, 4).expect("ring");
    let data = ring.alloc_data_for_bytes(b"abcdefgh").expect("data");
    let transfer: AppSendData = ring
        .send_from_data(data)
        .try_into()
        .expect("transfer");

    let copied = transfer.copy_range(2, 4).expect("copy");

    assert_eq!(copied.as_slice(), b"cdef");
    assert!(transfer.copy_range(9, 1).is_err());

    transfer.release();
}
```

Add this app-data range test to the existing or new `#[cfg(test)] mod tests` in `crates/hammer-runtime/src/app/data.rs`:

```rust
#[test]
fn app_data_addr_subrange_checks_bounds() {
    let addr = AppDataAddr::new(2, 3, 128, 16, 64);

    let selected = (addr + (2..6)).expect("range");

    assert_eq!(selected.chunk(), 2);
    assert_eq!(selected.generation(), 3);
    assert_eq!(selected.offset(), 130);
    assert_eq!(selected.len(), 4);
    assert_eq!(selected.capacity(), 64);
    assert!((addr + (6..2)).is_err());
    assert!((addr + (0..17)).is_err());
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test -p hammer-infra from_elem_copy_initializes_aligned_vec
cargo test -p hammer-runtime app_send_
```

Expected: fail because `Vec::from_elem_copy`, `AppSendData` export, `TryFrom<AppSend>`, `AppDataAddr + Range<usize>`, `AppSendData::len`, and `AppSendData::copy_range` are missing.

- [ ] **Step 3: Add the missing infra Vec API**

In `crates/hammer-infra/src/vec.rs`, add this method inside `impl<T, const ALIGN: usize> RawVec<T, ALIGN>` after `with_capacity`:

```rust
#[inline]
pub fn from_elem_copy(len: usize, value: T) -> Self
where
    T: Copy,
{
    let mut out = Self::with_capacity(len);
    for _ in 0..len {
        out.push(value);
    }
    out
}
```

- [ ] **Step 4: Add checked subrange selection**

In `crates/hammer-runtime/src/app/data.rs`, delete the `std::vec::Vec` alias import.

Add:

```rust
use std::ops::{Add, Range};
```

Add after `impl AppDataAddr`:

```rust
impl Add<Range<usize>> for AppDataAddr {
    type Output = HammerResult<Self>;

    #[inline]
    fn add(self, range: Range<usize>) -> Self::Output {
        if range.start > range.end {
            return Err(HammerError::internal("app data range start exceeds end"));
        }
        if range.end > self.len() {
            return Err(HammerError::internal("app data range exceeds length"));
        }
        let offset = u32::try_from(range.start)
            .map_err(|_| HammerError::internal("app data range offset exceeds u32"))?;
        let len = u32::try_from(range.end - range.start)
            .map_err(|_| HammerError::internal("app data range length exceeds u32"))?;
        let next_offset = self
            .offset
            .checked_add(offset)
            .ok_or_else(|| HammerError::internal("app data range offset overflow"))?;
        Ok(Self {
            offset: next_offset,
            len,
            ..self
        })
    }
}
```

- [ ] **Step 5: Add read-only validation and infra Vec reads**

In `crates/hammer-runtime/src/app/data.rs`, add this helper below `validate`:

```rust
fn validate_read_range(&self, addr: AppDataAddr) -> HammerResult<usize> {
    let Some(chunk) = self.chunks.get(addr.chunk() as usize) else {
        return Err(HammerError::internal("app data chunk index out of range"));
    };
    if !chunk.in_use.load(Ordering::Acquire) {
        return Err(HammerError::internal("app data chunk is not allocated"));
    }
    if chunk.generation.load(Ordering::Acquire) != addr.generation() {
        return Err(HammerError::internal("app data chunk generation is stale"));
    }
    if addr.capacity() != self.chunk_size {
        return Err(HammerError::internal("app data chunk capacity mismatch"));
    }
    let chunk_start = addr.chunk() as usize * self.chunk_size;
    let chunk_end = chunk_start
        .checked_add(self.chunk_size)
        .ok_or_else(|| HammerError::internal("app data chunk range overflow"))?;
    if addr.offset() < chunk_start || addr.offset() > chunk_end {
        return Err(HammerError::internal("app data chunk offset mismatch"));
    }
    if addr.offset().saturating_add(addr.len()) > chunk_end {
        return Err(HammerError::internal("app data chunk range out of bounds"));
    }
    if chunk_end > self.storage.len() {
        return Err(HammerError::internal("app data chunk range out of bounds"));
    }
    let published_end = chunk_start
        .checked_add(chunk.len.load(Ordering::Acquire) as usize)
        .ok_or_else(|| HammerError::internal("app data published range overflow"))?;
    let start = addr.offset();
    if start > published_end {
        return Err(HammerError::internal("app data range starts after published length"));
    }
    Ok(addr.len().min(published_end - start))
}
```

Replace `AppDataArea::read` with:

```rust
pub fn read(&self, addr: AppDataAddr) -> HammerResult<Vec<u8>> {
    let len = self.validate_read_range(addr)?;
    let start = addr.offset();
    let mut out = Vec::from_elem_copy(len, 0_u8);
    unsafe {
        ptr::copy_nonoverlapping(self.storage.as_ptr().add(start), out.as_mut_ptr(), len);
    }
    Ok(out)
}
```

- [ ] **Step 6: Convert app read surfaces to infra Vec**

In `crates/hammer-runtime/src/app/ring.rs`, change these signatures:

```rust
pub fn copy_current(&self) -> HammerResult<hammer_infra::vec::Vec<u8>>
pub fn read_data(&self, addr: AppDataAddr) -> HammerResult<hammer_infra::vec::Vec<u8>>
pub fn copy_current(&self) -> HammerResult<hammer_infra::vec::Vec<u8>>
```

In `crates/hammer-runtime/src/app/context.rs`, change:

```rust
pub fn read_data(&self, data: AppDataAddr) -> HammerResult<hammer_infra::vec::Vec<u8>> {
    self.ring.read_data(data)
}
```

In `crates/hammer-app/src/ring.rs`, keep the façade return type as `std::vec::Vec<u8>` and convert explicitly:

```rust
pub fn read_data(&self, data: AppDataAddr) -> HammerResult<std::vec::Vec<u8>> {
    Ok(self.inner.read_data(data)?.as_slice().to_vec())
}
```

In `crates/hammer-app/src/lib.rs`, keep the façade return type as `std::vec::Vec<u8>` and convert explicitly:

```rust
pub fn read_data(&self, data: AppDataAddr) -> HammerResult<std::vec::Vec<u8>> {
    Ok(self.inner.read_data(data)?.as_slice().to_vec())
}
```

- [ ] **Step 7: Replace app send transfer helper with `TryFrom`**

In `crates/hammer-runtime/src/app/ring.rs`, delete the crate-private `AppSend::into_transfer_data` method and add after `impl Drop for AppSend`:

```rust
impl TryFrom<AppSend> for AppSendData {
    type Error = HammerError;

    #[inline]
    fn try_from(send: AppSend) -> HammerResult<Self> {
        let mut send = send;
        match send
            .payload
            .take()
            .ok_or_else(|| HammerError::internal("app send released"))?
        {
            AppSendPayload::Data { data, ring } => Ok(ring.send_data_from_addr(data)),
        }
    }
}
```

In `crates/hammer-runtime/src/app/context.rs`, add `AppSendData` to the ring import and replace:

```rust
let send = send.into_transfer_data()?;
```

with:

```rust
let send: AppSendData = send.try_into()?;
```

- [ ] **Step 8: Add app-send length and checked copy without exposing data**

In `impl AppSendData` in `crates/hammer-runtime/src/app/ring.rs`, keep `data` private and add `len` plus `copy_range`:

```rust
#[inline]
fn data(&self) -> HammerResult<AppDataAddr> {
    self.data
        .ok_or_else(|| HammerError::internal("app send data released"))
}

#[inline]
pub fn len(&self) -> HammerResult<usize> {
    Ok(self.data()?.len())
}

#[inline]
pub fn copy_range(&self, offset: usize, len: usize) -> HammerResult<Vec<u8>> {
    let data = self.data()?;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| HammerError::internal("app send data range overflow"))?;
    self.data_area.read((data + (offset..end))?)
}
```

Keep `data_area(&self)` private.

- [ ] **Step 9: Re-export `AppSendData`**

In `crates/hammer-runtime/src/app/mod.rs`, add `AppSendData` to the `pub use ring::{ ... }` list:

```rust
AppSend, AppSendData, AppSqe, AppSqeData,
```

- [ ] **Step 10: Run tests and commit**

Run:

```bash
cargo test -p hammer-infra from_elem_copy_initializes_aligned_vec
cargo test -p hammer-runtime app_send_
cargo test -p hammer-runtime app_data_addr_subrange_checks_bounds
cargo test -p hammer-app
```

Expected: pass.

Then run:

```bash
git add crates/hammer-infra/src/vec.rs crates/hammer-infra/tests/vec.rs crates/hammer-runtime/src/app/data.rs crates/hammer-runtime/src/app/ring.rs crates/hammer-runtime/src/app/context.rs crates/hammer-runtime/src/app/mod.rs crates/hammer-app/src/ring.rs crates/hammer-app/src/lib.rs
git commit -m "hammer-runtime(Feat): add infra-backed app send ranges"
```

Expected: commit succeeds.

## Task 2: Session App TX Progress

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`

- [ ] **Step 1: Add failing session-app tests**

Add this test module to the bottom of `crates/hammer-service/src/session/app.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use hammer_runtime::app::{AppRingHandle, AppSendData};

    #[test]
    fn pending_send_progress_is_committed_by_session_id() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let first: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"first").expect("first"))
            .try_into()
            .expect("first transfer");
        let second: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"second").expect("second"))
            .try_into()
            .expect("second transfer");

        let mut app = SessionAppRuntime::new();
        let session_a = SessionId::new(10);
        let session_b = SessionId::new(20);

        app.push_pending_send(session_a, first);
        app.push_pending_send(session_b, second);

        let first_bytes = app
            .copy_pending_send_bytes(session_a, 8)
            .expect("copy first")
            .expect("first pending");
        assert_eq!(first_bytes.as_slice(), b"first");
        assert!(app.commit_pending_send_bytes(session_a, 5).expect("commit first"));
        assert!(!app.has_pending_send(session_a));
        assert!(app.has_pending_send(session_b));
    }

    #[test]
    fn unfinished_send_tracks_progress_without_exposing_entry() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"abcdef").expect("data"))
            .try_into()
            .expect("transfer");
        let mut app = SessionAppRuntime::new();
        let session_id = SessionId::new(1);

        app.push_pending_send(session_id, send);

        let first = app
            .copy_pending_send_bytes(session_id, 4)
            .expect("copy first")
            .expect("pending");
        assert_eq!(first.as_slice(), b"abcd");
        assert!(!app.commit_pending_send_bytes(session_id, 4).expect("partial"));

        let second = app
            .copy_pending_send_bytes(session_id, 8)
            .expect("copy second")
            .expect("pending after partial");
        assert_eq!(second.as_slice(), b"ef");
        assert!(app.commit_pending_send_bytes(session_id, 2).expect("finish"));
        assert!(!app.has_pending_send(session_id));
    }
}
```

- [ ] **Step 2: Run failing session-app tests**

Run:

```bash
cargo test -p hammer-service session::app::tests::pending_send_progress_is_committed_by_session_id session::app::tests::unfinished_send_tracks_progress_without_exposing_entry
```

Expected: fail because pending app TX behavior methods and the private progress entry do not exist yet.

- [ ] **Step 3: Add the private progress entry and pending storage**

In `crates/hammer-service/src/session/app.rs`, change imports to:

```rust
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::map::FlatHashTable;
use hammer_runtime::app::{
    AppCqe, AppObjectRef, AppOpId, AppOpcode, AppRingHandle, AppSendData, AppSqeData,
    AppSqeDescriptor,
};
```

Delete the old send-submission struct. Add:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionAppCloseSubmission {
    session_id: SessionId,
    op: AppOpId,
}

impl SessionAppCloseSubmission {
    #[inline]
    pub(crate) const fn new(session_id: SessionId, op: AppOpId) -> Self {
        Self { session_id, op }
    }

    #[inline]
    pub(crate) const fn session_id(self) -> SessionId {
        self.session_id
    }

    #[inline]
    pub(crate) const fn op(self) -> AppOpId {
        self.op
    }
}

#[derive(Debug)]
struct SessionAppTxProgress {
    session_id: SessionId,
    send: AppSendData,
    sent_len: usize,
}

impl SessionAppTxProgress {
    #[inline]
    fn new(session_id: SessionId, send: AppSendData) -> Self {
        Self {
            session_id,
            send,
            sent_len: 0,
        }
    }

    #[inline]
    const fn session_id(&self) -> SessionId {
        self.session_id
    }

    #[inline]
    fn remaining_len(&self) -> CoreResult<usize> {
        let total = self.send.len()?;
        Ok(total.saturating_sub(self.sent_len))
    }

    #[inline]
    fn copy_pending_bytes(&self, max_len: usize) -> CoreResult<hammer_infra::vec::Vec<u8>> {
        let len = self.remaining_len()?.min(max_len);
        self.send.copy_range(self.sent_len, len)
    }

    #[inline]
    fn commit_bytes(&mut self, len: usize) -> CoreResult<bool> {
        let remaining = self.remaining_len()?;
        if len > remaining {
            return Err(CoreError::internal(
                "session app tx commit exceeds remaining length",
            ));
        }
        self.sent_len += len;
        Ok(self.sent_len >= self.send.len()?)
    }

    #[inline]
    fn finish(self) {
        self.send.release();
    }
}
```

In `SessionAppRuntime`, replace the drained-send field with:

```rust
pending_sends: hammer_infra::vec::Vec<SessionAppTxProgress>,
```

In `SessionAppRuntime::new`, initialize it with:

```rust
pending_sends: hammer_infra::vec::Vec::new(),
```

- [ ] **Step 4: Add behavior APIs on `SessionAppRuntime`**

Delete the old drained-send accessor and add:

```rust
#[inline]
pub(crate) fn push_pending_send(
    &mut self,
    session_id: SessionId,
    send: AppSendData,
) {
    self.pending_sends
        .push(SessionAppTxProgress::new(session_id, send));
}

pub(crate) fn copy_pending_send_bytes(
    &self,
    session_id: SessionId,
    max_len: usize,
) -> CoreResult<Option<hammer_infra::vec::Vec<u8>>> {
    let Some(send) = self
        .pending_sends
        .iter()
        .find(|send| send.session_id() == session_id)
    else {
        return Ok(None);
    };
    Ok(Some(send.copy_pending_bytes(max_len)?))
}

pub(crate) fn commit_pending_send_bytes(
    &mut self,
    session_id: SessionId,
    len: usize,
) -> CoreResult<bool> {
    let index = self
        .pending_sends
        .iter()
        .position(|send| send.session_id() == session_id)
        .ok_or_else(|| CoreError::internal("session app tx progress is missing"))?;
    let completed = {
        let send = &mut self.pending_sends.as_mut_slice()[index];
        send.commit_bytes(len)?
    };
    if completed {
        let send = self.pending_sends.remove(index);
        send.finish();
    }
    Ok(completed)
}

#[inline]
pub(crate) fn has_pending_send(&self, session_id: SessionId) -> bool {
    self.pending_sends
        .iter()
        .any(|send| send.session_id() == session_id)
}

pub(crate) fn pending_send_session_ids(&self, out: &mut hammer_infra::vec::Vec<SessionId>) {
    for send in &self.pending_sends {
        if !out.iter().any(|session_id| *session_id == send.session_id()) {
            out.push(send.session_id());
        }
    }
}

#[inline]
pub(crate) fn pending_closes(&self) -> &[SessionAppCloseSubmission] {
    self.drained_closes.as_slice()
}
```

- [ ] **Step 5: Convert app ring descriptors into session behavior records**

In `handle_submission_descriptor`, replace the `AppOpcode::Send` and `AppOpcode::Close` arms with:

```rust
AppOpcode::Send => {
    if let AppSqeData::Send { data } = descriptor.payload() {
        let op = app_op_from_descriptor(descriptor);
        let Some(session_id) = self.session_for_op(op) else {
            return Ok(());
        };
        if let Some(ring) = self.ring.clone() {
            let send: AppSendData = ring.send_from_data(data).try_into()?;
            self.push_pending_send(session_id, send);
        }
    }
}
AppOpcode::Close => {
    let op = app_op_from_descriptor(descriptor);
    if let Some(session_id) = self.session_for_op(op) {
        self.drained_closes
            .push(SessionAppCloseSubmission::new(session_id, op));
    }
}
```

- [ ] **Step 6: Poll app from the generic session driver**

In `crates/hammer-service/src/session/runtime.rs`, add this method to `impl<S> SessionDriverRuntime<S>`:

```rust
pub(crate) fn poll_app(&mut self) -> CoreResult<()> {
    self.app.drain_submissions()?;
    let mut ready = hammer_infra::vec::Vec::new();
    self.app.pending_send_session_ids(&mut ready);
    for close in self.app.pending_closes() {
        if !ready.iter().any(|session_id| *session_id == close.session_id()) {
            ready.push(close.session_id());
        }
    }
    for session_id in ready {
        self.mark_ready(session_id);
    }
    Ok(())
}
```

- [ ] **Step 7: Remove send-submission exports and TCP forwarding helpers**

In `crates/hammer-service/src/session/mod.rs`, replace the app re-export with:

```rust
pub use app::SessionAppRuntime;
pub(crate) use app::SessionAppCloseSubmission;
```

In `crates/hammer-service/src/transport/tcp/session.rs`, remove the send-submission import and delete TCP protocol forwarding helpers for drained app sends. Also delete any TCP helper that reaches into the app runtime through a protocol context to take drained sends or closes; close handling must go through explicit session-app behavior methods from tests or session runtime, not through a transport-owned app accessor.

- [ ] **Step 8: Run tests and commit**

Run:

```bash
cargo test -p hammer-service session::app::tests::pending_send_progress_is_committed_by_session_id session::app::tests::unfinished_send_tracks_progress_without_exposing_entry
```

Expected: pass.

Then run:

```bash
git add crates/hammer-service/src/session/app.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/src/session/mod.rs crates/hammer-service/src/transport/tcp/session.rs
git commit -m "hammer-service(Feat): track session app tx progress"
```

Expected: commit succeeds.

## Task 3: Generic Session TX Loop

**Files:**
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/node.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`

- [ ] **Step 1: Add failing generic TX tests**

Add these imports inside `#[cfg(test)] mod tests` in `crates/hammer-service/src/session/runtime.rs`:

```rust
use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataPlaneRuntime, NodeId, RouteMetadata};
use hammer_core::error::CoreError;
use hammer_runtime::app::{AppRingHandle, AppSendData};

use crate::session::SessionQueueControlContext;
```

Add this fake protocol and tests inside the same module:

```rust
#[derive(Debug, Clone)]
struct FakeTxState {
    metadata: RouteMetadata,
    prepared: usize,
    committed: usize,
}

#[derive(Debug, Clone, Copy)]
struct FakeTxUpdate {
    payload_len: usize,
}

struct FakeTxProtocol;

impl SessionQueueProtocol<FakeTxState> for FakeTxProtocol {
    type TxUpdate = FakeTxUpdate;

    fn handle_timer_expiry(
        &mut self,
        context: &mut SessionQueueControlContext<'_, FakeTxState>,
        expiry: SessionTimerExpiry,
    ) -> CoreResult<()> {
        context.mark_ready(expiry.session_id());
        Ok(())
    }

    fn handle_ready_session(
        &mut self,
        _: &DataPlaneRuntime,
        _: &mut SessionQueueControlContext<'_, FakeTxState>,
        _: SessionId,
        _: crate::session::SessionQueueNext,
        _: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<()> {
        Ok(())
    }

    fn transport_tx_capacity(
        &mut self,
        state: &FakeTxState,
        _: SessionId,
    ) -> CoreResult<SessionTransportTxCapacity> {
        Ok(SessionTransportTxCapacity {
            metadata: state.metadata.clone(),
            header_len: 8,
            payload_budget: 4,
            should_reschedule: true,
        })
    }

    fn write_transport_segment_header(
        &mut self,
        state: &FakeTxState,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
        payload_len: usize,
        _: Instant,
    ) -> CoreResult<Self::TxUpdate> {
        {
            let mut buffer = buffers.get_buffer_mut(index)?;
            buffer.current_mut()[..8].copy_from_slice(b"fakehdr!");
        }
        assert_eq!(state.prepared, 0);
        Ok(FakeTxUpdate { payload_len })
    }

    fn commit_transport_tx(
        &mut self,
        state: &mut FakeTxState,
        update: Self::TxUpdate,
    ) -> CoreResult<()> {
        state.prepared += update.payload_len;
        state.committed += update.payload_len;
        Ok(())
    }
}

#[test]
fn session_tx_copies_app_send_writes_header_and_keeps_remainder() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let buffers = runtime.packet_buffers();
    let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), buffers.clone());
    let session_id = driver.insert_session(FakeTxState {
        metadata: RouteMetadata::default(),
        prepared: 0,
        committed: 0,
    });
    let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
    let send: AppSendData = ring
        .send_from_data(ring.alloc_data_for_bytes(b"abcdef").expect("data"))
        .try_into()
        .expect("transfer");
    driver.app_mut().push_pending_send(session_id, send);
    driver.mark_ready(session_id);

    let mut protocol = FakeTxProtocol;
    let next = crate::session::SessionQueueNext::from_node(NodeId::new(9));
    let mut output = crate::session::node::SessionQueueOutput::default();
    let mut step = driver.poll_once_for_ticks(0).expect("poll");

    dispatch_session_queue_pending(
        &runtime,
        &mut driver,
        &mut protocol,
        next,
        &mut output,
        &mut step,
        Instant::now(),
    )
    .expect("dispatch");

    let state = driver.session_state(session_id).expect("state");
    assert_eq!(state.prepared, 4);
    assert_eq!(state.committed, 4);
    assert!(driver.app().has_pending_send(session_id));
    let remaining = driver
        .app()
        .copy_pending_send_bytes(session_id, 8)
        .expect("copy remaining")
        .expect("remaining pending");
    assert_eq!(remaining.as_slice(), b"ef");
}

#[test]
fn session_tx_does_not_call_transport_when_app_has_no_pending_send() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let buffers = runtime.packet_buffers();
    let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), buffers);
    let session_id = driver.insert_session(FakeTxState {
        metadata: RouteMetadata::default(),
        prepared: 0,
        committed: 0,
    });
    driver.mark_ready(session_id);

    let mut protocol = FakeTxProtocol;
    let next = crate::session::SessionQueueNext::from_node(NodeId::new(9));
    let mut output = crate::session::node::SessionQueueOutput::default();
    let mut step = driver.poll_once_for_ticks(0).expect("poll");

    dispatch_session_queue_pending(
        &runtime,
        &mut driver,
        &mut protocol,
        next,
        &mut output,
        &mut step,
        Instant::now(),
    )
    .expect("dispatch without app tx");

    let state = driver.session_state(session_id).expect("state");
    assert_eq!(state.prepared, 0);
    assert_eq!(state.committed, 0);
}
```

- [ ] **Step 2: Run failing generic TX tests**

Run:

```bash
cargo test -p hammer-service session::runtime::tests::session_tx_copies_app_send_writes_header_and_keeps_remainder session::runtime::tests::session_tx_does_not_call_transport_when_app_has_no_pending_send
```

Expected: fail because generic transport TX protocol methods, caller-owned output dispatch, no-pending-send short-circuit, and TX flushing do not exist.

- [ ] **Step 3: Make session node own output scheduling**

In `crates/hammer-service/src/session/node.rs`, change `SessionQueueDispatchFn` to:

```rust
pub type SessionQueueDispatchFn = fn(
    &DataPlaneRuntime,
    SessionQueueHandle,
    SessionQueueNext,
    Instant,
    &mut SessionQueueOutput,
) -> CoreResult<()>;
```

Replace `session_queue_node_process` with:

```rust
fn session_queue_node_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    frame.clear();
    let slot = data.usize_word(0)?;
    let attachments = SESSION_QUEUE_NODES.with(|nodes| {
        let nodes = nodes
            .try_borrow()
            .map_err(|_| CoreError::internal("session queue nodes borrowed"))?;
        let node = nodes
            .get(slot)
            .ok_or_else(|| CoreError::internal("session queue node slot is invalid"))?;
        Ok::<_, CoreError>(node.clone())
    })?;
    let now = Instant::now();
    let mut output = SessionQueueOutput::default();
    for attachment in attachments {
        (attachment.dispatch)(
            runtime,
            attachment.handle,
            attachment.output_next,
            now,
            &mut output,
        )?;
    }
    output.schedule(runtime)?;
    Ok(NodeResult::drop())
}
```

- [ ] **Step 4: Add a narrow queue control context**

In `crates/hammer-service/src/session/protocol.rs`, add this context next to the existing `SessionProtocolContext`:

```rust
pub struct SessionQueueControlContext<'a, S> {
    driver: &'a mut SessionDriverRuntime<S>,
}

impl<'a, S> SessionQueueControlContext<'a, S> {
    #[inline]
    pub(crate) fn new(driver: &'a mut SessionDriverRuntime<S>) -> Self {
        Self { driver }
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.driver.worker()
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: SessionId) {
        self.driver.mark_ready(session_id);
    }

    #[inline]
    pub fn arm_timer_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.driver.arm_timer_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        self.driver.cancel_timer(session_id, token)
    }

    #[inline]
    pub fn session_state(&self, session_id: SessionId) -> Option<&S> {
        self.driver.session_state(session_id)
    }

    #[inline]
    pub fn session_state_mut(&mut self, session_id: SessionId) -> Option<&mut S> {
        self.driver.session_state_mut(session_id)
    }

    #[inline]
    pub fn buffers(&self) -> &DataPlaneBuffers {
        self.driver.buffers()
    }
}
```

Do not add `app`, `app_mut`, `take_ready_sessions`, `take_timer_expiries`, `remove_session`, `close_session`, `session_mut`, or `replace_session_state` to this context. Those capabilities belong to session runtime orchestration or connection/open paths, not queue-time transport callbacks.

In the existing `SessionProtocolContext<'a, S>` impl in the same file, delete these methods:

```rust
pub fn app(&self) -> &SessionAppRuntime
pub fn app_mut(&mut self) -> &mut SessionAppRuntime
```

After deleting those methods, remove `SessionAppRuntime` from the `use crate::session::{ ... }` import list in `session/protocol.rs`.

In `crates/hammer-service/src/session/mod.rs`, add the re-export:

```rust
pub use protocol::{SessionProtocolContext, SessionQueueControlContext};
```

- [ ] **Step 5: Extend `SessionQueueProtocol` with narrow TX hooks**

In `crates/hammer-service/src/session/runtime.rs`, add:

```rust
#[derive(Debug, Clone)]
pub(crate) struct SessionTransportTxCapacity {
    pub(crate) metadata: hammer_adapter::RouteMetadata,
    pub(crate) header_len: usize,
    pub(crate) payload_budget: usize,
    pub(crate) should_reschedule: bool,
}
```

Change `SessionQueueProtocol<S>` to include an associated transport update and narrow TX methods:

```rust
pub(crate) trait SessionQueueProtocol<S> {
    type TxUpdate;

    fn handle_timer_expiry(
        &mut self,
        context: &mut crate::session::SessionQueueControlContext<'_, S>,
        expiry: SessionTimerExpiry,
    ) -> CoreResult<()>;

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::SessionQueueControlContext<'_, S>,
        session_id: SessionId,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<()>;

    fn transport_tx_capacity(
        &mut self,
        state: &S,
        session_id: SessionId,
    ) -> CoreResult<SessionTransportTxCapacity>;

    fn write_transport_segment_header(
        &mut self,
        state: &S,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<Self::TxUpdate>;

    fn commit_transport_tx(&mut self, state: &mut S, update: Self::TxUpdate) -> CoreResult<()>;
}
```

`Self::TxUpdate` is deliberately opaque to the session layer. It exists only to let transport prepare headers without mutating sequence state until output enqueue succeeds.

- [ ] **Step 6: Add generic session TX flushing**

In `crates/hammer-service/src/session/runtime.rs`, import `CoreError`:

```rust
use hammer_core::error::{CoreError, CoreResult};
```

Add this helper above `dispatch_session_queue_pending`:

```rust
fn flush_one_session_tx<S, P>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S>,
    protocol: &mut P,
    session_id: SessionId,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
    now: Instant,
) -> CoreResult<bool>
where
    P: SessionQueueProtocol<S>,
{
    if !driver.app().has_pending_send(session_id) {
        return Ok(false);
    }
    let state = driver
        .session_state(session_id)
        .ok_or_else(|| CoreError::internal("session tx state is missing"))?;
    let capacity = protocol.transport_tx_capacity(state, session_id)?;
    if capacity.payload_budget == 0 {
        return Ok(false);
    }
    let Some(payload) = driver
        .app()
        .copy_pending_send_bytes(session_id, capacity.payload_budget)?
    else {
        return Ok(false);
    };
    if payload.is_empty() {
        let completed = driver.app_mut().commit_pending_send_bytes(session_id, 0)?;
        return Ok(completed);
    }

    let index = driver.buffers().alloc_index(capacity.metadata.clone())?;
    let reserved = (|| {
        let mut buffer = driver.buffers().get_buffer_mut(index)?;
        let tail = buffer.writable_tail_mut();
        if tail.len() < capacity.header_len {
            return Err(CoreError::internal("transport header prefix exceeds buffer capacity"));
        }
        tail[..capacity.header_len].fill(0);
        buffer.commit_writable_tail(capacity.header_len)
    })();
    if let Err(err) = reserved {
        driver.buffers().free_index(index);
        return Err(err);
    }

    match driver.buffers().current_len(index) {
        Ok(len) if len == capacity.header_len => {}
        Ok(_) => {
            driver.buffers().free_index(index);
            return Err(CoreError::internal("packet append offset does not match header length"));
        }
        Err(err) => {
            driver.buffers().free_index(index);
            return Err(err);
        }
    }
    if let Err(err) = driver.buffers().append(index, payload.as_slice()) {
        driver.buffers().free_index(index);
        return Err(err);
    }
    let payload_len = payload.len();

    let update = {
        let state = driver
            .session_state(session_id)
            .ok_or_else(|| CoreError::internal("session tx state is missing"))?;
        match protocol.write_transport_segment_header(
            state,
            driver.buffers(),
            index,
            payload_len,
            now,
        ) {
            Ok(update) => update,
            Err(err) => {
                driver.buffers().free_index(index);
                return Err(err);
            }
        }
    };

    if let Err(err) = output.enqueue(runtime, output_next.node(), index) {
        driver.buffers().free_index(index);
        return Err(err);
    }

    {
        let state = driver
            .session_state_mut(session_id)
            .ok_or_else(|| CoreError::internal("session tx state is missing"))?;
        protocol.commit_transport_tx(state, update)?;
    }
    let completed = driver
        .app_mut()
        .commit_pending_send_bytes(session_id, payload_len)?;
    if !completed && capacity.should_reschedule {
        driver.mark_ready(session_id);
    }
    Ok(true)
}
```

This ordering is the transaction boundary for normal TX: no transport sequence state and no app progress advances until output enqueue succeeds. App progress is committed only after enqueue and transport sequence update both succeed. The only mutation before enqueue is packet-buffer content on a buffer index that is freed on error.

- [ ] **Step 7: Add complete dispatch helper bodies**

Replace `dispatch_session_queue_for_ticks` with:

```rust
#[cfg(test)]
pub(crate) fn dispatch_session_queue_for_ticks<S, P>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S>,
    protocol: &mut P,
    timer_ticks: u32,
    output_next: crate::session::SessionQueueNext,
) -> CoreResult<SessionQueueStep>
where
    P: SessionQueueProtocol<S>,
{
    let mut step = driver.poll_once_for_ticks(timer_ticks)?;
    let now = Instant::now();
    let mut output = crate::session::node::SessionQueueOutput::default();
    dispatch_session_queue_pending(
        runtime,
        driver,
        protocol,
        output_next,
        &mut output,
        &mut step,
        now,
    )?;
    output.schedule(runtime)?;
    Ok(step)
}
```

Replace `dispatch_session_queue_once_at` with:

```rust
pub(crate) fn dispatch_session_queue_once_at<S, P>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S>,
    protocol: &mut P,
    now: Instant,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<SessionQueueStep>
where
    P: SessionQueueProtocol<S>,
{
    let mut step = driver.poll_once_at(now)?;
    dispatch_session_queue_pending(
        runtime,
        driver,
        protocol,
        output_next,
        output,
        &mut step,
        now,
    )?;
    Ok(step)
}
```

Replace `dispatch_session_queue_pending` with:

```rust
fn dispatch_session_queue_pending<S, P>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S>,
    protocol: &mut P,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
    step: &mut SessionQueueStep,
    now: Instant,
) -> CoreResult<()>
where
    P: SessionQueueProtocol<S>,
{
    driver.poll_app()?;
    let expiries = driver.take_timer_expiries();
    for expiry in expiries {
        let mut context = crate::session::SessionQueueControlContext::new(driver);
        protocol.handle_timer_expiry(&mut context, expiry)?;
    }
    let ready_sessions = driver.take_ready_sessions();
    step.ready_sessions = ready_sessions.len();
    for session_id in ready_sessions {
        while flush_one_session_tx(runtime, driver, protocol, session_id, output_next, output, now)? {}
        let mut context = crate::session::SessionQueueControlContext::new(driver);
        protocol.handle_ready_session(runtime, &mut context, session_id, output_next, output)?;
    }
    Ok(())
}
```

- [ ] **Step 8: Update TCP queue dispatch signatures with complete code**

In `crates/hammer-service/src/transport/tcp/session.rs`, replace `TcpSessionQueue::dispatch_once_at` with:

```rust
pub(crate) fn dispatch_once_at(
    &mut self,
    runtime: &DataPlaneRuntime,
    now: Instant,
    output_next: SessionQueueNext,
    output: &mut SessionQueueOutput,
) -> CoreResult<()> {
    dispatch_session_queue_once_at(
        runtime,
        &mut self.driver,
        &mut self.protocol,
        now,
        output_next,
        output,
    )?;
    Ok(())
}
```

Replace `tcp_session_queue_dispatch` with:

```rust
fn tcp_session_queue_dispatch(
    runtime: &DataPlaneRuntime,
    handle: SessionQueueHandle,
    output_next: SessionQueueNext,
    now: Instant,
    output: &mut SessionQueueOutput,
) -> CoreResult<()> {
    TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
        queue.dispatch_once_at(runtime, now, output_next, output)?;
        Ok(())
    })
}
```

Keep `TcpSessionProtocol::session_queue_dispatch_fn` as:

```rust
#[inline]
pub fn session_queue_dispatch_fn() -> SessionQueueDispatchFn {
    tcp_session_queue_dispatch
}
```

- [ ] **Step 9: Remove app drain from TCP ready handling**

In TCP `handle_ready_session`, remove this block entirely:

```rust
if driver
    .session(session_id)
    .and_then(|entry| entry.app_op())
    .is_none()
{
    return Ok(());
}
driver.app_mut().drain_submissions()
```

App polling is generic session runtime work. Leave TCP timer/control packet handling intact.

- [ ] **Step 10: Run tests and commit**

Run:

```bash
cargo test -p hammer-service session::runtime::tests::session_tx_copies_app_send_writes_header_and_keeps_remainder session::runtime::tests::session_tx_does_not_call_transport_when_app_has_no_pending_send
```

Expected: pass.

Then run:

```bash
git add crates/hammer-service/src/session/protocol.rs crates/hammer-service/src/session/runtime.rs crates/hammer-service/src/session/node.rs crates/hammer-service/src/transport/tcp/session.rs
git commit -m "hammer-service(Feat): add generic session tx flushing"
```

Expected: commit succeeds.

## Task 4: TCP Established TX Provider

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`

- [ ] **Step 1: Add failing TCP app-send output test**

In `crates/hammer-service/src/transport/tcp/session.rs`, update test imports to include `AppSendData` but no session-app progress type:

```rust
use hammer_runtime::app::{
    AppCqeKind, AppRingHandle, AppSendData, AppSqe, AppUserData,
};
```

Add this test inside the existing TCP session test module:

```rust
#[test]
fn established_app_send_reaches_tcp_output_lookup_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let worker = DataWorkerId::new(0);
    let local: SocketAddr = "127.0.0.1:10000".parse().expect("local");
    let remote: SocketAddr = "127.0.0.1:20000".parse().expect("remote");
    let connection = TcpServiceConnectionState::established_for_test(
        Some(TcpConnectionId::new(7)),
        worker,
        local.port(),
        Some(local),
        remote,
    );
    let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
    let session_id = queue.insert_session(connection);
    remember_established(&mut queue, session_id).expect("remember established");

    let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
    let send: AppSendData = ring
        .send_from_data(ring.alloc_data_for_bytes(b"hello").expect("data"))
        .try_into()
        .expect("transfer");
    queue
        .driver
        .app_mut()
        .push_pending_send(session_id, send);
    queue.mark_session_ready(session_id);

    let handle = TcpSessionProtocol::register_queue_for_test(queue).expect("register queue");
    let capture = Arc::new(Mutex::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&capture)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::new(Mutex::new(CaptureState::default()))));
    let tcp_output = runtime
        .nodes()
        .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, lookup)));
    let queue_driver = SessionQueueNode::new().expect("session queue node");
    queue_driver
        .attach_queue(
            handle,
            SessionQueueNext::from_node(tcp_output),
            TcpSessionProtocol::session_queue_dispatch_fn(),
        )
        .expect("attach tcp queue");
    let session_queue = runtime.nodes().register_driver(queue_driver);

    runtime
        .schedule_empty_frame(session_queue)
        .expect("schedule session queue");
    assert_eq!(runtime.run_ready_nodes().expect("run session and tcp output"), 3);

    let packets = &capture.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
    let segment = TcpSegmentView::parse(&packets[0]).expect("tcp segment");
    assert_eq!(segment.source_port(), local.port());
    assert_eq!(segment.destination_port(), remote.port());
    assert!(segment.flags().contains(TcpSegmentFlags::ACK));
    assert!(segment.flags().contains(TcpSegmentFlags::PSH));
    assert_eq!(&packets[0][segment.header_len()..], b"hello");
}
```

- [ ] **Step 2: Run failing TCP test**

Run:

```bash
cargo test -p hammer-service transport::tcp::session::tests::established_app_send_reaches_tcp_output_lookup_next
```

Expected: fail because TCP does not implement generic transport TX hooks and cannot write established payload headers into an existing reserved prefix.

- [ ] **Step 3: Add typed established TX capacity**

In `crates/hammer-service/src/transport/tcp/state_machine.rs`, add near the state marker structs:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpEstablishedTxCapacity {
    pub(crate) header_len: usize,
    pub(crate) payload_budget: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpEstablishedTxUpdate {
    pub(crate) payload_len: u32,
}
```

Add to `impl<C> TcpConnection<Established, C> where C: CongestionController`:

```rust
pub(crate) fn established_tx_capacity(&self) -> TcpEstablishedTxCapacity {
    let mss = self.output_payload_len();
    let in_flight = self.snd_nxt.wrapping_sub(self.snd_una);
    let window = self.snd_wnd.saturating_sub(in_flight) as usize;
    TcpEstablishedTxCapacity {
        header_len: 20,
        payload_budget: mss.min(window),
    }
}
```

- [ ] **Step 4: Add typed established payload segment preparation and commit**

In `crates/hammer-service/src/transport/tcp/state_machine.rs`, import:

```rust
use std::time::{Duration, Instant};

use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataWorkerId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::write_tcp_segment_header;
```

Keep the existing segment import focused on TCP packet parsing:

```rust
use super::segment::TcpPacket;
```

Add to the established impl:

```rust
pub(crate) fn write_established_payload_segment_header(
    &self,
    buffers: &DataPlaneBuffers,
    index: BufferIndex,
    payload_len: usize,
    _: Instant,
) -> CoreResult<TcpEstablishedTxUpdate> {
    let payload_len = u32::try_from(payload_len)
        .map_err(|_| CoreError::internal("tcp payload length exceeds u32"))?;
    let local = self
        .local()
        .ok_or_else(|| CoreError::internal("established tcp connection missing local address"))?;
    let header = TcpSegmentHeader {
        source_port: local.port(),
        destination_port: self.remote().port(),
        sequence_number: self.snd_nxt(),
        acknowledgment_number: self.rcv_nxt(),
        flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
        advertised_window: self.advertised_receive_window(self.rcv_wnd),
        capabilities: self.local_capabilities(),
    };
    let mut buffer = buffers.get_buffer_mut(index)?;
    if buffer.current_len() < 20 {
        return Err(CoreError::internal("tcp header prefix exceeds current buffer length"));
    }
    let written = write_tcp_segment_header(&mut buffer.current_mut()[..20], header)?;
    if written != 20 {
        return Err(CoreError::internal("tcp header length changed after prefix reservation"));
    }
    Ok(TcpEstablishedTxUpdate { payload_len })
}

pub(crate) fn commit_established_payload_segment(
    &mut self,
    update: TcpEstablishedTxUpdate,
) {
    self.snd_nxt = TcpSeq::new(self.snd_nxt).advance(update.payload_len).raw();
}
```

- [ ] **Step 5: Implement TCP transport TX hooks**

In `crates/hammer-service/src/transport/tcp/session.rs`, import `SessionTransportTxCapacity` and `TcpEstablishedTxUpdate`:

```rust
use crate::session::runtime::{
    SessionDriverRuntime, SessionEntry, SessionQueueProtocol, SessionStateFactory,
    SessionTransportTxCapacity, dispatch_session_queue_once_at,
};
use crate::transport::tcp::state_machine::TcpEstablishedTxUpdate;
```

Update `handle_timer_expiry`, update `handle_ready_session`, and add the TX methods in `impl SessionQueueProtocol<TcpServiceConnectionState> for TcpSessionProtocol`:

```rust
type TxUpdate = TcpEstablishedTxUpdate;

fn handle_timer_expiry(
    &mut self,
    context: &mut crate::session::SessionQueueControlContext<'_, TcpServiceConnectionState>,
    expiry: SessionTimerExpiry,
) -> CoreResult<()> {
    let Some(kind) = Self::timer_kind(expiry.token()) else {
        return Ok(());
    };
    if let Some(connection) = context.session_state_mut(expiry.session_id()) {
        connection.tcp_timer_expire(kind);
    }
    context.mark_ready(expiry.session_id());
    Ok(())
}

fn handle_ready_session(
    &mut self,
    runtime: &DataPlaneRuntime,
    context: &mut crate::session::SessionQueueControlContext<'_, TcpServiceConnectionState>,
    session_id: SessionId,
    output_next: SessionQueueNext,
    output: &mut SessionQueueOutput,
) -> CoreResult<()> {
    let timer_output = context
        .session_state_mut(session_id)
        .and_then(TcpConnectionState::on_tcp_timer_expiry);
    if let Some((kind, local, remote, header)) = timer_output {
        let index = alloc_tcp_segment(
            context.buffers(),
            tcp_segment_metadata(local, remote),
            header,
        )?;
        output.enqueue(runtime, output_next.node(), index)?;
        if let Some(token) = TcpSessionProtocol::timer_token(kind) {
            context.arm_timer_ticks(session_id, token, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
        }
    }
    Ok(())
}

fn transport_tx_capacity(
    &mut self,
    state: &TcpServiceConnectionState,
    _: SessionId,
) -> CoreResult<SessionTransportTxCapacity> {
    let connection: TcpConnection<Established, TcpServiceController> =
        state.clone().try_into()?;
    let local = connection
        .local()
        .ok_or_else(|| CoreError::internal("established tcp connection missing local address"))?;
    let tcp = connection.established_tx_capacity();
    let capacity = SessionTransportTxCapacity {
        metadata: tcp_segment_metadata(local, connection.remote()),
        header_len: tcp.header_len,
        payload_budget: tcp.payload_budget,
        should_reschedule: true,
    };
    Ok(capacity)
}

fn write_transport_segment_header(
    &mut self,
    state: &TcpServiceConnectionState,
    buffers: &DataPlaneBuffers,
    index: BufferIndex,
    payload_len: usize,
    now: Instant,
) -> CoreResult<Self::TxUpdate> {
    let connection: TcpConnection<Established, TcpServiceController> =
        state.clone().try_into()?;
    connection.write_established_payload_segment_header(buffers, index, payload_len, now)
}

fn commit_transport_tx(
    &mut self,
    state: &mut TcpServiceConnectionState,
    update: Self::TxUpdate,
) -> CoreResult<()> {
    let mut connection: TcpConnection<Established, TcpServiceController> =
        state.clone().try_into()?;
    connection.commit_established_payload_segment(update);
    *state = connection.into();
    Ok(())
}
```

Do not add a helper that takes `SessionDriverRuntime` for these TX hooks. The generic session runtime already has the session state and packet buffers and passes only those narrow inputs into transport.

- [ ] **Step 6: Run TCP test and commit**

Run:

```bash
cargo test -p hammer-service transport::tcp::session::tests::established_app_send_reaches_tcp_output_lookup_next
```

Expected: pass.

Then run:

```bash
git add crates/hammer-service/src/transport/tcp/state_machine.rs crates/hammer-service/src/transport/tcp/session.rs
git commit -m "hammer-service(Feat): provide tcp established session tx"
```

Expected: commit succeeds.

## Task 5: Boundary Cleanup And Verification

**Files:**
- Verify: `crates/hammer-infra/src/vec.rs`
- Verify: `crates/hammer-runtime/src/app`
- Verify: `crates/hammer-service/src/session`
- Verify: `crates/hammer-service/src/transport/tcp`
- Verify: `crates/hammer-service/src/service.rs`

- [ ] **Step 1: Verify rejected implementation names and APIs are absent**

Run:

```bash
rg -n "SessionAppSendSubmission|drained_sends|take_drained_sends|append_to_packet_buffer|AppDataArea::append_to_buffer|alloc_index_with_headroom|TcpSendQueue|TcpQueuedWrite|TcpSentWrite|write_payload_header|write_tcp_segment_header_prefix|SessionTransportSendParams|transport_send_params|write_transport_header|TcpNormalTxSendParams|normal_tx_send_params" crates/hammer-infra/src crates/hammer-runtime/src/app crates/hammer-service/src/session crates/hammer-service/src/transport/tcp
rg -n "std::vec::Vec<u8>|StdVec" crates/hammer-runtime/src/app crates/hammer-service/src/session crates/hammer-service/src/transport/tcp
rg -n "\\b(let|fn|mut)\\s+_[A-Za-z][A-Za-z0-9_]*\\b|\\|_[A-Za-z][A-Za-z0-9_]*\\b|,\\s*_[A-Za-z][A-Za-z0-9_]*\\s*:" crates/hammer-infra/src crates/hammer-infra/tests crates/hammer-runtime/src/app crates/hammer-service/src/session crates/hammer-service/src/transport/tcp
```

Expected: no output. This intentionally does not scan `crates/hammer-app`, because that façade keeps `std::vec::Vec<u8>` as an explicit external compatibility boundary.

- [ ] **Step 2: Verify session/TCP boundaries**

Run:

```bash
rg -n "TcpOutput|tcp_output|transport::tcp" crates/hammer-service/src/session
rg -n "AppSend|AppSendData|AppDataAddr|SessionQueueOutput|SessionDriverRuntime|SessionQueueControlContext|SessionId" crates/hammer-service/src/transport/tcp/state_machine.rs
rg -n "SessionQueueOutput::default\\(|\\.schedule\\(runtime\\)" crates/hammer-service/src/transport/tcp/session.rs
rg -n "transport_tx_capacity\\([^)]*SessionDriverRuntime|write_transport_segment_header\\([^)]*SessionDriverRuntime|commit_transport_tx\\([^)]*SessionDriverRuntime" crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/session/runtime.rs
rg -n "pub fn app\\(|pub fn app_mut\\(|pending_send|take_ready_sessions|take_timer_expiries|remove_session|close_session|replace_session_state" crates/hammer-service/src/session/protocol.rs
```

Expected: no output.

- [ ] **Step 3: Verify visibility and re-export boundaries**

Run:

```bash
rg -n "pub(\\(crate\\))? struct SessionAppTxProgress|pub use .*SessionAppTxProgress|pub use .*SessionAppCloseSubmission|pub struct SessionAppCloseSubmission|SessionTransportTxCapacity" crates/hammer-service/src/session/mod.rs crates/hammer-service/src/session/app.rs
rg -n "use .*AppSend|use .*AppSendData|use .*AppDataAddr|hammer_runtime::app" crates/hammer-service/src/transport/tcp/state_machine.rs
rg -n "SessionAppTxProgress|SessionAppRuntime|pending_send|copy_pending_send_bytes|commit_pending_send_bytes" crates/hammer-service/src/transport/tcp/state_machine.rs
```

Expected:

- First command may show `struct SessionAppTxProgress` in `session/app.rs` only if it is private; it must not show `pub struct`, `pub(crate) struct`, public close-submission re-exports, or transport capacity re-exports.
- Second command has no output for `state_machine.rs`. TCP app-send tests in `tcp/session.rs` may import app types only inside `#[cfg(test)]` test modules; production TCP code must not import them.
- Third command has no output.

- [ ] **Step 4: Verify service assembly remains the output boundary**

Run:

```bash
rg -n "SessionQueueNext::from_node\\(tcp_output\\)" crates/hammer-service/src/service.rs
```

Expected: finds the service assembly line that connects the session queue to TCP output.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test -p hammer-infra from_elem_copy_initializes_aligned_vec
cargo test -p hammer-runtime app_send_
cargo test -p hammer-runtime app_data_addr_subrange_checks_bounds
cargo test -p hammer-app
cargo test -p hammer-service session::app::tests::pending_send_progress_is_committed_by_session_id session::app::tests::unfinished_send_tracks_progress_without_exposing_entry
cargo test -p hammer-service session::runtime::tests::session_tx_copies_app_send_writes_header_and_keeps_remainder session::runtime::tests::session_tx_does_not_call_transport_when_app_has_no_pending_send
cargo test -p hammer-service transport::tcp::session::tests::established_app_send_reaches_tcp_output_lookup_next
```

Expected: all pass.

- [ ] **Step 6: Run regressions**

Run:

```bash
cargo test -p hammer-service --test tcp_connection_state
cargo test -p hammer-service --test tcp_passive_open
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service transport::tcp::session::tests
cargo test -p hammer-service transport::tcp::syn_sent::tests
cargo test -p hammer-runtime
```

Expected: all pass.

- [ ] **Step 7: Format and commit cleanup if needed**

Run:

```bash
cargo fmt --all
cargo test -p hammer-service
```

Expected: pass.

If formatting or cleanup changed files, run:

```bash
git add crates/hammer-infra/src/vec.rs crates/hammer-infra/tests/vec.rs crates/hammer-runtime/src/app crates/hammer-service/src/session crates/hammer-service/src/transport/tcp
git commit -m "hammer-service(Fix): preserve session tx boundaries"
```

Expected: commit succeeds if there were changes. If there were no changes, skip this commit.

## Out Of Scope

- No TCP Fast Open.
- No FIN app close send path.
- No payload retransmit queue.
- No app send completion opcode.
- No SACK or out-of-order receive queue.
- No RACK/TLP.
- No delayed ACK, persist, keepalive, or TIME-WAIT timer expansion.
- No BBR or sibling congestion-control node work.
- No QUIC transport provider implementation.

## Self-Review Checklist

- VPP mapping is explicit: session owns app TX scheduling, packet-buffer allocation, payload copy, and frame enqueue; TCP owns capacity and header mutation.
- App/session TX byte copies use `hammer_infra::vec::Vec<u8>`, not `std::vec::Vec<u8>`.
- Missing infra functionality is added in `hammer-infra`.
- Generic session files do not reference TCP.
- TCP state machine does not import app/session runtime types.
- Pending app send progress is named `SessionAppTxProgress`, private to `session/app.rs`, and not exposed as a convenience API.
- Session TX advances pending app progress only after successful packet enqueue.
- Existing `DataPlaneBuffers` APIs are reused; no app-layer buffer append helper is added.

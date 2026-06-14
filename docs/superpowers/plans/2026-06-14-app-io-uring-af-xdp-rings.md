# App io_uring AF-XDP Ring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor Hammer's app io_uring-like SQ/CQ runtime into AF-XDP-style descriptor rings plus a separate data area, with explicit layout metadata that can later be mapped for inter-process communication.

**Architecture:** The app ring stops treating dataplane packet buffers as long-lived app receive buffers. Runtime/session code copies packet payload into an app-owned data area and completes CQEs with data-area addresses, while app send SQEs reference the same app-owned data area. The first implementation stays in-process, but every ring and data-area type is built around stable offsets, cacheline-separated producer/consumer cursors, and exportable layout descriptors so a future process can map the same memory without changing SQE/CQE formats.

**Tech Stack:** Rust 2024, `hammer-runtime::app`, `hammer-infra::{ring,vec,map}`, `hammer-adapter::DataPlaneBuffers`, VPP session FIFO semantics, AF-XDP UMEM/ring shape, io_uring SQE/CQE semantics.

---

## Current Evidence

- VPP TCP receive uses packet buffers only as transport-node input. `tcp_session_enqueue_data` calls `session_enqueue_stream_connection(&tc->connection, b, ...)`, then session enqueue copies payload from `vlib_buffer_get_current(b)` into `s->rx_fifo`.
- VPP app workers receive session/fifo events, not packet buffer indexes. `session_event_t` carries `session_index` or `session_handle`.
- VPP TX reads from `s->tx_fifo` and builds new `vlib_buffer_t` chains for output.
- Current Hammer app runtime stores rings in `AppRingHandle` as `Rc<RefCell<AppRingState<AppSqeDescriptor>>>` and `Rc<RefCell<AppRingState<AppCqeDescriptor>>>`.
- Current Hammer receive completion can hand an `AppBufferLease` backed by `DataPlaneBuffers` to the app. That is a zero-copy shortcut but does not match VPP's session FIFO ownership model.
- Current `hammer-infra::ring` already has copyable descriptors and `LocalRing<T>`, which is a good in-process stepping stone but not yet an AF-XDP-style shared-memory layout.
- DPDK `rte_ring` is the right reference shape for cross-runtime app/dataplane rings: fixed-size table, power-of-two ring size, mask-based indexes, and lockless producer/consumer head-tail structures. The hot producer and consumer state are separated into cache-aligned blocks so poll-mode traffic does not bounce the same cacheline between cores.

## Target Shape

The runtime app surface should look like this:

```text
AppRingHandle
  control/layout metadata
  SQ lock-free ring        app -> session/runtime
  CQ lock-free ring        session/runtime -> app
  fill/free lock-free ring app returns reusable data slots
  data area                app-owned fixed-size chunks, AF-XDP UMEM-like
  local wakers             in-process only; not part of export layout
```

Rules:

1. App SQ/CQ descriptors only carry opaque operation ids, user data, flags, result codes, and data-area addresses.
2. App receive completion copies bytes from dataplane packet buffers into the app data area, then releases the dataplane packet buffer.
3. App send submission references app data-area addresses; transport/session copies or peeks bytes out of the app data area into protocol TX/FIFO state.
4. `DataPlaneBuffers` must not appear in stable app SQE/CQE payloads.
5. App/dataplane rings must use an infra-owned lock-free ring implementation. They must not use `Rc<RefCell<_>>`, `Mutex`, `LocalRing`, or a runtime-local queue as the cross-runtime data path.
6. Ring producer and consumer state must follow the DPDK head/tail shape. Cacheline 0 is producer-owned (`prod_head`, `prod_tail`); cacheline 1 is consumer-owned (`cons_head`, `cons_tail`). The hot path must not maintain a shared `len` field written by both sides.
7. Ring offsets, exported shared-memory regions, and data-area chunk starts must be aligned to `hammer_infra::align::CACHE_LINE` (`64`) using `hammer_infra::align::align_up`.
8. App data chunk size must be a non-zero multiple of `CACHE_LINE`; otherwise adjacent chunks can force false sharing in poll-mode app/session traffic.
9. IPC support is reserved by stable layout/export types. This plan does not implement Unix domain socket fd passing or cross-process wakeups.

## File Structure

- Modify `crates/hammer-infra/src/ring.rs`
  - Add a DPDK-style `LockFreeRing<T>` with power-of-two capacity, mask-based indexes, lockless single-producer/single-consumer enqueue/dequeue, free-space, and occupancy semantics.
  - Store producer and consumer head/tail blocks as cacheline-separated fields: cacheline 0 contains producer state and cacheline 1 contains consumer state.
  - Use atomic cursors and `UnsafeCell<MaybeUninit<T>>` slots internally. Keep the safe public API constrained to `T: Copy` descriptors for this plan.
  - Keep `LocalRing<T>` for existing users; do not break unrelated data-plane tests.

- Create `crates/hammer-runtime/src/app/data.rs`
  - Define `AppDataAddr`, `AppDataChunk`, and `AppDataArea`.
  - Implement fixed-size chunk allocation, copy-in, copy-out, release, and stale-generation rejection with atomic chunk metadata.
  - Keep first storage allocation process-local, but make the API shared-runtime safe: `AppDataArea` methods take `&self`, storage uses fixed chunk offsets, and ownership is transferred through lock-free fill/free rings.
  - Safety invariant: a chunk has exactly one writer/owner between ring handoffs. Producer copies bytes with a bounded `ptr::copy_nonoverlapping` before publishing the descriptor with `Release`; consumer copies bytes out after dequeuing with `Acquire`; release invalidates the generation before returning the chunk to the free ring.

- Create `crates/hammer-runtime/src/app/layout.rs`
  - Define `AppRingLayout`, `AppRingExport`, and `AppRingMemoryKind`.
  - Store offsets/capacities/chunk size/cacheline size in stable integer fields.
  - Align every exported region offset to `CACHE_LINE`.
  - Make all layout/export types `Copy` or `Clone + Debug + PartialEq + Eq` where possible.
  - Do not put `Rc`, `RefCell`, raw Rust pointers, `DataPlaneBuffers`, or wakers in exportable types.

- Modify `crates/hammer-runtime/src/app/ring.rs`
  - Replace stable SQ/CQ buffer payload storage with `AppDataArea`.
  - Replace `Rc<RefCell<AppRingState<_>>>` SQ/CQ/fill rings with `hammer_infra::ring::LockFreeRing<_>`.
  - Change SQE/CQE data payloads from `BufferIndex` to `AppDataAddr`.
  - Add `AppRingHandle::with_data_area(...)`, `layout()`, and `export_layout()`.
  - Keep compatibility constructors while tests migrate.

- Modify `crates/hammer-runtime/src/app/context.rs`
  - Convert `AppRuntime::send`, `AppRuntime::recv`, and `AppContext::try_complete_recv_buffer` to use app data-area addresses.
  - Ensure `try_complete_recv_buffer` copies packet bytes into app data storage and frees/releases the dataplane buffer immediately.

- Modify `crates/hammer-runtime/src/app/mod.rs`
  - Re-export app data/layout types.

- Modify `crates/hammer-service/src/session/app.rs`
  - Stop exposing recv completion to app code as an `AppBufferLease`.
  - Accept a temporary dataplane lease from TCP, copy bytes into the bound app ring data area, and release that dataplane lease before returning.

- Modify `crates/hammer-service/src/transport/tcp/established.rs`
  - Keep payload delivery as a temporary allocated packet buffer only until `complete_recv_buffer` copies it into the app data area and frees it.
  - Do not expose packet buffer leases to app code.

- Modify tests:
  - `crates/hammer-infra/tests/ring.rs`
  - `crates/hammer-runtime/tests/app_ring.rs`
  - `crates/hammer-service/tests/tcp_established_receive.rs`
  - `crates/hammer-service/src/transport/tcp/session.rs` unit tests

---

### Task 1: Add DPDK-Style Lock-Free Ring in Infra

**Files:**
- Modify: `crates/hammer-infra/src/ring.rs`
- Modify: `crates/hammer-infra/tests/ring.rs`

- [ ] **Step 1: Write the failing lock-free ring tests**

Append this test to `crates/hammer-infra/tests/ring.rs`:

```rust
use hammer_infra::align::CACHE_LINE;
use hammer_infra::ring::{LockFreeRing, LockFreeRingCursors, LockFreeRingHeadTail, RingError};

#[test]
fn lock_free_ring_sp_sc_tracks_capacity_and_wraparound() {
    let ring = LockFreeRing::with_capacity(4).expect("ring");

    assert_eq!(ring.capacity(), 3);
    assert_eq!(ring.available_to_read(), 0);
    assert_eq!(ring.available_to_write(), 3);

    assert_eq!(ring.enqueue_sp(10), Ok(()));
    assert_eq!(ring.enqueue_sp(11), Ok(()));
    assert_eq!(ring.enqueue_sp(12), Ok(()));
    assert_eq!(ring.enqueue_sp(13), Err(RingError::Full(13)));
    assert_eq!(ring.available_to_read(), 3);
    assert_eq!(ring.available_to_write(), 0);

    assert_eq!(ring.dequeue_sc(), Some(10));
    assert_eq!(ring.dequeue_sc(), Some(11));
    assert_eq!(ring.available_to_read(), 1);
    assert_eq!(ring.available_to_write(), 2);

    assert_eq!(ring.enqueue_sp(13), Ok(()));
    assert_eq!(ring.enqueue_sp(14), Ok(()));
    assert_eq!(ring.dequeue_sc(), Some(12));
    assert_eq!(ring.dequeue_sc(), Some(13));
    assert_eq!(ring.dequeue_sc(), Some(14));
    assert_eq!(ring.dequeue_sc(), None);
}

#[test]
fn lock_free_ring_rejects_non_power_of_two_size() {
    assert!(matches!(
        LockFreeRing::<u64>::with_capacity(3),
        Err(RingError::InvalidCapacity)
    ));
}

#[test]
fn lock_free_ring_cursors_are_split_by_cacheline() {
    assert_eq!(std::mem::align_of::<LockFreeRingHeadTail>(), CACHE_LINE);
    assert_eq!(std::mem::size_of::<LockFreeRingHeadTail>(), CACHE_LINE);
    assert_eq!(std::mem::align_of::<LockFreeRingCursors>(), CACHE_LINE);
    assert_eq!(LockFreeRingCursors::PRODUCER_CACHELINE_OFFSET, 0);
    assert_eq!(LockFreeRingCursors::CONSUMER_CACHELINE_OFFSET, CACHE_LINE);
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test -p hammer-infra --test ring lock_free_ring -- --nocapture
```

Expected: FAIL with unresolved imports `LockFreeRing`, `LockFreeRingCursors`, `LockFreeRingHeadTail`, or `RingError`.

- [ ] **Step 3: Implement the ring type**

Append this implementation to `crates/hammer-infra/src/ring.rs` after `LocalRing<T>`:

```rust
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::align::{CACHE_LINE, CacheLine};
use crate::boxed::Slice;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingError<T = ()> {
    InvalidCapacity,
    Full(T),
}

#[repr(C, align(64))]
pub struct LockFreeRingHeadTail {
    head: AtomicU32,
    tail: AtomicU32,
    _reserved: [u8; CACHE_LINE - 8],
}

impl LockFreeRingHeadTail {
    #[inline]
    pub const fn new() -> Self {
        Self {
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
            _reserved: [0; CACHE_LINE - 8],
        }
    }

    #[inline]
    pub fn head(&self) -> u32 {
        self.head.load(Ordering::Acquire)
    }

    #[inline]
    pub fn tail(&self) -> u32 {
        self.tail.load(Ordering::Acquire)
    }
}

impl Default for LockFreeRingHeadTail {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[repr(C)]
pub struct LockFreeRingCursors {
    producer: CacheLine<LockFreeRingHeadTail>,
    consumer: CacheLine<LockFreeRingHeadTail>,
}

impl LockFreeRingCursors {
    pub const PRODUCER_CACHELINE_OFFSET: usize = 0;
    pub const CONSUMER_CACHELINE_OFFSET: usize = CACHE_LINE;

    #[inline]
    pub const fn new() -> Self {
        Self {
            producer: CacheLine::new(LockFreeRingHeadTail::new()),
            consumer: CacheLine::new(LockFreeRingHeadTail::new()),
        }
    }
}

impl Default for LockFreeRingCursors {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

struct LockFreeRingSlot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> LockFreeRingSlot<T> {
    #[inline]
    const fn uninit() -> Self {
        Self {
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

pub struct LockFreeRing<T: Copy> {
    size: u32,
    mask: u32,
    capacity: u32,
    cursors: LockFreeRingCursors,
    slots: Slice<LockFreeRingSlot<T>>,
}

unsafe impl<T: Copy + Send> Send for LockFreeRing<T> {}
unsafe impl<T: Copy + Send> Sync for LockFreeRing<T> {}

impl<T: Copy> LockFreeRing<T> {
    pub fn with_capacity(size: usize) -> Result<Self, RingError> {
        if size < 2 || !size.is_power_of_two() || size > u32::MAX as usize {
            return Err(RingError::InvalidCapacity);
        }
        let slots = Slice::from_fn(size, |_| LockFreeRingSlot::uninit());
        Ok(Self {
            size: size as u32,
            mask: size as u32 - 1,
            capacity: size as u32 - 1,
            cursors: LockFreeRingCursors::new(),
            slots,
        })
    }

    #[inline]
    pub fn ring_size(&self) -> usize {
        self.size as usize
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity as usize
    }

    #[inline]
    pub fn available_to_read(&self) -> usize {
        let prod_tail = self.cursors.producer.tail.load(Ordering::Acquire);
        let cons_head = self.cursors.consumer.head.load(Ordering::Relaxed);
        prod_tail.wrapping_sub(cons_head) as usize
    }

    #[inline]
    pub fn available_to_write(&self) -> usize {
        let cons_tail = self.cursors.consumer.tail.load(Ordering::Acquire);
        let prod_head = self.cursors.producer.head.load(Ordering::Relaxed);
        (self.mask.wrapping_add(cons_tail).wrapping_sub(prod_head)) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.available_to_read() == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.available_to_write() == 0
    }

    #[inline]
    pub fn enqueue_sp(&self, value: T) -> Result<(), RingError<T>> {
        let prod_head = self.cursors.producer.head.load(Ordering::Relaxed);
        let cons_tail = self.cursors.consumer.tail.load(Ordering::Acquire);
        let free_entries = self.mask.wrapping_add(cons_tail).wrapping_sub(prod_head);
        if free_entries == 0 {
            return Err(RingError::Full(value));
        }
        let prod_next = prod_head.wrapping_add(1);
        self.cursors.producer.head.store(prod_next, Ordering::Relaxed);
        let slot = (prod_head & self.mask) as usize;
        unsafe { (*self.slots[slot].value.get()).write(value) };
        self.cursors.producer.tail.store(prod_next, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn dequeue_sc(&self) -> Option<T> {
        let cons_head = self.cursors.consumer.head.load(Ordering::Relaxed);
        let prod_tail = self.cursors.producer.tail.load(Ordering::Acquire);
        let entries = prod_tail.wrapping_sub(cons_head);
        if entries == 0 {
            return None;
        }
        let cons_next = cons_head.wrapping_add(1);
        self.cursors.consumer.head.store(cons_next, Ordering::Relaxed);
        let slot = (cons_head & self.mask) as usize;
        let value = unsafe { (*self.slots[slot].value.get()).assume_init_read() };
        self.cursors.consumer.tail.store(cons_next, Ordering::Release);
        Some(value)
    }
}
```

This is intentionally SP/SC first because app SQ, app CQ, and fill/free rings each have one producer and one consumer. Keep MP/MC CAS support out of this task; the head/tail field layout matches DPDK so it can be extended later without changing the exported ring shape.

- [ ] **Step 4: Run the focused infra test**

Run:

```bash
cargo test -p hammer-infra --test ring lock_free_ring -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run all infra ring tests**

Run:

```bash
cargo test -p hammer-infra --test ring
```

Expected: PASS for all `ring` tests.

- [ ] **Step 6: Commit**

Run:

```bash
git add crates/hammer-infra/src/ring.rs crates/hammer-infra/tests/ring.rs
git commit -m "hammer-infra(Feat): add lock-free ring"
```

Expected: one commit containing only the infra lock-free ring addition and tests.

---

### Task 2: Add App Data Area With AF-XDP-Style Addresses

**Files:**
- Create: `crates/hammer-runtime/src/app/data.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify: `crates/hammer-runtime/tests/app_ring.rs`

- [ ] **Step 1: Write the failing data-area test**

Append this test to `crates/hammer-runtime/tests/app_ring.rs`:

```rust
use hammer_infra::align::CACHE_LINE;
use hammer_runtime::app::{AppDataArea, AppDataAreaConfig};

#[test]
fn app_data_area_allocates_copies_and_rejects_stale_addresses() {
    let area = AppDataArea::new(AppDataAreaConfig {
        chunk_size: 64,
        chunk_count: 2,
    })
    .expect("data area");

    let first = area.alloc().expect("first chunk");
    assert_eq!(first.offset(), 0);
    assert_eq!(first.offset() % CACHE_LINE, 0);
    assert_eq!(first.len(), 0);
    assert_eq!(first.capacity(), 64);

    area.write(first, b"hello").expect("write first");
    assert_eq!(area.read(first).expect("read first"), b"hello");

    let second = area.alloc().expect("second chunk");
    assert_eq!(second.offset(), 64);
    assert_eq!(second.offset() % CACHE_LINE, 0);
    assert!(area.alloc().is_none());

    area.release(first).expect("release first");
    assert!(area.read(first).is_err(), "released address generation must be stale");

    let reused = area.alloc().expect("reused chunk");
    assert_eq!(reused.offset(), 0);
    assert_ne!(reused.generation(), first.generation());

    area.release(second).expect("release second");
    area.release(reused).expect("release reused");
}

#[test]
fn app_data_area_rejects_non_cacheline_chunk_size() {
    assert!(AppDataArea::new(AppDataAreaConfig {
        chunk_size: CACHE_LINE - 1,
        chunk_count: 1,
    })
    .is_err());
}

#[test]
fn app_data_area_uses_bulk_copy_not_byte_loop() {
    let source = include_str!("../src/app/data.rs");
    assert!(
        source.contains("ptr::copy_nonoverlapping"),
        "app data copy path must use bounded bulk copy"
    );
    assert!(
        !source.contains("for (offset, byte) in bytes.iter().copied().enumerate()"),
        "app data write path must not copy byte-by-byte"
    );
}
```

- [ ] **Step 2: Run the failing data-area test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_data_area_allocates_copies_and_rejects_stale_addresses -- --exact
```

Expected: FAIL with unresolved imports `AppDataArea` and `AppDataAreaConfig`.

- [ ] **Step 3: Create `data.rs`**

Create `crates/hammer-runtime/src/app/data.rs` with this content:

```rust
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::align::CACHE_LINE;
use hammer_infra::boxed::Slice;
use hammer_infra::vec::Vec;
use std::vec::Vec as StdVec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppDataAreaConfig {
    pub chunk_size: usize,
    pub chunk_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppDataAddr {
    chunk: u32,
    generation: u32,
    offset: u32,
    len: u32,
    capacity: u32,
}

impl AppDataAddr {
    #[inline]
    pub const fn new(chunk: u32, generation: u32, offset: u32, len: u32, capacity: u32) -> Self {
        Self {
            chunk,
            generation,
            offset,
            len,
            capacity,
        }
    }

    #[inline]
    pub const fn chunk(self) -> u32 {
        self.chunk
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.generation
    }

    #[inline]
    pub const fn offset(self) -> usize {
        self.offset as usize
    }

    #[inline]
    pub const fn len(self) -> usize {
        self.len as usize
    }

    #[inline]
    pub const fn capacity(self) -> usize {
        self.capacity as usize
    }

    #[inline]
    pub const fn with_len(self, len: usize) -> Self {
        Self {
            len: len as u32,
            ..self
        }
    }
}

struct AppDataChunk {
    generation: AtomicU32,
    len: AtomicU32,
    in_use: AtomicBool,
}

pub struct AppDataArea {
    chunk_size: usize,
    storage: Slice<u8>,
    chunks: Vec<AppDataChunk>,
}

unsafe impl Send for AppDataArea {}
unsafe impl Sync for AppDataArea {}

impl AppDataArea {
    pub fn new(config: AppDataAreaConfig) -> HammerResult<Self> {
        if config.chunk_size == 0 {
            return Err(HammerError::internal("app data chunk size must be non-zero"));
        }
        if config.chunk_size % CACHE_LINE != 0 {
            return Err(HammerError::internal(
                "app data chunk size must be cacheline aligned",
            ));
        }
        if config.chunk_count == 0 {
            return Err(HammerError::internal("app data chunk count must be non-zero"));
        }
        let total = config
            .chunk_size
            .checked_mul(config.chunk_count)
            .ok_or_else(|| HammerError::internal("app data area size overflow"))?;
        let storage = Slice::from_elem(total, 0);
        let mut chunks = Vec::with_capacity(config.chunk_count);
        for _ in 0..config.chunk_count {
            chunks.push(AppDataChunk {
                generation: AtomicU32::new(1),
                len: AtomicU32::new(0),
                in_use: AtomicBool::new(false),
            });
        }
        Ok(Self {
            chunk_size: config.chunk_size,
            storage,
            chunks,
        })
    }

    #[inline]
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn alloc(&self) -> Option<AppDataAddr> {
        let index = self.chunks.iter().position(|chunk| {
            chunk
                .in_use
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        })?;
        let chunk = &self.chunks[index];
        chunk.len.store(0, Ordering::Release);
        Some(AppDataAddr::new(
            index as u32,
            chunk.generation.load(Ordering::Acquire),
            (index * self.chunk_size) as u32,
            0,
            self.chunk_size as u32,
        ))
    }

    pub fn write(&self, addr: AppDataAddr, bytes: &[u8]) -> HammerResult<AppDataAddr> {
        self.validate(addr)?;
        if bytes.len() > self.chunk_size {
            return Err(HammerError::internal(format!(
                "app data write length {} exceeds chunk size {}",
                bytes.len(),
                self.chunk_size
            )));
        }
        let start = addr.offset();
        // SAFETY: `validate` proves that `addr` names a live chunk with this
        // area's chunk size. The length check above proves the destination
        // range stays inside that chunk. Chunk ownership is transferred through
        // app rings, so no other writer owns this range while this write runs.
        unsafe {
            ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.storage.as_ptr().add(start).cast_mut(),
                bytes.len(),
            );
        }
        self.chunks[addr.chunk() as usize]
            .len
            .store(bytes.len() as u32, Ordering::Release);
        Ok(addr.with_len(bytes.len()))
    }

    pub fn read(&self, addr: AppDataAddr) -> HammerResult<StdVec<u8>> {
        self.validate(addr)?;
        let chunk = &self.chunks[addr.chunk() as usize];
        let len = addr.len().min(chunk.len.load(Ordering::Acquire) as usize);
        let start = addr.offset();
        let mut out = vec![0u8; len];
        // SAFETY: `validate` proves that `addr` names a live chunk. `len` is
        // bounded by the chunk's published length and the address capacity.
        // The producer published the bytes before the descriptor was consumed.
        unsafe {
            ptr::copy_nonoverlapping(self.storage.as_ptr().add(start), out.as_mut_ptr(), len);
        }
        Ok(out)
    }

    pub fn release(&self, addr: AppDataAddr) -> HammerResult<()> {
        self.validate(addr)?;
        let chunk = &self.chunks[addr.chunk() as usize];
        chunk.len.store(0, Ordering::Release);
        let next = chunk
            .generation
            .load(Ordering::Acquire)
            .wrapping_add(1)
            .max(1);
        chunk.generation.store(next, Ordering::Release);
        chunk.in_use.store(false, Ordering::Release);
        Ok(())
    }

    fn validate(&self, addr: AppDataAddr) -> HammerResult<()> {
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
        Ok(())
    }
}
```

- [ ] **Step 4: Re-export data-area types**

Modify `crates/hammer-runtime/src/app/mod.rs` so it starts with:

```rust
mod context;
mod data;
mod ring;

pub use context::{
    AppContext, AppRecvFuture, AppRuntime, AppTaskContext, AppWorkerContext, AppWorkerLocalExecutor,
};
pub use data::{AppDataAddr, AppDataArea, AppDataAreaConfig};
```

Keep the existing `pub use ring::{...};` block below those lines.

- [ ] **Step 5: Run the data-area test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_data_area_allocates_copies_and_rejects_stale_addresses -- --exact
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add crates/hammer-runtime/src/app/data.rs crates/hammer-runtime/src/app/mod.rs crates/hammer-runtime/tests/app_ring.rs
git commit -m "hammer-runtime(Feat): add app data area"
```

Expected: one commit with the app-owned data area and focused test.

---

### Task 3: Add Exportable Ring Layout Metadata

**Files:**
- Create: `crates/hammer-runtime/src/app/layout.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-runtime/tests/app_ring.rs`

- [ ] **Step 1: Write the failing layout test**

Append this test to `crates/hammer-runtime/tests/app_ring.rs`:

```rust
use hammer_runtime::app::{AppRingExport, AppRingHandle, AppRingMemoryKind};
use hammer_infra::align::CACHE_LINE;

#[test]
fn app_ring_export_layout_has_no_process_local_state() {
    let ring = AppRingHandle::with_data_area(8, 16, 2048, 64).expect("ring");
    let export = ring.export_layout();

    assert_eq!(export.memory_kind(), AppRingMemoryKind::ProcessLocal);
    assert_eq!(export.cacheline_size(), CACHE_LINE);
    assert_eq!(export.submission_capacity(), 8);
    assert_eq!(export.completion_capacity(), 16);
    assert_eq!(export.data_chunk_count(), 64);
    assert_eq!(export.data_chunk_size(), 2048);
    assert_eq!(export.submission_ring_offset(), 0);
    assert!(export.submission_ring_bytes() > 0);
    assert!(export.completion_ring_bytes() > 0);
    assert!(export.fill_ring_bytes() > 0);
    assert!(export.submission_ring_offset() < export.completion_ring_offset());
    assert!(export.completion_ring_offset() < export.fill_ring_offset());
    assert!(export.completion_ring_offset() < export.data_area_offset());
    assert_eq!(export.submission_ring_offset() % CACHE_LINE, 0);
    assert_eq!(export.completion_ring_offset() % CACHE_LINE, 0);
    assert_eq!(export.fill_ring_offset() % CACHE_LINE, 0);
    assert_eq!(export.data_area_offset() % CACHE_LINE, 0);
}
```

- [ ] **Step 2: Run the failing layout test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_ring_export_layout_has_no_process_local_state -- --exact
```

Expected: FAIL because `AppRingExport`, `AppRingMemoryKind`, and `AppRingHandle::with_data_area` are missing.

- [ ] **Step 3: Create layout types**

Create `crates/hammer-runtime/src/app/layout.rs` with this content:

```rust
use hammer_infra::align::{align_up, CACHE_LINE};
use std::mem;

use crate::app::ring::{AppCqeDescriptor, AppSqeDescriptor};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppRingMemoryKind {
    ProcessLocal,
    SharedMemory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppRingLayout {
    submission_ring_offset: usize,
    completion_ring_offset: usize,
    fill_ring_offset: usize,
    data_area_offset: usize,
    cacheline_size: usize,
    submission_ring_bytes: usize,
    completion_ring_bytes: usize,
    fill_ring_bytes: usize,
    submission_capacity: usize,
    completion_capacity: usize,
    data_chunk_size: usize,
    data_chunk_count: usize,
}

impl AppRingLayout {
    pub fn new(
        submission_capacity: usize,
        completion_capacity: usize,
        data_chunk_size: usize,
        data_chunk_count: usize,
    ) -> Self {
        let submission_ring_size = submission_capacity
            .checked_add(1)
            .and_then(|value| value.checked_next_power_of_two())
            .expect("submission ring size overflow");
        let completion_ring_size = completion_capacity
            .checked_add(1)
            .and_then(|value| value.checked_next_power_of_two())
            .expect("completion ring size overflow");
        let fill_ring_size = data_chunk_count
            .checked_add(1)
            .and_then(|value| value.checked_next_power_of_two())
            .expect("fill ring size overflow");
        let submission_ring_bytes = mem::size_of::<AppSqeDescriptor>()
            .checked_mul(submission_ring_size)
            .expect("submission ring layout overflow");
        let completion_ring_bytes = mem::size_of::<AppCqeDescriptor>()
            .checked_mul(completion_ring_size)
            .expect("completion ring layout overflow");
        let fill_ring_bytes = mem::size_of::<u32>()
            .checked_mul(fill_ring_size)
            .expect("fill ring layout overflow");
        let submission_ring_offset = 0;
        let completion_ring_offset = align_up(
            submission_ring_offset + submission_ring_bytes,
            CACHE_LINE,
        );
        let fill_ring_offset = align_up(
            completion_ring_offset + completion_ring_bytes,
            CACHE_LINE,
        );
        let data_area_offset = align_up(fill_ring_offset + fill_ring_bytes, CACHE_LINE);
        Self {
            submission_ring_offset,
            completion_ring_offset,
            fill_ring_offset,
            data_area_offset,
            cacheline_size: CACHE_LINE,
            submission_ring_bytes,
            completion_ring_bytes,
            fill_ring_bytes,
            submission_capacity,
            completion_capacity,
            data_chunk_size,
            data_chunk_count,
        }
    }

    #[inline]
    pub const fn submission_ring_offset(self) -> usize {
        self.submission_ring_offset
    }

    #[inline]
    pub const fn completion_ring_offset(self) -> usize {
        self.completion_ring_offset
    }

    #[inline]
    pub const fn fill_ring_offset(self) -> usize {
        self.fill_ring_offset
    }

    #[inline]
    pub const fn data_area_offset(self) -> usize {
        self.data_area_offset
    }

    #[inline]
    pub const fn cacheline_size(self) -> usize {
        self.cacheline_size
    }

    #[inline]
    pub const fn submission_ring_bytes(self) -> usize {
        self.submission_ring_bytes
    }

    #[inline]
    pub const fn completion_ring_bytes(self) -> usize {
        self.completion_ring_bytes
    }

    #[inline]
    pub const fn fill_ring_bytes(self) -> usize {
        self.fill_ring_bytes
    }

    #[inline]
    pub const fn submission_capacity(self) -> usize {
        self.submission_capacity
    }

    #[inline]
    pub const fn completion_capacity(self) -> usize {
        self.completion_capacity
    }

    #[inline]
    pub const fn data_chunk_size(self) -> usize {
        self.data_chunk_size
    }

    #[inline]
    pub const fn data_chunk_count(self) -> usize {
        self.data_chunk_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppRingExport {
    memory_kind: AppRingMemoryKind,
    layout: AppRingLayout,
}

impl AppRingExport {
    #[inline]
    pub const fn new(memory_kind: AppRingMemoryKind, layout: AppRingLayout) -> Self {
        Self { memory_kind, layout }
    }

    #[inline]
    pub const fn memory_kind(self) -> AppRingMemoryKind {
        self.memory_kind
    }

    #[inline]
    pub const fn layout(self) -> AppRingLayout {
        self.layout
    }

    #[inline]
    pub const fn submission_ring_offset(self) -> usize {
        self.layout.submission_ring_offset()
    }

    #[inline]
    pub const fn completion_ring_offset(self) -> usize {
        self.layout.completion_ring_offset()
    }

    #[inline]
    pub const fn fill_ring_offset(self) -> usize {
        self.layout.fill_ring_offset()
    }

    #[inline]
    pub const fn data_area_offset(self) -> usize {
        self.layout.data_area_offset()
    }

    #[inline]
    pub const fn cacheline_size(self) -> usize {
        self.layout.cacheline_size()
    }

    #[inline]
    pub const fn submission_ring_bytes(self) -> usize {
        self.layout.submission_ring_bytes()
    }

    #[inline]
    pub const fn completion_ring_bytes(self) -> usize {
        self.layout.completion_ring_bytes()
    }

    #[inline]
    pub const fn fill_ring_bytes(self) -> usize {
        self.layout.fill_ring_bytes()
    }

    #[inline]
    pub const fn submission_capacity(self) -> usize {
        self.layout.submission_capacity()
    }

    #[inline]
    pub const fn completion_capacity(self) -> usize {
        self.layout.completion_capacity()
    }

    #[inline]
    pub const fn data_chunk_size(self) -> usize {
        self.layout.data_chunk_size()
    }

    #[inline]
    pub const fn data_chunk_count(self) -> usize {
        self.layout.data_chunk_count()
    }
}
```

- [ ] **Step 4: Re-export layout types**

Modify `crates/hammer-runtime/src/app/mod.rs` to include:

```rust
mod layout;

pub use layout::{AppRingExport, AppRingLayout, AppRingMemoryKind};
```

Keep the existing `context`, `data`, and `ring` module exports.

- [ ] **Step 5: Add layout fields to `AppRingHandle`**

Modify `crates/hammer-runtime/src/app/ring.rs`:

1. Add these imports near the top:

```rust
use crate::app::data::{AppDataArea, AppDataAreaConfig};
use crate::app::layout::{AppRingExport, AppRingLayout, AppRingMemoryKind};
use hammer_infra::ring::LockFreeRing;
```

2. Add these fields to `AppRingHandle`:

```rust
layout: AppRingLayout,
submissions: Arc<LockFreeRing<AppSqeDescriptor>>,
completions: Arc<LockFreeRing<AppCqeDescriptor>>,
free_chunks: Arc<LockFreeRing<u32>>,
data_area: Arc<AppDataArea>,
```

The ring fields replace the old `Rc<RefCell<AppRingState<_>>>` fields. Keep process-local wakers or pending operation registries as process-local state, but do not use them as the SQ/CQ transport between app-worker and dataplane.

3. Replace `AppRingHandle::new` with:

```rust
#[inline]
pub fn new(submission_capacity: usize, completion_capacity: usize) -> Self {
    Self::with_data_area(submission_capacity, completion_capacity, 2048, submission_capacity.max(completion_capacity).max(1))
        .expect("default app ring data area")
}

pub fn with_data_area(
    submission_capacity: usize,
    completion_capacity: usize,
    data_chunk_size: usize,
    data_chunk_count: usize,
) -> HammerResult<Self> {
    if submission_capacity == 0 || completion_capacity == 0 || data_chunk_count == 0 {
        return Err(HammerError::internal("app ring capacities must be non-zero"));
    }
    let submission_ring_size = submission_capacity
        .checked_add(1)
        .and_then(|value| value.checked_next_power_of_two())
        .ok_or_else(|| HammerError::internal("submission ring size overflow"))?;
    let completion_ring_size = completion_capacity
        .checked_add(1)
        .and_then(|value| value.checked_next_power_of_two())
        .ok_or_else(|| HammerError::internal("completion ring size overflow"))?;
    let fill_ring_size = data_chunk_count
        .checked_add(1)
        .and_then(|value| value.checked_next_power_of_two())
        .ok_or_else(|| HammerError::internal("fill ring size overflow"))?;
    let layout = AppRingLayout::new(
        submission_capacity,
        completion_capacity,
        data_chunk_size,
        data_chunk_count,
    );
    Ok(Self {
        submissions: Arc::new(LockFreeRing::with_capacity(submission_ring_size)
            .map_err(|_| HammerError::internal("invalid submission ring capacity"))?),
        completions: Arc::new(LockFreeRing::with_capacity(completion_ring_size)
            .map_err(|_| HammerError::internal("invalid completion ring capacity"))?),
        free_chunks: Arc::new(LockFreeRing::with_capacity(fill_ring_size)
            .map_err(|_| HammerError::internal("invalid fill ring capacity"))?),
        buffers: Rc::new(RefCell::new(AppRingBufferRegistry::default())),
        pending_submissions: Rc::new(RefCell::new(AppPendingSubmissionRegistry::default())),
        layout,
        data_area: Arc::new(AppDataArea::new(AppDataAreaConfig {
            chunk_size: data_chunk_size,
            chunk_count: data_chunk_count,
        })?),
    })
}
```

4. Add these methods to `impl AppRingHandle`:

```rust
#[inline]
pub fn layout(&self) -> AppRingLayout {
    self.layout
}

#[inline]
pub fn export_layout(&self) -> AppRingExport {
    AppRingExport::new(AppRingMemoryKind::ProcessLocal, self.layout)
}
```

- [ ] **Step 6: Run the focused layout test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_ring_export_layout_has_no_process_local_state -- --exact
```

Expected: PASS.

- [ ] **Step 7: Run all app ring tests**

Run:

```bash
cargo test -p hammer-runtime --test app_ring
```

Expected: PASS.

- [ ] **Step 8: Commit**

Run:

```bash
git add crates/hammer-runtime/src/app/layout.rs crates/hammer-runtime/src/app/mod.rs crates/hammer-runtime/src/app/ring.rs crates/hammer-runtime/tests/app_ring.rs
git commit -m "hammer-runtime(Feat): add exportable app ring layout"
```

Expected: one commit with layout/export metadata and tests.

---

### Task 4: Move SQ/CQ Payloads From BufferIndex to AppDataAddr

**Files:**
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-runtime/src/app/context.rs`
- Modify: `crates/hammer-runtime/src/app/mod.rs`
- Modify: `crates/hammer-runtime/tests/app_ring.rs`

- [ ] **Step 1: Write the failing descriptor payload test**

Append this test to `crates/hammer-runtime/tests/app_ring.rs`:

```rust
use hammer_runtime::app::{AppDataAddr, AppSqeData, AppCqeData};

#[test]
fn app_descriptors_use_data_area_addresses_not_dataplane_buffer_indexes() {
    let addr = AppDataAddr::new(3, 9, 4096, 12, 2048);

    assert_eq!(AppSqeData::Send { data: addr }, AppSqeData::Send { data: addr });
    assert_eq!(AppCqeData::Recv { data: addr }, AppCqeData::Recv { data: addr });
}
```

- [ ] **Step 2: Run the failing descriptor payload test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_descriptors_use_data_area_addresses_not_dataplane_buffer_indexes -- --exact
```

Expected: FAIL because `AppSqeData::Send { data }` and `AppCqeData::Recv { data }` do not exist yet.

- [ ] **Step 3: Change descriptor payload types**

In `crates/hammer-runtime/src/app/ring.rs`:

1. Add `AppDataAddr` to the data import:

```rust
use crate::app::data::{AppDataAddr, AppDataArea, AppDataAreaConfig};
```

2. Change `AppSqeData` to:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppSqeData {
    Nop,
    Recv { max_len: u32 },
    Send { data: AppDataAddr },
    Close,
}
```

3. Change `AppCqeData` to:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppCqeData {
    None,
    Recv { data: AppDataAddr },
    Closed,
}
```

4. Update all pattern matches in `ring.rs` from `buffer` to `data`.

- [ ] **Step 4: Add app data registration helpers**

Still in `crates/hammer-runtime/src/app/ring.rs`, add these methods to `AppRingHandle`:

```rust
pub fn alloc_data_for_bytes(&self, bytes: &[u8]) -> HammerResult<AppDataAddr> {
    let addr = self
        .data_area
        .alloc()
        .ok_or_else(|| HammerError::internal("app data area is full"))?;
    self.data_area.write(addr, bytes)
}

pub fn read_data(&self, addr: AppDataAddr) -> HammerResult<std::vec::Vec<u8>> {
    self.data_area.read(addr)
}

pub fn release_data(&self, addr: AppDataAddr) -> HammerResult<()> {
    self.data_area.release(addr)
}
```

- [ ] **Step 5: Update send descriptor conversion**

First replace descriptor push/pop helpers so they use `LockFreeRing`, not `AppRingState`:

```rust
pub fn try_push_submission_descriptor(&self, descriptor: AppSqeDescriptor) -> HammerResult<()> {
    self.submissions
        .enqueue_sp(descriptor)
        .map_err(|_| HammerError::internal("app submission ring is full"))
}

pub fn pop_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
    self.submissions.dequeue_sc()
}

pub fn try_push_completion_descriptor(&self, descriptor: AppCqeDescriptor) -> HammerResult<()> {
    self.completions
        .enqueue_sp(descriptor)
        .map_err(|_| HammerError::internal("app completion ring is full"))
}

pub fn pop_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
    self.completions.dequeue_sc()
}
```

Remove the old `AppRingState`-backed helper bodies. The only remaining `RefCell` use in `ring.rs` should be process-local pending operation bookkeeping, not SQ/CQ transport.

Replace `AppRingHandle::try_push_submission` with this implementation so `Send` SQEs are converted with access to the ring's app data area:

```rust
pub fn try_push_submission(&self, sqe: AppSqe) -> HammerResult<()> {
    match sqe {
        AppSqe::Send { user_data, op, send } => {
            let bytes = send.lease().copy_current()?;
            let data = self.alloc_data_for_bytes(&bytes)?;
            send.release();
            if let Err(err) = self.try_push_submission_descriptor(AppSqeDescriptor::new(
                AppOpcode::Send,
                user_data,
                AppObjectRef::Operation(op),
                AppSqeData::Send { data },
            )) {
                self.release_data(data)?;
                return Err(err);
            }
            Ok(())
        }
        other => {
            let descriptor = sqe_into_descriptor(other)?;
            self.try_push_submission_descriptor(descriptor)
        }
    }
}
```

After this change, replace `sqe_into_descriptor` with this helper so it only accepts non-send SQEs:

```rust
fn sqe_into_descriptor(sqe: AppSqe) -> HammerResult<AppSqeDescriptor> {
    match sqe {
        AppSqe::Nop { user_data } => Ok(AppSqeDescriptor::new(
            AppOpcode::Nop,
            user_data,
            AppObjectRef::None,
            AppSqeData::Nop,
        )),
        AppSqe::Recv { user_data, op, max } => Ok(AppSqeDescriptor::new(
            AppOpcode::Recv,
            user_data,
            AppObjectRef::Operation(op),
            AppSqeData::Recv {
                max_len: max as u32,
            },
        )),
        AppSqe::Close { user_data, op } => Ok(AppSqeDescriptor::new(
            AppOpcode::Close,
            user_data,
            AppObjectRef::Operation(op),
            AppSqeData::Close,
        )),
        AppSqe::Send { .. } => Err(HammerError::internal(
            "send sqe conversion requires app ring data area",
        )),
    }
}
```

- [ ] **Step 6: Update completion conversion**

In `AppRingHandle::try_complete_recv_lease`, copy the lease into app data and release the dataplane lease:

```rust
pub fn try_complete_recv_lease(
    &self,
    op: AppOpId,
    lease: AppBufferLease,
    fin: bool,
) -> HammerResult<()> {
    let bytes = lease.copy_current()?;
    lease.release();
    let data = self.alloc_data_for_bytes(&bytes)?;
    self.try_complete_recv_data(op, data, fin)
}
```

Add:

```rust
pub fn try_complete_recv_data(
    &self,
    op: AppOpId,
    data: AppDataAddr,
    fin: bool,
) -> HammerResult<()> {
    let (pending_index, pending) = {
        let pending = self.pending_submissions.borrow();
        pending.find_op(op, AppOpcode::Recv).ok_or_else(|| {
            HammerError::internal(format!(
                "pending recv submission missing for app op {}",
                op.value()
            ))
        })?
    };
    if !matches!(pending.payload, AppSqeData::Recv { .. }) {
        return Err(HammerError::internal("pending app op is not recv"));
    }
    let descriptor = AppCqeDescriptor::new(
        pending.user_data,
        data.len() as i32,
        if fin {
            AppCqeFlags::BUFFER.union(AppCqeFlags::FIN)
        } else {
            AppCqeFlags::BUFFER
        },
        pending.object,
        AppCqeData::Recv { data },
    );
    self.try_push_completion_descriptor(descriptor)?;
    self.pending_submissions.borrow_mut().remove_at(pending_index);
    Ok(())
}
```

- [ ] **Step 7: Update app receive object conversion**

Change `cqe_from_descriptor` and `completion_entry_from_descriptor` so `AppCqeData::Recv { data }` constructs `AppRecv` from app data, not `AppBufferLease`.

Use this temporary compatibility shape:

```rust
#[derive(Debug)]
pub struct AppRecv {
    data: AppDataAddr,
    ring: AppRingHandle,
}

impl AppRecv {
    pub fn data(&self) -> AppDataAddr {
        self.data
    }

    pub fn copy_current(&self) -> HammerResult<Vec<u8>> {
        self.ring.read_data(self.data)
    }

    pub fn release(self) {
        let _ = self.ring.release_data(self.data);
    }
}
```

Then update call sites in tests to use `recv.copy_current()` instead of `recv.lease().copy_current()`.

- [ ] **Step 8: Run the focused descriptor payload test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_descriptors_use_data_area_addresses_not_dataplane_buffer_indexes -- --exact
```

Expected: PASS.

- [ ] **Step 9: Run app ring tests and fix compile fallout**

Run:

```bash
cargo test -p hammer-runtime --test app_ring
```

Expected: PASS after updating all `buffer` payload patterns to `data` payload patterns and replacing receive lease assertions with app data assertions.

- [ ] **Step 10: Commit**

Run:

```bash
git add crates/hammer-runtime/src/app/ring.rs crates/hammer-runtime/src/app/context.rs crates/hammer-runtime/src/app/mod.rs crates/hammer-runtime/tests/app_ring.rs
git commit -m "hammer-runtime(Refactor): use app data addresses in rings"
```

Expected: one commit that removes stable app-ring dependency on `BufferIndex` payloads.

---

### Task 5: Make TCP Receive Copy Into App Data Area and Free Packet Buffers

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/tests/tcp_established_receive.rs`

- [ ] **Step 1: Strengthen the TCP established receive test**

In `crates/hammer-service/tests/tcp_established_receive.rs`, update the in-order payload test so it captures buffer usage before and after delivering `hello`:

```rust
let before_in_use = graph.runtime.packet_buffers().in_use_buffers();

send_packet(
    &graph.runtime,
    graph.established,
    tcp_packet(
        REMOTE,
        REMOTE_PORT,
        LOCAL,
        LOCAL_PORT,
        CLIENT_ISN + 1,
        SERVER_ISN + 1,
        ACK,
        b"hello",
    ),
);

assert_eq!(graph.runtime.run_ready_nodes().expect("run established"), 2);
let after_in_use = graph.runtime.packet_buffers().in_use_buffers();
assert_eq!(
    after_in_use, before_in_use,
    "TCP receive must not leave app-owned data in dataplane packet buffers"
);
```

Also change the completion assertion to read app data:

```rust
let completion = app_ring.pop_completion().expect("recv completion");
assert_eq!(completion.user_data(), Some(AppUserData::new(91)));
match completion.kind() {
    AppCqeKind::Recv { op, fin, recv } => {
        assert_eq!(*op, app_op);
        assert!(!fin);
        assert_eq!(recv.copy_current().expect("recv payload"), b"hello");
    }
    other => panic!("unexpected completion: {other:?}"),
}
```

- [ ] **Step 2: Run the failing TCP receive test**

Run:

```bash
cargo test -p hammer-service --test tcp_established_receive tcp_established_in_order_payload_advances_rcv_nxt_and_completes_recv -- --exact
```

Expected: FAIL until app receive completion no longer keeps a dataplane packet buffer lease alive.

- [ ] **Step 3: Update session app completion**

In `crates/hammer-service/src/session/app.rs`, keep the method name but let the runtime ring copy into app data and release the packet lease:

```rust
pub fn complete_recv(&self, op: AppOpId, lease: AppBufferLease, fin: bool) -> CoreResult<()> {
    let Some(ring) = self.ring_for_op(op) else {
        lease.release();
        return Ok(());
    };
    ring.try_complete_recv_lease(op, lease, fin)
}
```

This method should already have this shape after the earlier recv-user-data fix. Verify it does not call `AppCqe::recv(None, ...)`.

- [ ] **Step 4: Update TCP completion paths**

In `crates/hammer-service/src/transport/tcp/session.rs`, keep:

```rust
context.app().complete_recv(op, lease, fin)?;
```

Do not wrap `lease` in `AppRecv`.

In `crates/hammer-service/src/transport/tcp/established.rs`, keep the temporary payload buffer allocation, but rely on `complete_recv_buffer` to release it:

```rust
queue.complete_recv_buffer(
    session_id,
    AppBufferLease::from_buffer(runtime.packet_buffers().clone(), payload_index),
    false,
)?;
```

Do not reuse `payload_index` after calling `complete_recv_buffer`.

- [ ] **Step 5: Run the TCP receive test**

Run:

```bash
cargo test -p hammer-service --test tcp_established_receive tcp_established_in_order_payload_advances_rcv_nxt_and_completes_recv -- --exact
```

Expected: PASS.

- [ ] **Step 6: Run all TCP receive tests**

Run:

```bash
cargo test -p hammer-service --test tcp_established_receive
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add crates/hammer-service/src/session/app.rs crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/transport/tcp/established.rs crates/hammer-service/tests/tcp_established_receive.rs
git commit -m "hammer-service(Refactor): copy tcp receive into app data area"
```

Expected: one commit proving TCP receive does not hand dataplane packet buffers to app workers.

---

### Task 6: Add IPC Reservation Types Without Implementing IPC

**Files:**
- Modify: `crates/hammer-runtime/src/app/layout.rs`
- Modify: `crates/hammer-runtime/tests/app_ring.rs`

- [ ] **Step 1: Write the failing IPC reservation test**

Append this test to `crates/hammer-runtime/tests/app_ring.rs`:

```rust
use hammer_runtime::app::{AppRingIpcReservation, AppRingMemoryKind};

#[test]
fn app_ring_ipc_reservation_is_layout_only_and_has_no_file_descriptor() {
    let reservation = AppRingIpcReservation::new(4096, 4, 8, 8, 2048, 64);

    assert_eq!(reservation.memory_kind(), AppRingMemoryKind::SharedMemory);
    assert_eq!(reservation.page_size(), 4096);
    assert_eq!(reservation.producer_consumer_page_count(), 4);
    assert_eq!(reservation.export().submission_capacity(), 8);
    assert_eq!(reservation.export().completion_capacity(), 8);
    assert_eq!(reservation.export().data_chunk_size(), 2048);
    assert_eq!(reservation.export().data_chunk_count(), 64);
    assert_eq!(reservation.export().cacheline_size(), 64);
    assert_eq!(reservation.export().submission_ring_offset() % 64, 0);
    assert_eq!(reservation.export().completion_ring_offset() % 64, 0);
    assert_eq!(reservation.export().fill_ring_offset() % 64, 0);
    assert_eq!(reservation.export().data_area_offset() % 64, 0);
}
```

- [ ] **Step 2: Run the failing IPC reservation test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_ring_ipc_reservation_is_layout_only_and_has_no_file_descriptor -- --exact
```

Expected: FAIL because `AppRingIpcReservation` does not exist.

- [ ] **Step 3: Add IPC reservation type**

Append this to `crates/hammer-runtime/src/app/layout.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppRingIpcReservation {
    page_size: usize,
    producer_consumer_page_count: usize,
    export: AppRingExport,
}

impl AppRingIpcReservation {
    pub fn new(
        page_size: usize,
        producer_consumer_page_count: usize,
        submission_capacity: usize,
        completion_capacity: usize,
        data_chunk_size: usize,
        data_chunk_count: usize,
    ) -> Self {
        let layout = AppRingLayout::new(
            submission_capacity,
            completion_capacity,
            data_chunk_size,
            data_chunk_count,
        );
        Self {
            page_size,
            producer_consumer_page_count,
            export: AppRingExport::new(AppRingMemoryKind::SharedMemory, layout),
        }
    }

    #[inline]
    pub const fn page_size(self) -> usize {
        self.page_size
    }

    #[inline]
    pub const fn producer_consumer_page_count(self) -> usize {
        self.producer_consumer_page_count
    }

    #[inline]
    pub const fn memory_kind(self) -> AppRingMemoryKind {
        self.export.memory_kind()
    }

    #[inline]
    pub const fn export(self) -> AppRingExport {
        self.export
    }
}
```

- [ ] **Step 4: Re-export IPC reservation type**

Modify `crates/hammer-runtime/src/app/mod.rs`:

```rust
pub use layout::{AppRingExport, AppRingIpcReservation, AppRingLayout, AppRingMemoryKind};
```

- [ ] **Step 5: Run the IPC reservation test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_ring_ipc_reservation_is_layout_only_and_has_no_file_descriptor -- --exact
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add crates/hammer-runtime/src/app/layout.rs crates/hammer-runtime/src/app/mod.rs crates/hammer-runtime/tests/app_ring.rs
git commit -m "hammer-runtime(Feat): reserve app ring ipc layout"
```

Expected: one commit with IPC reservation metadata only, no fd passing and no cross-process wakeup implementation.

---

### Task 7: Remove Stable App Packet-Buffer Lease Exposure

**Files:**
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-runtime/src/app/context.rs`
- Modify: `crates/hammer-runtime/tests/app_ring.rs`
- Modify: `crates/hammer-service/tests/tcp_established_receive.rs`

- [ ] **Step 1: Add a source-level guard test**

Append this test to `crates/hammer-runtime/tests/app_ring.rs`:

```rust
#[test]
fn app_cqe_payloads_do_not_expose_dataplane_buffer_index() {
    let source = include_str!("../src/app/ring.rs");
    assert!(
        !source.contains("AppCqeData::Recv { buffer"),
        "AppCqeData::Recv must carry AppDataAddr, not BufferIndex"
    );
    assert!(
        !source.contains("AppSqeData::Send { buffer"),
        "AppSqeData::Send must carry AppDataAddr, not BufferIndex"
    );
}
```

- [ ] **Step 2: Run the source guard test**

Run:

```bash
cargo test -p hammer-runtime --test app_ring app_cqe_payloads_do_not_expose_dataplane_buffer_index -- --exact
```

Expected: PASS after Task 4.

- [ ] **Step 3: Remove public receive lease dependency**

In `crates/hammer-runtime/src/app/ring.rs`, ensure public app receive APIs are data-area based:

```rust
impl AppRecv {
    #[inline]
    pub fn data(&self) -> AppDataAddr {
        self.data
    }

    #[inline]
    pub fn copy_current(&self) -> HammerResult<Vec<u8>> {
        self.ring.read_data(self.data)
    }

    #[inline]
    pub fn release(self) {
        let _ = self.ring.release_data(self.data);
    }
}
```

Remove or make `pub(crate)` any `AppRecv::lease`, `AppRecv::into_lease`, and `AppRecv::into_send` methods that expose dataplane packet buffers.

- [ ] **Step 4: Replace tests that expect zero-copy packet buffer reuse**

In `crates/hammer-runtime/tests/app_ring.rs`, replace pointer-equality assertions for recv buffers with payload equality assertions:

```rust
assert_eq!(recv.copy_current().expect("recv payload"), b"complete-recv");
```

Do not assert that an app recv pointer equals a dataplane packet buffer pointer.

- [ ] **Step 5: Run app and TCP receive tests**

Run:

```bash
cargo test -p hammer-runtime --test app_ring
cargo test -p hammer-service --test tcp_established_receive
```

Expected: both commands PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add crates/hammer-runtime/src/app/ring.rs crates/hammer-runtime/src/app/context.rs crates/hammer-runtime/tests/app_ring.rs crates/hammer-service/tests/tcp_established_receive.rs
git commit -m "hammer-runtime(Refactor): hide dataplane buffers from app recv"
```

Expected: one commit removing stable app receive exposure to dataplane packet buffers.

---

### Task 8: Verification and Boundary Review

**Files:**
- Inspect only; no required edits unless a check fails.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt --all
```

Expected: exits 0.

- [ ] **Step 2: Run focused runtime tests**

Run:

```bash
cargo test -p hammer-infra --test ring
cargo test -p hammer-runtime --test app_ring
```

Expected: both commands PASS.

- [ ] **Step 3: Run focused service tests**

Run:

```bash
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service transport::tcp::session::tests
```

Expected: both commands PASS.

- [ ] **Step 4: Run boundary scans**

Run:

```bash
rg -n "AppCqeData::Recv \\{ buffer|AppSqeData::Send \\{ buffer|AppRecv::lease|AppRecv::into_lease|AppRecv::into_send" crates/hammer-runtime/src/app crates/hammer-runtime/tests crates/hammer-service/src crates/hammer-service/tests
```

Expected: no output.

Run:

```bash
rg -n "DataPlaneBuffers|BufferIndex|AppBufferLease" crates/hammer-runtime/src/app/layout.rs crates/hammer-runtime/src/app/data.rs
```

Expected: no output from `layout.rs`; `data.rs` may mention none of these types.

Run:

```bash
rg -n "ptr::copy_nonoverlapping|for \\(offset, byte\\) in bytes\\.iter\\(\\)\\.copied\\(\\)\\.enumerate\\(\\)|for offset in 0\\.\\.len" crates/hammer-runtime/src/app/data.rs
```

Expected: output includes `ptr::copy_nonoverlapping`; no output for either byte-loop pattern.

Run:

```bash
rg -n "LockFreeRing|LockFreeRingHeadTail|LockFreeRingCursors|AtomicU32|CacheLine" crates/hammer-infra/src/ring.rs
```

Expected: output includes the infra lock-free ring, DPDK-style head/tail cursor blocks, atomic cursor storage, and cacheline-separated cursor fields.

Run:

```bash
rg -n "Rc<RefCell<AppRingState|LocalRing<|Mutex<|AppRingState<" crates/hammer-runtime/src/app/ring.rs
```

Expected: no output for SQ/CQ/fill transport. If `AppRingState` remains for unrelated compatibility code, delete it before completing this task; app-worker/dataplane communication must use `LockFreeRing`.

- [ ] **Step 5: Run broader verification**

Run:

```bash
cargo test -p hammer-runtime
cargo test -p hammer-service
```

Expected: both commands PASS.

- [ ] **Step 6: Review for P1 risks**

Inspect these files:

```bash
sed -n '1,260p' crates/hammer-runtime/src/app/data.rs
sed -n '1,260p' crates/hammer-runtime/src/app/layout.rs
sed -n '1,940p' crates/hammer-runtime/src/app/ring.rs
sed -n '1,220p' crates/hammer-service/src/session/app.rs
sed -n '120,180p' crates/hammer-service/src/transport/tcp/established.rs
```

Confirm:

- `AppDataArea::release` invalidates stale addresses through generation increments.
- `AppDataArea` methods take `&self` and use atomic chunk metadata, so app-worker/dataplane access does not require `Rc<RefCell<_>>`.
- `AppDataArea::write` and `AppDataArea::read` use `ptr::copy_nonoverlapping` only after validating chunk identity, generation, capacity, and length.
- Unsafe data copies have local `// SAFETY:` comments explaining non-overlap, in-bounds chunk range, and single-owner ring handoff.
- `AppRingExport` contains only integers/enums and no process-local handles.
- `AppRingExport::cacheline_size()` returns `64`, and SQ/CQ/fill/data offsets are all cacheline aligned.
- `LockFreeRingHeadTail` is `#[repr(C, align(64))]` and `LockFreeRingCursors` places producer state at cacheline 0 and consumer state at cacheline 1.
- App SQ/CQ/fill transport is `LockFreeRing`, not `LocalRing`, `Rc<RefCell<_>>`, `Mutex`, or a runtime-local queue.
- `try_complete_recv_lease` releases the dataplane packet lease after copying.
- CQ-full failure does not leak app data chunks; if `try_push_completion_descriptor` fails, release the newly allocated `AppDataAddr` before returning the error.
- SQ-full failure does not leak app data chunks; if `try_push_submission_descriptor` fails after allocating send data, release the newly allocated `AppDataAddr` before returning the error.

- [ ] **Step 7: Commit any review fixes**

If Step 6 found a leak, make the minimal fix and run:

```bash
cargo test -p hammer-runtime --test app_ring
cargo test -p hammer-service --test tcp_established_receive
git add crates/hammer-runtime/src/app crates/hammer-service/src/session/app.rs crates/hammer-service/src/transport/tcp/established.rs crates/hammer-runtime/tests/app_ring.rs crates/hammer-service/tests/tcp_established_receive.rs
git commit -m "hammer-runtime(Fix): close app ring data leak"
```

Expected: commit exists only if Step 6 found a real issue.

- [ ] **Step 8: Final workspace test**

Run:

```bash
cargo test --workspace
```

Expected: PASS. If unrelated `hammer-app` legacy wrappers fail, either update that crate to the new op/data-area API in a separate plan or remove it from workspace only with explicit owner approval.

---

## Self-Review

Spec coverage:

- io_uring-like SQ/CQ remains: Tasks 3 and 4 keep descriptor rings and completion descriptors.
- AF-XDP-like form is covered: Tasks 1, 2, 3, and 4 split producer/consumer rings from app-owned fixed-size data chunks.
- VPP session FIFO memory behavior is covered: Task 5 copies TCP receive payload into app data and frees packet buffers; Task 7 removes public packet-buffer recv exposure.
- IPC reservation is covered: Task 6 adds layout-only shared-memory reservation metadata without implementing fd passing.
- Existing Hammer boundaries are preserved: service TCP still owns TCP state; runtime app owns app rings/data; infra owns generic ring machinery.

Red-flag scan:

- The plan contains no banned vague markers and no "similar to" task shortcuts.
- Every code-changing step includes concrete code or exact replacement instructions.
- The only conditional step is Task 8 Step 7, and it is tied to concrete leak checks and commands.

Type consistency:

- `AppDataAddr` is introduced in Task 2 and used by `AppSqeData::Send { data }` and `AppCqeData::Recv { data }` in Task 4.
- `AppRingExport`, `AppRingLayout`, and `AppRingIpcReservation` are introduced before tests rely on them.
- `AppRecv::copy_current` replaces `AppRecv::lease().copy_current()` consistently in later tasks.

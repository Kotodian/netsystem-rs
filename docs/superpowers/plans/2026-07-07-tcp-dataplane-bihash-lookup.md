# TCP Dataplane Bihash Lookup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Replace TCP dataplane exact-match lookup tables with Hammer's VPP-style bihash while keeping existing Rust domain key types and opaque `u64` bihash values.

**Architecture:** Mature `hammer-infra::bihash` first so TCP does not depend on a container backed by `std::Vec`. Then implement `BihashKey` on existing TCP key types and migrate every `FlatHashTable` in `hammer-service/src/transport/tcp/lookup.rs` to bihash-backed indices. `BihashKey` relies on Rust `Eq` for equality; bihash values are opaque `u64` handles; business records remain in pools or existing owner storage.

**Tech Stack:** Rust 2024, `hammer_infra::{bihash, boxed::Slice, heap::Heap, pool::Pool, vec::Vec}`, `hammer_core::protocol::transport::TransportConnectionKey`, `hammer-service::transport::tcp::lookup`, VPP references `third_party/vpp/src/vnet/session/session_lookup.c`, `third_party/vpp/src/vppinfra/bihash_template_inlines.h`, and `third_party/vpp/docs/developer/corearchitecture/bihash.rst`.

## Global Constraints

- Do not add TCP lookup key wrapper types such as `TcpBihashKey`, `TcpV4RouteKey`, or `TcpV6RouteKey`.
- Do not convert TCP call sites to raw `[u64; N]` or `u128` key plumbing; implement `BihashKey` on existing key types.
- Bihash values are always `u64` handles; do not make bihash generic over business value types.
- Do not leave `FlatHashTable` or `FlatHashKey` in `crates/hammer-service/src/transport/tcp/lookup.rs`.
- Do not migrate low-frequency control-plane bookkeeping tables in `crates/hammer-service/src/service.rs`.
- Do not add test-only public APIs.
- Do not add new panic or `expect` strings on the data-plane hot path.
- Use `hammer_infra::vec::Vec`, `hammer_infra::boxed::Slice`, or existing raw infra memory primitives in touched infra/dataplane code; do not add `std::vec::Vec` to bihash or TCP lookup implementation.
- Verification must be targeted; do not run `cargo test --workspace`.

## File Responsibility Map

- `crates/hammer-infra/src/bihash/mod.rs` owns the public bihash constructor surface, bucket storage, heap handle, and read-only bucket access for iteration/prefetch.
- `crates/hammer-infra/src/bihash/value.rs` owns the internal `u64` free sentinel, `Kv<K>`, and `ValuePage<K, KVP>` layout.
- `crates/hammer-infra/src/bihash/alloc.rs` owns value-page allocation and freelists, using the same `Heap` as the table.
- `crates/hammer-infra/src/bihash/ops.rs` owns hot-path lookup, prefetch, insert, remove, and clear.
- `crates/hammer-infra/src/bihash/split.rs` owns slow-path split/rehash and must use infra `Vec` for temporary working storage.
- `crates/hammer-core/src/protocol/transport.rs` owns `TransportConnectionKey` hashing/default behavior for existing IPv4 and IPv6 key forms.
- `crates/hammer-service/src/transport/tcp/lookup.rs` owns TCP dataplane lookup indices and all private `PoolIndex <-> u64` handle encoding.
- `crates/hammer-infra/tests/bihash.rs` tests generic bihash storage/prefetch behavior.
- TCP tests remain near existing unit tests in `crates/hammer-service/src/transport/tcp/lookup.rs` and `crates/hammer-service/src/transport/tcp/input.rs`.
- `docs/adr/0001-tcp-dataplane-lookup-uses-bihash.md` records the architectural decision.

## Approval Section

### Approved new generic infra APIs

```rust
impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP>
{
    pub fn with_capacity_in(nbuckets: u32, heap: std::sync::Arc<hammer_infra::heap::Heap>) -> Self;
    pub fn prefetch(&self, key: &K);
    pub fn prefetch_with_hash(&self, hash: u64);
}
```

```rust
impl<K: Copy + Default, const KVP: usize> PageAlloc<K, KVP> {
    pub fn new_in(heap: std::sync::Arc<hammer_infra::heap::Heap>) -> Self;
}
```

These are generic bihash capabilities and carry no TCP business meaning.

### Approved trait implementations on existing key types

```rust
impl<A: Copy + Default> Default for TransportConnectionKey<A>;
impl BihashKey for TransportConnectionKey<std::net::Ipv4Addr>;
impl BihashKey for TransportConnectionKey<std::net::Ipv6Addr>;
impl<A: TcpListenerAddress + Default> Default for TcpListenerKey<A>;
impl<A: TcpListenerAddress> BihashKey for TcpListenerKey<A>;
```

These do not create new domain types; they make existing keys usable by bihash.

### Approved private TCP lookup helpers

```rust
fn pool_index_to_bihash_value(index: PoolIndex) -> u64;
fn pool_index_from_bihash_value(value: u64) -> PoolIndex;
```

These stay private to `crates/hammer-service/src/transport/tcp/lookup.rs` and are the handle encoding helpers required for route, pending, and listener-pending pools. Listener snapshot and TFO cache tables store existing Hammer `Vec` indices directly as `u64` values.

### Explicitly not approved

- New TCP key wrapper structs or enums.
- Public TCP-specific bihash APIs in `hammer-infra`.
- Generic business values in bihash.
- Compatibility tables that keep `FlatHashTable` beside bihash.

---

### Task 1: Mature `hammer-infra::bihash` storage and prefetch

**Files:**
- Modify: `crates/hammer-infra/src/bihash/mod.rs`
- Modify: `crates/hammer-infra/src/bihash/value.rs`
- Modify: `crates/hammer-infra/src/bihash/template.rs`
- Modify: `crates/hammer-infra/src/bihash/alloc.rs`
- Modify: `crates/hammer-infra/src/bihash/ops.rs`
- Modify: `crates/hammer-infra/src/bihash/split.rs`
- Modify: `crates/hammer-infra/src/bihash/iter.rs`
- Test: `crates/hammer-infra/tests/bihash.rs`

**Interfaces:**
- Consumes:
  - `hammer_infra::boxed::Slice<T>`
  - `hammer_infra::vec::Vec<T>`
  - `hammer_infra::heap::Heap`
  - `hammer_infra::prefetch::prefetch_read_l1`
- Produces:
  - `Bihash::with_capacity_in(nbuckets: u32, heap: Arc<Heap>) -> Self`
  - `Bihash::prefetch(&self, key: &K)`
  - `Bihash::prefetch_with_hash(&self, hash: u64)`
  - `PageAlloc::new_in(heap: Arc<Heap>) -> Self`

- [x] **Step 1: Add failing constructor and prefetch tests**

Add these tests to `crates/hammer-infra/tests/bihash.rs`:

```rust
use std::sync::Arc;
use hammer_infra::heap::Heap;

#[test]
fn bihash_with_capacity_in_uses_supplied_heap_surface() {
    let heap = Arc::new(Heap::main());
    let mut table: Bihash<u64, 7> = Bihash::with_capacity_in(8, heap);

    table.insert(10, 100);
    table.insert(11, 110);

    assert_eq!(table.nbuckets(), 8);
    assert_eq!(table.lookup(&10), Some(100));
    assert_eq!(table.lookup(&11), Some(110));
}

#[test]
fn bihash_prefetch_accepts_empty_and_present_keys() {
    let mut table: Bihash<u64, 7> = Bihash::new(8);

    table.prefetch(&42);
    table.insert(42, 420);
    table.prefetch(&42);

    assert_eq!(table.lookup(&42), Some(420));
}
```

- [x] **Step 2: Run the failing infra tests**

Run: `cargo test -p hammer-infra --test bihash bihash_with_capacity_in_uses_supplied_heap_surface bihash_prefetch_accepts_empty_and_present_keys -- --nocapture`

Expected: FAIL because `Bihash::with_capacity_in` and `Bihash::prefetch` do not exist.

- [x] **Step 3: Collapse bihash values to fixed `u64` and remove the public free-marker trait**

Change `crates/hammer-infra/src/bihash/value.rs` to this shape:

```rust
pub const FREE_U64: u64 = 0xFEEDFACE_8BADF00D;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Kv<K> {
    pub key: K,
    pub value: u64,
}

impl<K: Default> Default for Kv<K> {
    #[inline]
    fn default() -> Self {
        Self {
            key: K::default(),
            value: FREE_U64,
        }
    }
}

impl<K> Kv<K> {
    #[inline(always)]
    pub const fn is_free(&self) -> bool {
        self.value == FREE_U64
    }

    #[inline(always)]
    pub fn mark_free(&mut self) {
        self.value = FREE_U64;
    }
}

#[derive(Clone)]
pub struct ValuePage<K, const KVP: usize> {
    slots: [Kv<K>; KVP],
}

impl<K: Copy + Default, const KVP: usize> ValuePage<K, KVP> {
    pub fn new() -> Self {
        Self {
            slots: core::array::from_fn(|_| Kv::default()),
        }
    }

    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        KVP
    }

    #[inline(always)]
    pub const fn slots(&self) -> &[Kv<K>; KVP] {
        &self.slots
    }

    #[inline(always)]
    pub fn slots_mut(&mut self) -> &mut [Kv<K>; KVP] {
        &mut self.slots
    }
}
```

Remove the old public free-marker trait and its re-export from `crates/hammer-infra/src/bihash/mod.rs`. Update `crates/hammer-infra/src/bihash/template.rs` so aliases are fixed-value forms such as `pub type Bihash8x8 = Bihash<u64, 7>;`, not value-generic aliases.

- [x] **Step 4: Replace bihash bucket storage with `Slice<Bucket>` and retain the heap**

Change `crates/hammer-infra/src/bihash/mod.rs` to this shape:

```rust
use std::sync::Arc;

use crate::bihash::alloc::PageAlloc;
use crate::bihash::{BihashKey, Bucket};
use crate::boxed::Slice;
use crate::heap::Heap;

pub struct Bihash<K: BihashKey, const KVP: usize> {
    buckets: Slice<Bucket>,
    pages: PageAlloc<K, KVP>,
    heap: Arc<Heap>,
    len: usize,
    nbuckets: u32,
    log2_nbuckets: u8,
}

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    pub fn new(nbuckets: u32) -> Self {
        Self::with_capacity_in(nbuckets, Arc::new(Heap::local()))
    }

    pub fn with_capacity_in(mut nbuckets: u32, heap: Arc<Heap>) -> Self {
        if nbuckets == 0 {
            nbuckets = 1;
        }
        let actual_buckets = nbuckets.next_power_of_two();
        let log2 = actual_buckets.trailing_zeros() as u8;
        Self {
            buckets: Slice::from_elem_in(actual_buckets as usize, Bucket::empty(), heap.clone()),
            pages: PageAlloc::new_in(heap.clone()),
            heap,
            len: 0,
            nbuckets: actual_buckets,
            log2_nbuckets: log2,
        }
    }

    #[inline]
    pub(crate) fn heap(&self) -> Arc<Heap> {
        self.heap.clone()
    }

    #[inline]
    pub(crate) fn buckets(&self) -> &[Bucket] {
        self.buckets.as_slice()
    }
}
```

Keep the existing `len`, `is_empty`, `nbuckets`, `pages`, and `iter` methods, updating their bucket access to `Slice`.

- [x] **Step 5: Move `PageAlloc` pages and freelists to infra `Vec`**

Change `crates/hammer-infra/src/bihash/alloc.rs` to this storage shape:

```rust
use std::sync::Arc;

use crate::bihash::value::ValuePage;
use crate::heap::Heap;
use crate::vec::Vec;

pub struct PageAlloc<K, const KVP: usize> {
    pages: Vec<ValuePage<K, KVP>>,
    freelists: [Vec<PageId>; 8],
    heap: Arc<Heap>,
    live: usize,
}

impl<K: Copy + Default, const KVP: usize> PageAlloc<K, KVP> {
    pub fn new() -> Self {
        Self::new_in(Arc::new(Heap::local()))
    }

    pub fn new_in(heap: Arc<Heap>) -> Self {
        Self {
            pages: Vec::with_capacity_in(0, heap.clone()),
            freelists: core::array::from_fn(|_| Vec::with_capacity_in(0, heap.clone())),
            heap,
            live: 0,
        }
    }

    #[inline]
    pub(crate) fn heap(&self) -> Arc<Heap> {
        self.heap.clone()
    }
}
```

Keep the existing allocation, free, access, and `Default` behavior, replacing every `std::vec::Vec` constructor with `hammer_infra::vec::Vec` constructors using the retained heap.

- [x] **Step 6: Add bihash prefetch operations**

Add to `crates/hammer-infra/src/bihash/ops.rs`:

```rust
use crate::prefetch::prefetch_read_l1;

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    #[inline(always)]
    pub fn prefetch(&self, key: &K) {
        self.prefetch_with_hash(key.hash());
    }

    #[inline(always)]
    pub fn prefetch_with_hash(&self, hash: u64) {
        if self.buckets.is_empty() {
            return;
        }
        let bucket_idx = (hash as u32) & (self.nbuckets - 1);
        let bucket_ptr = self.buckets.as_ptr().wrapping_add(bucket_idx as usize);
        prefetch_read_l1(bucket_ptr);
    }
}
```

- [x] **Step 7: Move split slow-path temporary vectors to infra `Vec`**

In `crates/hammer-infra/src/bihash/split.rs`, replace unqualified `Vec` with `crate::vec::Vec` and allocate from the table heap:

```rust
use crate::vec::Vec;

let heap = self.heap();
let mut working: Vec<Kv<K>> = Vec::with_capacity_in(0, heap.clone());
let mut page_ids: Vec<PageId> = Vec::with_capacity_in(new_count as usize, heap);
```

Remove the two `eprintln!` debug dumps from `place_in_run`; keep one explicit panic path only if the existing behavior requires it:

```rust
panic!("bihash page run is full");
```

- [x] **Step 8: Run targeted infra tests**

Run: `cargo test -p hammer-infra --test bihash -- --nocapture`

Expected: PASS.

- [x] **Step 9: Verify bihash implementation no longer uses `std::Vec` or public free-marker traits**

Run: `rg "std::vec::Vec|vec!\\[|pub trait .*Free" crates/hammer-infra/src/bihash crates/hammer-infra/tests/bihash.rs`

Expected: no matches. Intentional `hammer_infra::vec::Vec` imports may still appear as `use crate::vec::Vec;`; this check only rejects `std::Vec`, `vec![]`, and public free-marker traits.

- [x] **Step 10: Commit**

```bash
git add crates/hammer-infra/src/bihash/mod.rs crates/hammer-infra/src/bihash/value.rs crates/hammer-infra/src/bihash/template.rs crates/hammer-infra/src/bihash/alloc.rs crates/hammer-infra/src/bihash/ops.rs crates/hammer-infra/src/bihash/split.rs crates/hammer-infra/src/bihash/iter.rs crates/hammer-infra/tests/bihash.rs
git commit -m "hammer-infra(Refactor): back bihash with Hammer memory"
```

---

### Task 2: Add existing TCP lookup key support for bihash

**Files:**
- Modify: `crates/hammer-core/src/protocol/transport.rs`
- Modify: `crates/hammer-service/src/transport/tcp/lookup.rs`
- Test: `crates/hammer-core/tests/protocol_tcp.rs`
- Test: `crates/hammer-service/src/transport/tcp/lookup.rs`

**Interfaces:**
- Consumes:
  - `hammer_infra::bihash::BihashKey`
  - `TransportConnectionKey<Ipv4Addr>`
  - `TransportConnectionKey<Ipv6Addr>`
  - `TcpListenerKey<A>`
- Produces:
  - `Default` and `BihashKey` impls for existing TCP lookup key types
  - private `PoolIndex <-> u64` encoding in `lookup.rs`

- [x] **Step 1: Add failing core key round-trip tests**

Add these tests to `crates/hammer-core/tests/protocol_tcp.rs`:

```rust
use hammer_infra::bihash::Bihash;
use std::net::{Ipv4Addr, Ipv6Addr};

#[test]
fn transport_connection_key_v4_works_as_bihash_key() {
    let key = TransportConnectionKey::new(
        0,
        Ipv4Addr::new(10, 0, 0, 1),
        1234,
        Ipv4Addr::new(10, 0, 0, 2),
        80,
    );
    let mut table: Bihash<TransportConnectionKey<Ipv4Addr>, 3> = Bihash::new(8);

    table.insert(key, 99);

    assert_eq!(table.lookup(&key), Some(99));
}

#[test]
fn transport_connection_key_v6_works_as_bihash_key() {
    let key = TransportConnectionKey::new(
        0,
        Ipv6Addr::LOCALHOST,
        1234,
        Ipv6Addr::UNSPECIFIED,
        443,
    );
    let mut table: Bihash<TransportConnectionKey<Ipv6Addr>, 1> = Bihash::new(8);

    table.insert(key, 199);

    assert_eq!(table.lookup(&key), Some(199));
}
```

- [x] **Step 2: Run the failing core tests**

Run: `cargo test -p hammer-core --test protocol_tcp transport_connection_key_v4_works_as_bihash_key transport_connection_key_v6_works_as_bihash_key -- --nocapture`

Expected: FAIL because `TransportConnectionKey<Ipv4Addr>` and `TransportConnectionKey<Ipv6Addr>` do not implement `BihashKey` and `Default`.

- [x] **Step 3: Implement `Default` and `BihashKey` for existing transport keys**

In `crates/hammer-core/src/protocol/transport.rs`, replace the `FlatHashKey` dependency with `BihashKey`:

```rust
use hammer_infra::bihash::BihashKey;
```

Add:

```rust
impl<A: Copy + Default> Default for TransportConnectionKey<A> {
    #[inline]
    fn default() -> Self {
        Self {
            scope_id: 0,
            local_addr: A::default(),
            remote_addr: A::default(),
            ports: 0,
        }
    }
}

impl BihashKey for TransportConnectionKey<Ipv4Addr> {
    #[inline(always)]
    fn hash(self) -> u64 {
        let packed = (u128::from(self.scope_id) << 96)
            | (u128::from(u32::from(self.local_addr)) << 64)
            | (u128::from(u32::from(self.remote_addr)) << 32)
            | u128::from(self.ports);
        splitmix64((packed ^ (packed >> 64)) as u64)
    }

    #[inline(always)]
}

impl BihashKey for TransportConnectionKey<Ipv6Addr> {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&[
            fold_u128(u128::from(self.local_addr)),
            fold_u128(u128::from(self.remote_addr)),
            u64::from(self.scope_id),
            u64::from(self.ports),
        ]) as u64
    }

    #[inline(always)]
}
```

Remove the `FlatHashKey` implementations for `TransportConnectionKey<Ipv4Addr>`, `TransportConnectionKey<Ipv6Addr>`, and `TransportConnectionKey<IpAddr>` from this file.

- [x] **Step 4: Add listener key and pool-index encoding tests**

Add tests inside the existing `#[cfg(test)] mod tests` in `crates/hammer-service/src/transport/tcp/lookup.rs`:

```rust
use hammer_infra::bihash::Bihash;

#[test]
fn tcp_listener_key_works_as_bihash_key() {
    let key = TcpV4ListenerKey::new(0, Ipv4Addr::new(127, 0, 0, 1), 7300);
    let mut table: Bihash<TcpV4ListenerKey, 1> = Bihash::new(8);

    table.insert(key, 77);

    assert_eq!(table.lookup(&key), Some(77));
}

#[test]
fn pool_index_bihash_value_round_trip() {
    let index = PoolIndex::new(17, 23);
    let value = pool_index_to_bihash_value(index);

    assert_eq!(pool_index_from_bihash_value(value), index);
}
```

- [x] **Step 5: Run the failing service lookup tests**

Run: `cargo test -p hammer-service transport::tcp::lookup::tests::tcp_listener_key_works_as_bihash_key transport::tcp::lookup::tests::pool_index_bihash_value_round_trip -- --nocapture`

Expected: FAIL because `TcpListenerKey<A>` does not implement `BihashKey` and the private pool-index encoding functions do not exist.

- [x] **Step 6: Implement listener key support without new wrapper types**

In `crates/hammer-service/src/transport/tcp/lookup.rs`, change the listener address trait bound:

```rust
pub trait TcpListenerAddress: Copy + Eq {
    type Ip;
    type Key: BihashKey + Default;

    fn key(scope_id: u32, local_addr: Self::Ip, local_port: u16) -> Self::Key;
}
```

Derive `Default` for marker address types:

```rust
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TcpIpv4ListenerAddress;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TcpIpv6ListenerAddress;
```

Add:

```rust
impl<A> Default for TcpListenerKey<A>
where
    A: TcpListenerAddress + Default,
{
    #[inline]
    fn default() -> Self {
        Self {
            words: [0, 0],
            address: A::default(),
        }
    }
}

impl<A: TcpListenerAddress> BihashKey for TcpListenerKey<A> {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&[fold_u128(self.words[0]), fold_u128(self.words[1])])
    }

    #[inline(always)]
}
```

Remove `impl<A: TcpListenerAddress> FlatHashKey for TcpListenerKey<A>`.

- [x] **Step 7: Add private pool-index encoding**

Add near the TCP lookup constants in `crates/hammer-service/src/transport/tcp/lookup.rs`:

```rust
#[inline(always)]
fn pool_index_to_bihash_value(index: PoolIndex) -> u64 {
    let value = (u64::from(index.generation()) << 32) | u64::from(index.slot());
    debug_assert_ne!(value, hammer_infra::bihash::FREE_U64);
    value
}

#[inline(always)]
fn pool_index_from_bihash_value(value: u64) -> PoolIndex {
    PoolIndex::new(value as u32, (value >> 32) as u32)
}
```

- [x] **Step 8: Run targeted key tests**

Run: `cargo test -p hammer-core --test protocol_tcp transport_connection_key_v4_works_as_bihash_key transport_connection_key_v6_works_as_bihash_key -- --nocapture`

Expected: PASS.

Run: `cargo test -p hammer-service transport::tcp::lookup::tests::tcp_listener_key_works_as_bihash_key transport::tcp::lookup::tests::pool_index_bihash_value_round_trip -- --nocapture`

Expected: PASS.

- [x] **Step 9: Commit**

```bash
git add crates/hammer-core/src/protocol/transport.rs crates/hammer-core/tests/protocol_tcp.rs crates/hammer-service/src/transport/tcp/lookup.rs
git commit -m "hammer-service(Refactor): make TCP lookup keys bihash-ready"
```

---

### Task 3: Migrate all TCP lookup indices from `FlatHashTable` to bihash

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/lookup.rs`
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Test: `crates/hammer-service/src/transport/tcp/lookup.rs`
- Test: `crates/hammer-service/src/transport/tcp/input.rs`

**Interfaces:**
- Consumes:
  - `Bihash::lookup(&K) -> Option<u64>`
  - `Bihash::insert(K, u64)`
  - `Bihash::remove(&K) -> bool`
  - `Bihash::prefetch(&K)`
  - `pool_index_to_bihash_value(index: PoolIndex) -> u64`
  - `pool_index_from_bihash_value(value: u64) -> PoolIndex`
- Produces:
  - No `FlatHashTable` or `FlatHashKey` usage in `crates/hammer-service/src/transport/tcp/lookup.rs`
  - Bihash-backed session route, pending route, listener snapshot, listener pending, listener count, and TFO cache indices

- [x] **Step 1: Add regression tests for every migrated lookup surface**

Extend existing tests in `crates/hammer-service/src/transport/tcp/lookup.rs` with these assertions:

```rust
#[test]
fn tcp_connection_route_index_bihash_keeps_v4_and_v6_routes() {
    let mut index = TcpConnectionRouteIndex::empty();
    let owner = DataWorkerId::new(0);
    let v4_session = SessionId::from(PoolIndex::new(1, 1));
    let v6_session = SessionId::from(PoolIndex::new(2, 1));
    let v4_local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1000);
    let v4_remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 2000);
    let v6_local = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1001);
    let v6_remote = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 2001);

    index.upsert(v4_session, None, Some(v4_local), v4_remote, owner, TcpInputNext::Established);
    index.upsert(v6_session, None, Some(v6_local), v6_remote, owner, TcpInputNext::Established);

    assert_eq!(
        index.lookup_by_tuple(v4_local, v4_remote),
        Some((v4_session, owner, TcpInputNext::Established))
    );
    assert_eq!(
        index.lookup_by_tuple(v6_local, v6_remote),
        Some((v6_session, owner, TcpInputNext::Established))
    );
}

#[test]
fn tcp_listener_lookup_bihash_preserves_lookup_value() {
    let key = TcpV4ListenerKey::new(0, Ipv4Addr::new(127, 0, 0, 1), 7300);
    let value = TcpLookupValue {
        id: 7,
        owner_worker: DataWorkerId::new(2),
        capabilities: TcpCapabilities {
            max_segment_size: Some(1200),
            window_scale: Some(4),
            sack: true,
            timestamps: true,
            ecn: true,
            accurate_ecn: false,
            fast_open: true,
        },
    };
    let mut table = TcpListenerTable::<TcpIpv4ListenerAddress>::empty();

    table.insert(key, value);

    assert_eq!(table.lookup(key), Some(value));
}

#[test]
fn tcp_fast_open_cache_bihash_updates_existing_tuple() {
    let mut state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 50_000);
    let first = TcpFastOpenCookie::from_bytes(&[1, 2, 3, 4]).expect("first cookie");
    let second = TcpFastOpenCookie::from_bytes(&[5, 6, 7, 8]).expect("second cookie");

    state.remember_fast_open_cookie(local, remote, first, Some(1200));
    state.remember_fast_open_cookie(local, remote, second, Some(1300));

    assert_eq!(state.fast_open_cookie(local, remote), Some((second, Some(1300))));
}
```

- [x] **Step 2: Run the failing regression tests**

Run: `cargo test -p hammer-service transport::tcp::lookup::tests::tcp_connection_route_index_bihash_keeps_v4_and_v6_routes transport::tcp::lookup::tests::tcp_listener_lookup_bihash_preserves_lookup_value transport::tcp::lookup::tests::tcp_fast_open_cache_bihash_updates_existing_tuple -- --nocapture`

Expected: FAIL until the tables are migrated and listener values are stored behind bihash `u64` handles.

- [x] **Step 3: Replace route indices with bihash tables**

In `TcpConnectionRouteIndex`, replace fields with:

```rust
entries: Pool<TcpConnectionRouteEntry>,
session_slots: Bihash<u64, 7>,
connection_slots: Bihash<u64, 7>,
tuple_slots_v4: Bihash<TransportConnectionKey<Ipv4Addr>, 3>,
tuple_slots_v6: Bihash<TransportConnectionKey<Ipv6Addr>, 1>,
```

In `TcpPendingRouteIndex`, replace fields with:

```rust
entries: Pool<TcpPendingRouteEntry>,
session_slots: Bihash<u64, 7>,
tuple_slots_v4: Bihash<TransportConnectionKey<Ipv4Addr>, 3>,
tuple_slots_v6: Bihash<TransportConnectionKey<Ipv6Addr>, 1>,
```

Update `empty()` constructors to use `Bihash::new(1024)`. Replace every stored `PoolIndex` value with `pool_index_to_bihash_value(entry_index)`, and decode lookup results with `pool_index_from_bihash_value(value)`.

- [x] **Step 4: Replace tuple key handling without adding wrapper types**

Replace `TcpConnectionRouteEntry::tuple_key` and `TcpPendingRouteEntry::tuple_key` with v4/v6 methods:

```rust
#[inline]
fn tuple_key_v4(self) -> Option<TransportConnectionKey<Ipv4Addr>> {
    match (self.local?, self.remote) {
        (SocketAddr::V4(local), SocketAddr::V4(remote)) => Some(TransportConnectionKey::new(
            0,
            *local.ip(),
            local.port(),
            *remote.ip(),
            remote.port(),
        )),
        _ => None,
    }
}

#[inline]
fn tuple_key_v6(self) -> Option<TransportConnectionKey<Ipv6Addr>> {
    match (self.local?, self.remote) {
        (SocketAddr::V6(local), SocketAddr::V6(remote)) => Some(TransportConnectionKey::new(
            0,
            *local.ip(),
            local.port(),
            *remote.ip(),
            remote.port(),
        )),
        _ => None,
    }
}
```

For `lookup_by_tuple` and `prefetch_tuple`, match directly on `(local, remote)` and probe the matching v4 or v6 table. Mixed-family tuples return `None` and perform no prefetch.

- [x] **Step 5: Replace listener snapshot storage with bihash + Hammer Vec owner**

Change `TcpListenerTable<A>` to keep listener values in a growable Hammer infra vector and store the vector index in bihash:

```rust
pub struct TcpListenerTable<A: TcpListenerAddress> {
    values: Vec<TcpLookupValue>,
    entries: Bihash<A::Key, 1>,
}
```

Implement:

```rust
impl<A: TcpListenerAddress> TcpListenerTable<A> {
    #[inline]
    fn empty() -> Self {
        Self {
            values: Vec::with_capacity(64),
            entries: Bihash::new(64),
        }
    }

    #[inline]
    pub fn lookup(&self, key: A::Key) -> Option<TcpLookupValue> {
        let index = self.entries.lookup(&key)? as usize;
        self.values.get(index).copied()
    }

    #[inline]
    pub fn prefetch(&self, key: A::Key) {
        self.entries.prefetch(&key);
    }

    #[inline]
    pub fn insert(&mut self, key: A::Key, value: TcpLookupValue) {
        if let Some(raw) = self.entries.lookup(&key) {
            if let Some(slot) = self.values.get_mut(raw as usize) {
                *slot = value;
                return;
            }
        }
        let index = self.values.len() as u64;
        debug_assert_ne!(index, FREE_U64);
        self.values.push(value);
        self.entries.insert(key, index);
    }
}
```

Add a manual `Clone` that rebuilds the vector and bihash from `entries.iter()`:

```rust
impl<A: TcpListenerAddress> Clone for TcpListenerTable<A> {
    fn clone(&self) -> Self {
        let mut cloned = Self::empty();
        for (key, raw) in self.entries.iter() {
            if let Some(value) = self.values.get(*raw as usize).copied() {
                cloned.insert(*key, value);
            }
        }
        cloned
    }
}
```

- [x] **Step 6: Replace listener pending indices**

Change `TcpListenerPendingTable` fields:

```rust
entries: Pool<TcpListenerPendingEntry>,
tuple_index_v4: Bihash<TransportConnectionKey<Ipv4Addr>, 3>,
tuple_index_v6: Bihash<TransportConnectionKey<Ipv6Addr>, 1>,
listener_counts: Bihash<u32, 5>,
epoch_buckets: [Vec<PoolIndex>; TCP_LISTENER_PENDING_BUCKET_COUNT],
bucket_epochs: [u32; TCP_LISTENER_PENDING_BUCKET_COUNT],
pruned_epoch: Option<u32>,
```

Store listener counts as `u64` values and cast to `usize` at read sites:

```rust
let used = self.listener_counts.lookup(&listener_id).unwrap_or(0) as usize;
self.listener_counts.insert(listener_id, (used + 1) as u64);
```

For tuple index operations, use the same v4/v6 matching style as route lookup and store pool indices through `pool_index_to_bihash_value`.

- [x] **Step 7: Replace TFO cache index**

Change `TcpWorkerOwnedStateCacheline1`:

```rust
fast_open_cache: Vec<TcpFastOpenCacheEntry>,
fast_open_cache_index_v4: Bihash<TransportConnectionKey<Ipv4Addr>, 3>,
fast_open_cache_index_v6: Bihash<TransportConnectionKey<Ipv6Addr>, 1>,
fast_open_secrets: Vec<TcpFastOpenSecret>,
listener_pending: TcpListenerPendingTable,
listener_cookie_secrets: Vec<TcpFastOpenSecret>,
```

When inserting a TFO cache entry, keep the existing `Vec<TcpFastOpenCacheEntry>` as the owner and store the vector index as a bihash value:

```rust
let index = self.cacheline1.fast_open_cache.len() - 1;
let value = index as u64;
debug_assert_ne!(value, hammer_infra::bihash::FREE_U64);
```

Lookup decodes `value as usize` and then reads from `fast_open_cache`.

- [x] **Step 8: Replace imports and remove deprecated TCP lookup map surface**

At the top of `crates/hammer-service/src/transport/tcp/lookup.rs`, remove:

```rust
use hammer_infra::map::{FlatHashKey, FlatHashTable};
```

Add:

```rust
use hammer_infra::bihash::{Bihash, BihashKey};
```

Keep:

```rust
use hammer_infra::vec::Vec;
```

- [x] **Step 9: Run TCP lookup and input tests**

Run: `cargo test -p hammer-service transport::tcp::lookup -- --nocapture`

Expected: PASS.

Run: `cargo test -p hammer-service transport::tcp::input -- --nocapture`

Expected: PASS.

- [x] **Step 10: Run crate-level targeted check**

Run: `cargo check -p hammer-service`

Expected: PASS with no `FlatHashTable` deprecation warnings from `crates/hammer-service/src/transport/tcp/lookup.rs`.

- [x] **Step 11: Verify forbidden migration leftovers**

Run: `rg "FlatHashTable|FlatHashKey" crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-core/src/protocol/transport.rs`

Expected: no matches.

Run: `rg "TcpBihashKey|TcpV4RouteKey|TcpV6RouteKey|std::vec::Vec|pub trait .*Free" crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-core/src/protocol/transport.rs crates/hammer-infra/src/bihash`

Expected: no matches.

- [x] **Step 12: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-service/src/transport/tcp/input.rs
git commit -m "hammer-service(Refactor): migrate TCP lookup to bihash"
```

---

## Final Verification

Run these commands after all tasks:

```bash
cargo fmt --all -- --check
cargo test -p hammer-infra --test bihash -- --nocapture
cargo test -p hammer-core --test protocol_tcp -- --nocapture
cargo test -p hammer-service transport::tcp::lookup -- --nocapture
cargo test -p hammer-service transport::tcp::input -- --nocapture
cargo check -p hammer-service
rg "FlatHashTable|FlatHashKey" crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-core/src/protocol/transport.rs
rg "TcpBihashKey|TcpV4RouteKey|TcpV6RouteKey|std::vec::Vec|pub trait .*Free" crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-core/src/protocol/transport.rs crates/hammer-infra/src/bihash
rg "key_eq" crates/hammer-infra crates/hammer-core crates/hammer-service
```

Expected:

- `cargo fmt --all -- --check` passes.
- All listed targeted tests pass.
- `cargo check -p hammer-service` passes.
- The first `rg` command prints nothing.
- The second `rg` command prints nothing.
- The `key_eq` scan prints nothing.
- No workspace-wide test command is run unless explicitly requested.

## Final Review Fixes

The final code review found and the implementation fixed these blockers:

- `Bihash::insert` now scans the full linear-search bucket run before inserting or splitting, so fallback-page entries are overwritten instead of duplicated.
- `BihashKey::key_eq` was removed; `BihashKey` now inherits `Eq`, and bihash compares keys with `==`.
- `remember_fast_open_cookie` returns before mutating state for mixed-family tuples.
- `TcpListenerTable` uses `hammer_infra::vec::Vec<TcpLookupValue>` as the listener value owner instead of a fixed-capacity `Pool`, so listener snapshots no longer panic at 65 entries.
- `Bihash::insert` has a narrow debug assertion reserving `FREE_U64` as the free-slot sentinel.

## Self-Review

- Spec coverage: the plan covers full TCP dataplane lookup migration, all `FlatHashTable` users in `lookup.rs`, bihash storage maturation, existing-key `BihashKey` impls, opaque `u64` values, VPP alignment, ADR creation, and targeted verification.
- Placeholder scan: no task contains unresolved placeholder language or unbounded implementation instructions.
- Type consistency: `Bihash::with_capacity_in`, `Bihash::prefetch`, `PageAlloc::new_in`, `pool_index_to_bihash_value`, and `pool_index_from_bihash_value` are introduced before later tasks consume them.

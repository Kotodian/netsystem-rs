# infra Pool and RB-tree VPP Alignment Design

## Problem Statement

`hammer-infra::Pool` currently combines fixed-capacity storage with a generation-bearing `pool::Index`, generation checks, and a retained `Arc<Heap>`. Those choices do not match the VPP pool index contract or Hammer's main-heap ownership model.

`hammer-infra::RbTree` currently uses `Pool<Node<K, V>>` as its backing storage. VPP uses the same ownership shape: `rb_tree_t.nodes` is a Pool of `rb_node_t`. Hammer retains that Pool-backed RB-tree design and removes the fixed-capacity failure by making the underlying Pool dynamic.

The design must use Rust ownership correctly, preserve stable numeric indexes, keep VPP semantics where they apply, and avoid inventing additional storage or index wrapper types.

## VPP Reference

VPP Pool is a vector plus a pool header containing:

- `free_bitmap`;
- `free_indices`;
- `max_elts` for fixed pools;
- `opaque` consumer storage.

A VPP pool index is the numeric vector position. It has no generation. `pool_get` reuses the last free index when available and otherwise grows the vector. `pool_put_index` marks the index free and appends it to the free-index vector. `pool_elts` is active count, `pool_len` is vector length, and `pool_max_len` is allocation capacity. These semantics are defined in `third_party/vpp/src/vppinfra/pool.h`.

VPP RB-tree does use a pool for nodes. `rb_tree_t.nodes` is a pool of `rb_node_t`; node parent, left, and right fields are `u32` indexes, and the root is kept in pool header opaque storage. This is the reference implementation in `third_party/vpp/src/vppinfra/rbtree.h` and `rbtree.c`.

Hammer follows VPP at the container boundary: `RbTree` depends on the generic `hammer-infra::Pool` for node storage. The generic Pool is the shared VPP-style index-addressed container used by RB-tree, FIFO, and other infra callers.

## Solution

### Pool

VPP `pool_header_t` contains `free_bitmap`, `free_indices`, `max_elts`, and `opaque`; the pool elements are the CLIB vector itself. The concrete Hammer type is:

```rust
pub struct Pool<T, const ALIGN: usize = CACHE_LINE> {
    vector: aligned storage for `MaybeUninit<T>` using the `ALIGN` policy,
    free_bitmap: Bitmap,
    free_indices: Vec<u32>,
    max_elts: u32,
    opaque: u32,
}
```

The direct field mapping is:

```text
vector.len()       = VPP pool_len
vector.capacity()  = VPP pool_max_len
free_bitmap        = VPP pool_header.free_bitmap
free_indices       = VPP pool_header.free_indices
max_elts           = VPP pool_header.max_elts
opaque             = VPP pool_header.opaque
```

`MaybeUninit<T>` is required because VPP free positions are addressable vector positions but are not initialized `T` values. `free_bitmap` and `free_indices` are the occupancy authority. `insert` initializes a position, `get`/`get_mut` read only occupied positions, `remove` performs one `assume_init_read`, and `Drop` performs `assume_init_drop` only for occupied positions. `Vec` owns the allocation; no `Option<T>`, `Layout`, raw pointer, `PhantomData`, `Arc<Heap>`, generation array, or index wrapper is part of the Pool type.

The default representation uses Hammer's existing cache-line alignment policy. An explicit alignment is optional and uses the existing aligned allocation primitive, subject to Rust layout constraints. VPP's default `pool_get` maps to `Pool<T>`; `pool_get_aligned(..., A)` maps to `Pool<T, A>`. Alignment is selected when the Pool is created and does not change for an individual allocation.

Keep one `Pool<T, const ALIGN: usize = CACHE_LINE>` implementation with dynamic and fixed modes. The const parameter is optional for callers; `Pool<T>` uses the existing default alignment, while `Pool<T, A>` selects an explicit alignment:

- dynamic construction reserves an initial backing vector and grows when necessary;
- fixed construction preallocates `max_elts` and returns typed exhaustion instead of growing;
- free indexes are reused in VPP LIFO order;
- numeric indexes remain stable across growth;
- no generation state is stored or checked.

The Pool APIs use `u32` directly as the VPP index. There is no `pool::Index` newtype and no replacement index abstraction.

The Pool owns its aligned backing allocation and the metadata required by its state machine. It must not add a `SlotStorage` or other one-use storage wrapper.

### RB-tree

VPP's `rb_tree_t.nodes` is a Pool of `rb_node_t`; this is defined by `third_party/vpp/src/vppinfra/rbtree.h:31-37`. The RB-tree node pool uses index zero as the `T.nil` sentinel, stores the root in the Pool header's `opaque` field, and stores parent/left/right as `u32` node indexes.

Hammer follows that design. The concrete Rust shape is:

```rust
pub struct RbTree<K, V> {
    nodes: Pool<Node<K, V>>,
}
```

The node type is private and its links are numeric Pool indexes:

```rust
struct Node<K, V> {
    color: Color,
    parent: u32,
    left: u32,
    right: u32,
    key: K,
    value: V,
}
```

`RbTree` reserves Pool index zero for the VPP nil node. Its root is the Pool's `opaque` value, and its node count is derived from the Pool active count excluding nil. `with_capacity` reserves enough Pool capacity for nil plus the requested nodes; the dynamic Pool grows when needed. RB-tree insertion and deletion use Pool allocation and release, with no separate node storage abstraction and no `NodeStore`, `SlotStorage`, `NodeIndex`, or `PoolIndex`.

The public RB-tree interface remains key/value based. Node indexes are private implementation details, while the Pool index itself remains the VPP numeric `u32` value.

### Heap ownership

Hammer's main heap is registered as the Rust global allocator. The default Pool path uses that process-wide main-heap backend and does not create or retain `Arc<Heap>`.

VPP stores allocator provenance with the allocated vector so growth and free use the original allocator. If Hammer later has a real non-main/SVM Pool caller, that path may retain the minimum allocator owner/provenance needed for that allocation. `Arc<Heap>` is not a VPP semantic requirement and is not part of the default Pool design.

Pool metadata such as ordinary Rust `Vec` and bitmap storage follows its actual Rust allocator path. The design must not claim that every metadata allocation is automatically owned by a retained Heap.

## Rust API Contract

### Pool public operations

- `Pool::with_capacity(capacity)` creates a dynamic pool with an initial reservation.
- `Pool::with_fixed_capacity(max_elts)` creates a non-growing pool.
- `len()` returns active initialized element count.
- `capacity()` returns backing allocation capacity.
- `is_empty()` reports whether the active count is zero.
- `insert(value)` returns a `u32` VPP index; allocation failure and fixed-pool exhaustion panic at the infra boundary.
- `contains_key(index: u32)` reports whether an index currently names an active element.
- `get(index: u32)` and `get_mut(index: u32)` return an active element or `None`.
- `remove(index: u32)` returns an active value or `None`.
- `iter()` yields active `(u32, &T)` pairs in numeric index order.

`insert_with` is not part of the design. It is an unused non-VPP helper and is removed.

### Crate-private Pool semantics

The implementation may keep the following behavior private until a real caller needs it:

- logical vector length (`pool_len`);
- active count (`pool_elts`);
- free-element count including dynamic spare capacity (`pool_free_elts`);
- allocation capacity (`pool_max_len`);
- `opaque` storage;
- growth preflight corresponding to `pool_get_will_expand` and `pool_put_will_expand`;
- index traversal and occupied-index iteration;
- region iteration;
- two-phase flush;
- `Clone` for `T: Clone`, preserving occupancy, free-index order, numeric indexes, and the alignment policy;
- invariant validation corresponding to `pool_validate`.

No C macro-shaped public API, pointer arithmetic API, byte-zero API for arbitrary `T`, or per-operation allocator replacement is added.

## VPP Pool Interface Mapping

Every VPP Pool interface in `third_party/vpp/src/vppinfra/pool.h` is accounted for here:

| VPP interface | Rust design | Exposure |
|---|---|---|
| `pool_init_fixed` | `Pool::with_fixed_capacity` | public constructor |
| `pool_validate` | `validate` | crate-private typed invariant check |
| `pool_elts` | `len` | public active count |
| `pool_len` | `vector.len()` | crate-private logical length |
| `_pool_len` | no safe mutable length escape | private implementation only |
| `pool_header_bytes` | `header_bytes` | crate-private diagnostics |
| `pool_bytes` | `bytes` | crate-private diagnostics |
| `pool_max_len` | `vector.capacity()` | public `capacity` |
| `pool_free_elts` | `free_capacity` | crate-private, including dynamic spare vector capacity |
| `pool_get` | `insert` | public allocation |
| `pool_get_aligned` | `Pool<T, A>` construction-time alignment policy | optional type-level alignment, no per-operation mutation |
| `pool_get_aligned_zero` | no safe generic equivalent; callers use typed construction | not exposed |
| `pool_get_zero` | no safe generic byte-zero operation | not exposed |
| `pool_get_will_expand` | `will_get_grow` | crate-private preflight |
| `pool_is_free_index` | `is_free_index(u32)` | crate-private state query |
| `pool_put_index` | `put_index(u32)` | crate-private drop-only release |
| `pool_put` | no pointer-to-index form | replaced by `put_index`/`remove` |
| `pool_put_will_expand` | `will_put_grow(u32)` | crate-private preflight |
| `pool_alloc` | `try_reserve` | crate-private growth reservation |
| `pool_alloc_aligned` | `try_reserve` under the Pool's fixed alignment policy | crate-private |
| `pool_alloc_heap` | no default-path equivalent | explicit non-main allocator requires a real caller |
| `pool_alloc_aligned_heap` | no default-path equivalent | explicit non-main allocator requires a real caller |
| `pool_dup` | `Clone::clone` for `T: Clone` | Rust trait implementation |
| `pool_dup_aligned` | `Clone::clone` preserves the alignment policy | Rust trait implementation |
| `pool_free` | `Drop` | Rust ownership operation |
| `pool_get_first_index` | `first_index` | crate-private iterator primitive |
| `pool_get_next_index` | `next_index(u32)` | crate-private iterator primitive |
| `pool_foreach_region` | `regions` | crate-private range iterator |
| `pool_foreach` | `iter` | public occupied-element iterator |
| `pool_elt_at_index` | `get(u32)` | public safe lookup returning `Option` |
| `pool_next_index` | `next_occupied_index(u32)` | crate-private iterator primitive |
| `pool_foreach_index` | `indices` | crate-private occupied-index iterator |
| `pool_foreach_stepping_index` | `indices_range(start, end)` | crate-private range iterator |
| `pool_foreach_pointer` | ordinary `iter` plus caller-side mapping | no separate pointer iterator |
| `pool_flush` | `clear_with` | crate-private two-phase flush |
| `pool_is_free(P, E)` | no pointer arithmetic interface | Rust callers already hold `u32` |

### Alignment mapping

VPP stores vector alignment in `vec_attr_t.align` and combines it with native element alignment through `_vec_align`; see `third_party/vpp/src/vppinfra/vec.h:63-95`. `pool_init_fixed` passes the vector's configured alignment, and the aligned Pool operations pass an alignment to vector allocation; see `pool.h:44-49` and `pool.h:187-198`.

Hammer exposes VPP's optional alignment as a construction/type policy. `Pool<T>` uses the default cache-line policy; `Pool<T, A>` uses explicit alignment `A`. The policy does not change for an individual allocation, and no alignment argument is required for the default path.

## Pool State Invariants

```text
free_bitmap.count_ones() == free_indices.len()
active_len + free_indices.len() == storage_len
all free_indices < storage_len
free_indices contains no duplicate index
bitmap bit(index) == 1 exactly when the index is free
fixed mode: storage_len == max_elts

dynamic free capacity = free_indices.len()
                       + allocation_capacity - storage_len
```

There is no generation array, generation counter, stale-index check, or hidden alternate index.

## Index Cleanup Scope

The following existing types or wrappers are removed:

- `hammer-infra::pool::Index`;
- every `PoolIndex` import alias;
- Pool generation array and generation checks;
- `Index::new(slot, generation)` and `generation()` on Pool indexes;
- `Pool::index_at_slot`;
- `Pool::insert_with`;
- TCP `RackDeadlineIndex`, replaced by its direct `Option<Instant>` value.

The following are explicitly retained because they have independent resource, protocol, or data-plane semantics:

- `hammer_core::data_plane::Index`;
- `NodeErrorIndex`;
- `SessionId`;
- `SessionHandle`;
- application, listener, and connection identity wrapper types;
- `VclSessionHandle`;
- stats `DirectoryIndex` and `SymlinkIndex`;
- `AdjacencyIndex`, `LoadBalanceIndex`, and other forwarding-domain indexes;
- transport-owned indexes whose meaning is defined by the owning protocol worker.

No global search-and-replace of every type named `Index`, `Id`, or `Handle` is allowed. A type is removed only when its concrete references and ownership boundary show that it is a redundant wrapper.

`FibRouteDpoIndex` is a separate future cleanup candidate because it wraps a private vector offset, but it is not part of this design's deletion set.

## Caller Boundaries

FIFO out-of-order storage uses one Pool for segment ownership and one RbTree whose nodes are held by its own Pool. The tree's values are plain numeric indexes into the FIFO segment Pool.

TCP, UDP, session, runtime, and application code may continue to use their own domain handles. They must not rely on a Pool generation field after this design is implemented; if a domain requires lifecycle identity, that identity remains owned by the domain type rather than being smuggled through `pool::Index`.

No change is made to worker ownership, TCP state, timers, packet output, session routing, or data-plane buffer ownership by this design.

## Testing Decisions

Tests validate observable behavior at the highest useful seam.

Pool tests cover:

- dynamic zero-capacity construction;
- initial reservation and growth;
- fixed capacity and panic on exhaustion;
- reverse free-index initialization and LIFO reuse;
- active count, logical length, free count, and allocation capacity;
- invalid and already-free indexes without metadata mutation;
- lookup, mutable lookup, removal, and numeric iteration;
- sparse bitmap iteration;
- exactly-once Drop for remove, flush, and Pool Drop;
- numeric index stability across growth;
- allocator/alignment preservation;
- growth preflight, flush, clone, and invariant validation.

RbTree tests cover:

- insert, replace, remove, and automatic growth beyond the initial reservation;
- key ordering, predecessor, successor, first, last, and iteration;
- free-index reuse after deletion;
- node Drop exactly once;
- prefetch before and after vector growth;
- RB-tree node allocation and release through its Pool;
- nil sentinel, root opaque value, and numeric parent/child indexes.

Caller tests cover FIFO out-of-order enqueue, merge, delivery, and removal across the initial Pool capacity without rebuilding the segment Pool and RbTree merely because either Pool grows.

VPP pool and RB-tree tests are behavioral prior art. Rust tests validate Rust ownership and public behavior rather than reproducing C macro syntax.

## Performance and Ownership

- Pool insert/remove are O(1) when free metadata has capacity.
- Dynamic growth is amortized O(1) per insertion and O(n) for a growth operation.
- Lookup is O(1).
- Bitmap iteration is O(bitmap words plus occupied elements).
- RB-tree operations remain O(log n).
- No generation checks or per-operation `Arc` cloning are added to hot paths.
- No locks or cross-worker ownership are introduced.
- Raw pointers into growable backing vectors are immediate-use only and are never persisted across growth.
- Every initialized Rust value is dropped exactly once; free storage is never interpreted as an initialized value.

## Out of Scope

- Data-plane `Index` redesign;
- Session/application/transport handle redesign;
- removal of valid protocol or shared-memory index types;
- VPP C ABI compatibility;
- generic byte-zero initialization;
- per-operation allocator replacement;
- compaction or shrinking that changes numeric indexes;
- unrelated TCP, session, worker, or packet architecture changes.

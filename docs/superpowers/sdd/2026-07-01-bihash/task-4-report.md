# Task 4 Report: Hot Path — `lookup` (atomic bucket read + page scan)

## Status

**Completed.** All 16 bihash tests pass (15 pre-existing from Tasks 1-3, 1 new). Zero warnings from bihash code. The `dead_code` warnings on `Bihash.buckets`, `.pages`, `.log2_nbuckets` are now **resolved** — `ops.rs` uses all three.

## Files Created

| File | Purpose |
|---|---|
| `crates/hammer-infra/src/bihash/ops.rs` | `Bihash::lookup()`, `Bihash::lookup_with_hash()` |

## Files Modified

| File | Change |
|---|---|
| `crates/hammer-infra/src/bihash/mod.rs` | Added `pub mod ops;` |
| `crates/hammer-infra/tests/bihash.rs` | Added `bihash_lookup_miss_on_empty_table` test |

## Implementation Notes

### `ops.rs`

- `lookup(&self, key: &K) -> Option<V>` — read-only hot-path entry point. Delegates to `lookup_with_hash`.
- `lookup_with_hash(&self, key: K, hash: u64) -> Option<V>` — full logic:
  1. Early return `None` on empty bucket array or empty bucket
  2. Extract `PageId` from bucket offset
  3. Compute page offset from upper hash bits (non-linear) or 0 (linear-search mode)
  4. Scan up to `2^log2_pages` pages, iterating all `KVP` slots in each page
  5. Skip free slots via `kv.value == V::default()` — works for `u64` (value `0` never stored)
  6. Return `Some(value)` on key match, `None` after full scan
- **Bounds**: `K: BihashKey + Default` (latter required by `PageAlloc::get`), `V: Copy + Eq + Default`
- **Inline**: `#[inline(always)]` on both methods

### Trait bound notes

Added `K: Default` to the impl bound because `PageAlloc::get` is defined under `impl<K: Copy + Default, ...>`. The upstream `Bihash::new` already requires `K: Default`, so this is consistent. Removed the `ValuePage<K, V, KVP>: Clone` bound from the spec — it's unnecessary (`get` returns a reference, no cloning occurs).

## Test Results

```
$ cargo test -p hammer-infra --test bihash bihash_lookup_miss_on_empty_table -- --exact
   1 passed, 0 failed

$ cargo test -p hammer-infra --test bihash
  16 passed, 0 failed

$ cargo test -p hammer-infra
  51 passed, 1 failed (pre-existing msg_queue::cross_process_signal_has_fd)
```

## Concerns

1. **Free-slot detection via `V::default()`** — correct for Phase 1 (`u64`), but Phase 2 will need a sentinel-based approach. Current code uses `kv.value == V::default()`. For `u64`, `default()` is `0`, and the bihash never stores `0` as a valid value.
2. **`Vec<Bucket>` (non-atomic)** — `self.buckets` is `Vec<Bucket>`, not `Vec<AtomicBucket>`. Phase 1 uses exclusive-borrow (`&self`), so this is safe. Phase 2 concurrency will need atomic bucket reads via `AtomicBucket`.

## Commit

```
8496295c hammer-infra(Feat): Bihash lookup (hot path, single-threaded Phase 1)
```

# Task 1 Report — Bihash Scaffolding + `BihashKey` Trait

## Status: DONE

**Commit:** `0c424b4d` — `hammer-infra(Feat): Bihash scaffolding + BihashKey trait`

## Files Created/Modified

| File | Action | Lines |
|---|---|---|
| `crates/hammer-infra/src/bihash/key.rs` | Created | 80 |
| `crates/hammer-infra/src/bihash/mod.rs` | Created | 77 |
| `crates/hammer-infra/tests/bihash.rs` | Created | 28 |
| `crates/hammer-infra/src/lib.rs` | Modified | +1 (added `pub mod bihash;`) |

## What was built

### `BihashKey` trait (`key.rs`)
- Trait with `hash() -> u64` and `key_eq()` methods
- Implemented for `u64`, `u32`, `u16`, `usize`, `u128`
- `splitmix64()` hash function (matches `FlatHashTable` in `map.rs`)
- `hash_words()` helper for composite keys

### `Bihash<K, V, const KVP>` skeleton (`mod.rs`)
- Struct with `buckets`, `pages`, `freelists`, `len`, `nbuckets`, `log2_nbuckets`
- Constructor `new(nbuckets)` — rounds up to next power of two
- Accessors: `len()`, `is_empty()`, `nbuckets()`
- Stub types: `Bucket(u64)` with `EMPTY` sentinel, `ValuePage` (PhantomData)

### Tests (`tests/bihash.rs`)
- `bihash_key_u64_hashes_deterministically` — verifies hash ≠ for distinct keys, hash == for same key
- `bihash_key_u64_eq_symmetric` — verifies `key_eq` reflection/symmetry
- `bihash_skeleton_constructs_with_zero_entries` — verifies `new(64)` gives 64 buckets, len=0, empty

## Deviation from Plan

The plan's `log2` calculation formula was inverted for power-of-two inputs:

```rust
// Plan (buggy):
let log2 = 32 - nbuckets.leading_zeros() as u8 - if nbuckets.is_power_of_two() { 0 } else { 1 };

// Fix (used in implementation):
let actual_buckets = nbuckets.next_power_of_two();
let log2 = actual_buckets.trailing_zeros() as u8;
```

This is equivalent to the standard `next_power_of_two()` round-up and avoids the bug. The fix was confirmed by the test `bihash_skeleton_constructs_with_zero_entries` which failed with the original formula.

## Verification

- `cargo test -p hammer-infra --test bihash` — 3/3 passed
- `cargo test -p hammer-infra` — 51/52 passed (1 pre-existing failure in `msg_queue::cross_process_signal_has_fd`)
- 2 `dead_code` warnings on fields that are stubs for Tasks 2-3 (expected and harmless)

## Pre-existing Issues

The test `cross_process_signal_has_fd` in `msg_queue.rs` fails (assertion `q.read_fd().is_some()` fails). This is unrelated to these changes and existed before this task.

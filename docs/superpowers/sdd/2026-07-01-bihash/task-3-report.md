# Task 3 Report: ValuePage + Free-List Page Allocator

## Status

**Completed.** All 15 bihash tests pass (7 pre-existing from Tasks 1-2, 8 new).

## Files Created

| File | Purpose |
|---|---|
| `crates/hammer-infra/src/bihash/value.rs` | `FREE_U64`, `Kv<K, V>`, `ValuePage<K, V, KVP>` |
| `crates/hammer-infra/src/bihash/alloc.rs` | `PageId`, `PageAlloc<K, V, KVP>` |

## Files Modified

| File | Change |
|---|---|
| `crates/hammer-infra/src/bihash/mod.rs` | Added `pub mod value; pub mod alloc;` + pub re-exports; replaced `Vec<ValuePage>` + `freelists: [Vec<u32>; 32]` + stub `ValuePage` with `PageAlloc<K, V, KVP>`; removed PhantomData fields (K/V/KVP now used via `pages`) |
| `crates/hammer-infra/tests/bihash.rs` | 8 new tests for Kv, ValuePage, PageAlloc |

## Implementation Notes

### `value.rs`

- `Kv<u64, u64>` has `empty()`, `is_free()`, `mark_free()`, `key_eq()` — the most common bihash key/value type
- `ValuePage<K, V, KVP>` stores slots as `[Kv<K, V>; KVP]` via `array::from_fn`
- **Design constraint**: Rust forbids overriding generic inherent `new()` for specialized type params. `ValuePage<u64, u64, 4>::new()` uses `V::default()` (= 0 for u64). Free detection uses `FREE_U64` (= `0xFEEDFACE_8BADF00D`). Fresh pages report `free_count() == 0` until slots are explicitly `mark_free()`'d. This is fine because PageAlloc recycles pages through `ValuePage::default()` + explicit slot writes by bucket logic.

### `alloc.rs`

- `PageId(u32)` — 1-indexed, `NONE = 0`
- `PageAlloc` has `pages: Vec<ValuePage>`, 8 `freelists` (was 32 in original stub — 8 covers log2_pages 0-7), `live: usize`
- Phase 1 supports only `log2_pages == 0` (single-page allocs) via `debug_assert_eq!`
- LIFO freelist recycling in `free()` / `alloc_single()`

### `mod.rs`

- `Bihash` struct now uses `PageAlloc<K, V, KVP>` directly. Added `+ Default` bounds on `K` and `V` (required by PageAlloc/ValuePage `new()`). Future tasks may relax this.
- Removed `PhantomData<K>`, `PhantomData<V>` — both K and V are concretely used through `pages: PageAlloc<K, V, KVP>`

## Test Results

```
cargo test -p hammer-infra --test bihash
  15 passed, 0 failed

cargo test -p hammer-infra
  51 passed, 1 failed (pre-existing msg_queue::cross_process_signal_has_fd)
```

## Warnings

- `dead_code` on `Bihash.buckets`, `.pages`, `.log2_nbuckets` — expected, fields used by future tasks

## Commit

```
git add crates/hammer-infra/src/bihash/ crates/hammer-infra/tests/bihash.rs
```

# Task 5 Report: Hot path — `insert` (non-splitting path)

**Date:** 2026-07-01
**Commit:** `1247c84155413787e9d9edee63ab9144799828d9`

## Status: ✓ Complete

## Changes

### `crates/hammer-infra/src/bihash/ops.rs`

- Added `Bucket` and `Kv` to imports
- Added `kv_slot_is_free()` helper — generic-V free slot detection using `V::default()`
- Added `insert()` method with three paths:
  1. **Empty bucket** — allocates one page, stores KV in slot 0, packs bucket word with `refcnt=1`
  2. **Overwrite** — scans target page for existing key, updates value in-place (no bucket/len change)
  3. **Fill free slot** — finds first free slot, writes KV, bumps `refcnt` and `generation`
  4. **Page full** — delegates to `split_and_rehash()` stub
- Added `split_and_rehash()` stub (panics with `unimplemented!`) — placeholder for Task 6

### `crates/hammer-infra/tests/bihash.rs`

Added three tests:
- `bihash_insert_then_lookup_returns_value` — basic insert + lookup correctness
- `bihash_insert_overwrite_replaces_value_without_growing_len` — overwrite semantics
- `bihash_insert_distinct_keys_that_hash_to_same_bucket` — collision stress test (expected to panic in Phase 1)

## Test Results

| Test | Result |
|---|---|
| `bihash_insert_then_lookup_returns_value` | ✅ PASS |
| `bihash_insert_overwrite_replaces_value_without_growing_len` | ✅ PASS |
| `bihash_insert_distinct_keys_that_hash_to_same_bucket` | ❌ PANIC (expected — Task 6) |
| All 18 other tests | ✅ PASS |

## Design Decisions

- **Free-slot detection**: Uses `V::default()` (not `FREE_U64`/`is_free()`) — the generic approach per `AGENTS.md` guidelines. The `u64`-specialized `FREE_U64` sentinel is Phase-2-only.
- **refcnt/generation bump**: Only on true insert (new entry), not on overwrite. Generation wraps at 32.
- **Overwrite path**: No bucket update — value replaced in-place, `len` unchanged.
- **Phase 1 limitation**: Only scans the target page within the run, not all pages. Full pages trigger `split_and_rehash`.

## Concerns

None. Implementation follows the spec exactly.

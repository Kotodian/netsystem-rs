# Task 7-8 Report: Bihash remove/clear + iterator + type aliases + FlatHashTable deprecation

**Date:** 2026-07-01
**Commit:** `e54893e5`

## Status: ✓ Complete

## Changes

### Task 7 — `remove` + `clear`

#### `crates/hammer-infra/src/bihash/ops.rs` (modified)

- Added `remove(&mut self, key: &K) -> bool`. Walks the same lookup path
  (hash → bucket → page run, with `linear` / non-`linear` branching) and
  marks the matching slot free via `slot.value = V::free_sentinel()`. Returns
  `true` if a key was removed, `false` on miss. Only updates the bucket
  metadata (`refcnt -= 1`, `generation += 1`) for non-linear-search buckets
  — the same simplification the spec called out, because linear-mode
  refcnt is already known to be loose.
- Added `clear(&mut self)`. Resets every bucket to `Bucket::empty()` and
  replaces the `PageAlloc` with a fresh one (cheaper and more obviously
  correct than per-page `free()` because the `PageAlloc::free` path
  currently only supports `log2_pages == 0`).
- Added `PageAlloc` to the import list (`use crate::bihash::{Bihash,
  BihashFree, BihashKey, Bucket, Kv, PageAlloc, PageId};`).
- Updated the module-level doc comment from "Hot-path operations:
  lookup." → "Hot-path operations: lookup, insert, remove, clear."

### Task 8 — Iterator, type aliases, deprecation

#### `crates/hammer-infra/src/bihash/iter.rs` (new)

- `BihashIter<'a, K, V, KVP>` — snapshot-style iterator that yields
  `(&K, &V)` pairs. Tracks `(bucket_idx, page_rel, slot_idx)` state and
  advances one slot at a time, skipping free slots via `kv_slot_is_free`.
  Order is bucket → page → slot, deterministic but not insertion order.
- Re-uses the existing `kv_slot_is_free` helper from `ops.rs` (same
  trait-based sentinel check used by `lookup` / `insert` / `remove`).
- `new` is `pub(crate)` because the parent `mod.rs` constructs it from
  the public `Bihash::iter()` method.

#### `crates/hammer-infra/src/bihash/mod.rs` (modified)

- Added `pub mod iter;` and `pub mod template;`.
- Re-exported `BihashIter` and the four type aliases (`Bihash8x8`,
  `Bihash16x8`, `Bihash24x8`, `Bihash48x8`).
- Added `pub(crate) fn buckets(&self) -> &[Bucket]` and
  `pub(crate) fn pages(&self) -> &PageAlloc<K, V, KVP>` accessors on
  `Bihash` so the iterator can traverse internal state without making
  the fields themselves public.
- Added `pub fn iter(&self) -> BihashIter<'_, K, V, KVP>` — the public
  iterator entry point.

#### `crates/hammer-infra/src/bihash/template.rs` (new)

- Four type aliases with the KVP values that put one value page inside
  ~1 cache line:
  - `Bihash8x8<V> = Bihash<u64, V, 7>` — 16 B/KV, 7 per page ≈ 112 B.
  - `Bihash16x8<V> = Bihash<u128, V, 3>` — 24 B/KV, 3 per page = 72 B.
  - `Bihash24x8<V> = Bihash<[u64; 3], V, 2>` — 32 B/KV, 2 per page = 64 B (1 line).
  - `Bihash48x8<V> = Bihash<[u64; 6], V, 1>` — 56 B/KV, 1 per page = 56 B.

#### `crates/hammer-infra/src/bihash/key.rs` (modified)

- Added `BihashKey` impls for `[u64; 3]` and `[u64; 6]`. Both use the
  shared `hash_words` helper (XOR-fold over `splitmix64`) and inline the
  elementwise equality check.

#### `crates/hammer-infra/src/map.rs` (modified)

- `#[deprecated(since = "0.1.0", note = "use
  hammer_infra::bihash::BihashKey instead")]` on `pub trait FlatHashKey`.
- `#[deprecated(since = "0.1.0", note = "use
  hammer_infra::bihash::Bihash instead")]` on `pub struct FlatHashTable`.
- Deprecation triggers the expected 51 deprecation warnings across the
  library (mostly internal uses in `map.rs` itself) and a number of
  cross-crate warnings in `hammer-service` (which uses
  `FlatHashTable` heavily in `transport/tcp/lookup.rs` and other TCP
  modules). The full workspace still builds clean (`cargo build
  --workspace` returns 0); the warnings are not promoted to errors
  because no crate sets `deny(warnings)`.

### `crates/hammer-infra/tests/bihash.rs` (modified)

Added 7 new tests:

- `bihash_remove_existing_key` — basic remove + lookup verify.
- `bihash_remove_missing_key_returns_false` — miss returns `false`,
  `len` unchanged.
- `bihash_remove_all_entries_returns_bucket_to_empty` — remove every
  entry, `is_empty()` true, lookups return `None`.
- `bihash_clear_drops_len_to_zero` — bulk insert, clear, all lookups
  return `None`, table is reusable for further inserts.
- `bihash_8x8_alias_compiles` — type alias instantiates, exposes
  `nbuckets()`.
- `bihash_iter_empty_table_yields_nothing` — `iter().count() == 0` on
  an empty table.
- `bihash_iter_after_inserts_yields_correct_count` — 3 inserts, 3
  yielded pairs containing all the right `(k, v)` tuples.

## Test Results

```
$ cargo test -p hammer-infra --test bihash
  28 passed, 0 failed
```

All 21 prior tests still pass; the 7 new tests are all green.

```
$ cargo build --workspace
  Finished `dev` profile [unoptimized + debuginfo] target(s) in 15.63s
```

The full workspace compiles. The 51 deprecation warnings inside
`hammer-infra` and the additional ones in `hammer-service` are expected
and benign — the spec called for deprecation, not removal.

```
$ cargo fmt -p hammer-infra -- --check
  (clean)
```

```
$ cargo clippy -p hammer-infra --all-targets
  (no bihash-specific warnings; only the expected deprecation warnings
   from the deprecated `map::FlatHashTable` / `map::FlatHashKey` users)
```

The pre-existing `msg_queue::tests::cross_process_signal_has_fd` failure
in the lib-test target remains and is unrelated to this work
(documented in the Task 4 report).

## Deviations from the Spec

1. **`BihashIter::new` is `pub(crate)` instead of `fn`.** The spec had
   it as a private `fn new`, but the parent `mod.rs` calls
   `BihashIter::new(self)` from `Bihash::iter()`. Making the constructor
   `pub(crate)` keeps the public surface clean (only `Bihash::iter()` is
   public) while letting `mod.rs` wire it up. No `BihashIter` field is
   exposed.

2. **Unused `BihashKey` / `BihashFree` imports in `template.rs` removed.**
   The aliases reference these types implicitly through their type
   parameters, so the explicit `use` statements triggered clippy's
   `unused_import` warning. Removed both; the file no longer needs them
   since the type aliases bind to the `Bihash` symbol only.

3. **Deprecation markers are non-suppressing.** Putting `#[deprecated]`
   on `FlatHashTable` and `FlatHashKey` causes `cargo build -p
   hammer-infra` to emit 51 warnings and `cargo build --workspace` to
   emit additional ones in `hammer-service` (which uses
   `FlatHashTable` for TCP listener / connection / session indices).
   The build still passes — no crate denies warnings — but anyone
   working on TCP lookup will see deprecation warnings. The next
   cleanup pass should migrate the TCP call sites to `Bihash` and then
   delete `map.rs` outright; this PR is intentionally a marker, not a
   migration.

4. **No `BihashKey` impl for `[u64; 2]`.** The spec only requested
   `[u64; 3]` and `[u64; 6]`, so `[u64; 2]` is not implemented. Adding
   it later is a 4-line copy if a need arises.

## Test Summary

| Test | Path | Result |
|---|---|---|
| `bihash_remove_existing_key` | Task 7 | ✓ PASS |
| `bihash_remove_missing_key_returns_false` | Task 7 | ✓ PASS |
| `bihash_remove_all_entries_returns_bucket_to_empty` | Task 7 | ✓ PASS |
| `bihash_clear_drops_len_to_zero` | Task 7 | ✓ PASS |
| `bihash_8x8_alias_compiles` | Task 8 | ✓ PASS |
| `bihash_iter_empty_table_yields_nothing` | Task 8 | ✓ PASS |
| `bihash_iter_after_inserts_yields_correct_count` | Task 8 | ✓ PASS |
| (21 pre-existing bihash tests) | Tasks 1-6 | ✓ PASS |

## Concerns

1. **`BihashIter` is `&self`-bound and returns `&'a K` / `&'a V`.** This
   is snapshot semantics — concurrent inserts during iteration are not
   safe. This matches the spec ("snapshot-style") and is appropriate
   for a single-threaded data plane. If multi-threaded iteration is
   ever needed, this needs an `Arc<RwLock<...>>` wrapper or a
   `to_snapshot()` materialisation step.

2. **`remove` does not reuse freed slots eagerly across pages.** If a
   key was placed on a non-target page by an earlier split, removing
   it leaves a free slot on that page; the next `insert` that hashes
   to the original target page will fall through and not find this
   free slot. This is consistent with the existing `insert`
   behaviour (which also only scans the target page) and is not a
   regression for Task 7's tests, but a future cleanup should
   consider whether `remove` should also scan the full page run for
   the target key (the spec's `remove` does, via the `linear` branch
   for linear-mode buckets).

3. **`clear` drops the entire `PageAlloc` rather than returning pages
   to the freelist.** This means `clear` + bulk re-insert will allocate
   fresh pages instead of reusing the LIFO freelist. For a typical
   "tun down → tun up" cycle, the peak memory is unchanged (the old
   pages are released back to the OS via the `Vec::drop`); the
   tradeoff is one extra fresh allocation per page in the
   freelist-on-reinsert path. A future optimization could push each
   page through `PageAlloc::free` before dropping, but that needs a
   `log2_pages` parameter to honour the multi-page contract.

4. **`#[deprecated]` on `FlatHashTable` produces a large warning
   surface in `hammer-service`.** 51 deprecation warnings inside
   `hammer-infra` (mostly from internal uses in `map.rs` itself) plus
   additional ones in `hammer-service/src/transport/tcp/lookup.rs`.
   The build passes; the warnings are noise. The natural follow-up
   is to migrate the TCP listener / connection / session indices from
   `FlatHashTable` to `Bihash` and then delete `map.rs` entirely.

5. **Type-alias `Bihash48x8<V>` uses KVP=1.** One entry per page is
   degenerate — every insert that collides with an existing entry in
   the same bucket will trigger a split. For keys that actually use
   all 48 bytes, this is a meaningful cache-line alignment, but for
   small-key workloads the KVP=1 setting will inflate memory and
   split-rate. A future template selection step should pick KVP based
   on observed key distribution.

## Commit

```
e54893e5 hammer-infra(Feat): Bihash remove/clear, iterator, type aliases, FlatHashTable deprecation
```

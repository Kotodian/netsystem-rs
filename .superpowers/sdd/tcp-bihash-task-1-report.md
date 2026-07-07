# Task 1 Report: Mature `hammer-infra::bihash` storage and prefetch

## What I implemented

- Collapsed `bihash` slot values to fixed `u64` storage and removed the public free-marker trait.
- Changed `Bihash` to `Bihash<K, const KVP: usize>`, with:
  - `Slice<Bucket>` bucket storage
  - retained `Arc<Heap>`
  - `with_capacity_in(nbuckets, heap)` constructor
- Changed `PageAlloc` to allocate pages and freelists from `hammer_infra::vec::Vec` with retained heap ownership via `new_in`.
- Added `prefetch(&self, key)` and `prefetch_with_hash(&self, hash)` using `prefetch_read_l1`.
- Moved split slow-path temporary storage to heap-backed infra `Vec`.
- Removed split-path debug dumps and kept the single explicit slow-path panic: `panic!("bihash page run is full")`.
- Updated the `bihash` tests to the fixed-`u64` public API and added the new constructor/prefetch coverage from the task brief.

## TDD evidence

### RED

Command run:

```bash
cargo test -p hammer-infra --test bihash bihash_with_capacity_in_uses_supplied_heap_surface -- --nocapture
```

Note: the brief listed two test names on one Cargo command line, but Cargo accepts one filter string. I used the first new test filter so the `bihash` test target still compiled and failed on the new API seam.

Result summary:

- failed with `E0107` because `Bihash` still required three generic parameters
- failed with `E0599` because `Bihash::with_capacity_in` did not exist
- the second new test also participated in the compile failure because `Bihash<u64, 7>` was not yet valid

### GREEN

Command run:

```bash
cargo test -p hammer-infra --test bihash -- --nocapture
```

Result summary:

- `30 passed; 0 failed`
- includes both new tests:
  - `bihash_with_capacity_in_uses_supplied_heap_surface`
  - `bihash_prefetch_accepts_empty_and_present_keys`

Follow-up verification:

```bash
rg "std::vec::Vec|vec!\\[|pub trait .*Free" crates/hammer-infra/src/bihash crates/hammer-infra/tests/bihash.rs
```

Result summary:

- no matches

## Tests run and results

1. `cargo test -p hammer-infra --test bihash bihash_with_capacity_in_uses_supplied_heap_surface -- --nocapture`
   - expected RED
   - failed on missing/old `Bihash` API
2. `cargo test -p hammer-infra --test bihash -- --nocapture`
   - PASS
   - `30 passed; 0 failed`
3. `rg "std::vec::Vec|vec!\\[|pub trait .*Free" crates/hammer-infra/src/bihash crates/hammer-infra/tests/bihash.rs`
   - PASS
   - no matches

## Files changed

- `crates/hammer-infra/src/bihash/mod.rs`
- `crates/hammer-infra/src/bihash/value.rs`
- `crates/hammer-infra/src/bihash/template.rs`
- `crates/hammer-infra/src/bihash/alloc.rs`
- `crates/hammer-infra/src/bihash/ops.rs`
- `crates/hammer-infra/src/bihash/split.rs`
- `crates/hammer-infra/src/bihash/iter.rs`
- `crates/hammer-infra/tests/bihash.rs`
- `.superpowers/sdd/tcp-bihash-task-1-report.md`

## Self-review findings

- Confirmed the public `Bihash` surface now matches the task brief: fixed `u64` values, heap-aware constructor, and prefetch methods.
- Confirmed `bihash` no longer uses `std::vec::Vec`, `vec![]`, or a public free-marker trait in the task-owned files.
- Kept edits scoped to the task-owned `bihash` implementation and tests only.
- Preserved existing collision, split, remove, clear, and iterator behavior under the updated storage model via the existing test suite.

## Concerns

- No functional concerns from this task scope.
- The focused test command still emits pre-existing deprecation warnings from `crates/hammer-infra/src/map.rs`, but they are outside this task’s owned files.

# Task 2 Report — Bucket 64-bit Packed Struct + Bitfield Accessors

## Status: DONE

**Commit:** `c1044bbc` — `hammer-infra(Feat): Bihash Bucket 64-bit packed word + bitfield accessors`

## Files Created/Modified

| File | Action | Lines |
|---|---|---|
| `crates/hammer-infra/src/bihash/bucket.rs` | Created | 175 |
| `crates/hammer-infra/src/bihash/mod.rs` | Modified | −9 (replaced stub `Bucket`) |
| `crates/hammer-infra/tests/bihash.rs` | Modified | +51 (5 new tests) |

## What was built

### `Bucket` packed word (`bucket.rs`)

A 64-bit packed struct with this bit layout (LSB → MSB):

```
| offset (36) | lock (1) | linear_search (1) | log2_pages (8) | refcnt (13) | generation (5) |
```

Field constants:

| Field | Bits | Shift | Mask |
|---|---|---|---|
| `generation` | 5 | 0 | `0x1F` |
| `refcnt` | 13 | 5 | `0x1FFF` |
| `log2_pages` | 8 | 18 | `0xFF` |
| `linear_search` | 1 | 26 | `0x1` |
| `lock` | 1 | 27 | `0x1` |
| `offset` | 36 | 28 | `0xF_FFFF_FFFF` |

Methods implemented (all `pub`, all `#[inline(always)]`):

| Method | Description |
|---|---|
| `empty()` | Zero-valued sentinel (all zeros) |
| `from_raw(u64) -> Self` | Wrap raw word |
| `as_u64(self) -> u64` | Unwrap |
| `is_empty(self) -> bool` | `offset == 0 && (log2_pages == 0 \|\| refcnt == 0)` |
| `offset(self) -> u64` | 36-bit page-arena index |
| `is_locked(self) -> bool` | Lock flag |
| `is_linear_search(self) -> bool` | Linear-scan flag |
| `log2_pages(self) -> u8` | Log2 of backing page count |
| `refcnt(self) -> u16` | Reference count (max 8191) |
| `generation(self) -> u8` | 5-bit generation counter (wraps at 32) |
| `pack(...) -> Self` | Pack all fields into one word |
| `bump_generation(self) -> Self` | Increment gen mod 32 |
| `make_linear_search(refcnt) -> Self` | Lock+linear sentinel |
| `fmt::Debug` | All fields shown |

Also includes `AtomicBucket` — an `AtomicU64` wrapper with `new`, `load`, `store`, `compare_exchange`, `swap`.

### `mod.rs` changes

- Removed the stub `struct Bucket(u64)` and `impl Bucket { const EMPTY: u64 = u64::MAX }`
- Added `pub mod bucket; pub use bucket::Bucket;`
- Updated `Bihash::new()` to initialize with `Bucket::empty()` (0) instead of `Bucket(Bucket::EMPTY)` (`u64::MAX`)

## Tests

All 5 new tests pass (total 8 bihash tests):

| Test | What it verifies |
|---|---|
| `bucket_empty_sentinel_is_all_zero` | `Bucket::empty()` returns all-zero, all fields 0, empty sentinel |
| `bucket_pack_round_trip` | Pack with non-trivial values, read back every field |
| `bucket_size_is_exactly_eight_bytes` | `size_of::<Bucket>() == 8` |
| `bucket_generation_increments_modulo_32` | Bump from 31 → 0 |
| `bucket_refcnt_max_is_8191` | Max refcnt (2^13 − 1) fits |

## Verification

- `cargo test -p hammer-infra --test bihash` — **8/8 passed**
- `cargo test -p hammer-infra` — 51/52 passed (1 pre-existing failure in `msg_queue::cross_process_signal_has_fd`, same as Task 1)
- No new warnings; `dead_code` warnings on `Bihash` fields remain (expected, to be resolved in Tasks 3+)

## Concerns

None. All bit layouts verified against the spec — the 36+1+1+8+13+5 = 64 bits check passes, and all accessors round-trip correctly.

## Report

- **Status:** DONE
- **Commit:** `c1044bbc` — `hammer-infra(Feat): Bihash Bucket 64-bit packed word + bitfield accessors`
- **Test summary:** 8/8 bihash tests pass (3 from Task 1 + 5 from Task 2)
- **Concerns:** None
- **Report file:** `docs/superpowers/sdd/2026-07-01-bihash/task-2-report.md`

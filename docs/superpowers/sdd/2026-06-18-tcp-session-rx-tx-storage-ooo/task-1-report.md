# Task 1 Report

## Status

DONE

## Scope

Per task constraints, work stayed inside:

- `/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/crates/hammer-adapter/src/buffer.rs`
- `/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/crates/hammer-adapter/tests/buffer.rs`

No TCP-specific buffer/runtime API was added.

## TDD Record

I followed a red/green/refactor loop for the new generic sharing primitive.

### Red

Added a failing test first:

- `attach_clone_keeps_tail_alive_until_both_chains_are_freed`

Verified failure with:

```bash
cargo test -p hammer-adapter attach_clone_keeps_tail_alive_until_both_chains_are_freed
```

Observed failure:

- `no method named 'attach_clone' found for struct 'BufferPool'`

### Green

Implemented the smallest generic buffer-layer change to satisfy the approved design:

- Added `attach_clone(head, tail)` to `BufferPool`
- Added `attach_clone(head, tail)` to `DataPlaneBuffers`
- Added internal `ref_count` tracking to `Buffer`
- Updated `free_index()` release behavior so decrement/cache return/pool return remain inside the buffer layer

### Refactor / tighten

Added focused lifecycle tests:

- `freeing_head_with_attached_clone_does_not_free_session_tail`
- `freeing_original_tail_after_output_head_returns_storage_once`

Then ran formatting and full crate verification.

## Implemented Behavior

### New generic primitive

Added a generic VPP-aligned buffer primitive:

```rust
BufferPool::attach_clone(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()>
DataPlaneBuffers::attach_clone(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()>
```

Semantics:

- `head` must be a distinct buffer index
- `head` must not already have a `next_buffer`
- `tail` remains the same backing chain
- `attach_clone` increments refcount for every buffer in the attached tail chain
- `head` points at `tail` through normal chain metadata (`next_buffer`, `NEXT_PRESENT`, `total_len_not_including_first`)

### Release semantics

`free_index()` remains the only release entrypoint.

Release behavior now works as:

- walking a chain still happens inside the buffer layer
- each visited slot decrements its internal refcount
- only refcount zero resets the slot and returns it to the existing cache/pool path

This keeps refcount decrement, cache return, and pool return internal to `hammer-adapter::buffer`.

## Why this matches the brief

- Buffer chains can now be reused by multiple logical owners without copying payload bytes.
- Sharing stays in the existing buffer infra layer.
- No wrapper owner type was introduced.
- No TCP-specific buffer/runtime API was introduced.
- `free_index()` remains the single public release surface.
- The change does not expand `with_current_chain_range` / view / range style APIs.

## Verification

Ran:

```bash
cargo fmt --all
cargo test -p hammer-adapter --test buffer
cargo test -p hammer-adapter
```

Result:

- `hammer-adapter` tests passed
- New attach-clone/refcount tests passed

## Notes / Follow-up Concern

This task intentionally implements the approved generic attach/refcount foundation only. The current behavior assumes attached sharing uses an unshared head and a shared tail chain, which matches the approved VPP-style session/output payload-sharing direction for later TCP work.

## Rerun After Review Bug

Reviewer-reported bug:

- `DataPlaneBuffers::free_index()` finalized trace marks before refcount-aware free.
- Freeing a cloned head could therefore clear trace state on a still-live shared tail.

Root cause:

- The old trace finalization path walked the logical chain from the freed head and called `take_trace_mark()` before the buffer layer decided which slots actually reached refcount zero.
- With attached shared tails, logical membership and physical release are not the same thing.

Fix:

- Moved trace-mark collection into the refcount-aware buffer release path.
- `BufferPoolInner` now collects `TraceMark`s only for slots whose refcount actually drops to zero.
- `DataPlaneBuffers::free_index()` finalizes only those released marks after the buffer layer completes the free decision.

Regression test added:

- `freeing_cloned_head_keeps_shared_tail_trace_mark_live`

Rerun commands:

```bash
cargo test -p hammer-adapter --test buffer freeing_cloned_head_keeps_shared_tail_trace_mark_live
cargo test -p hammer-adapter --test buffer
cargo fmt --all
```

Rerun results:

- The new regression test failed before the fix with `left: None` / `right: Some(TraceMark { handle: 7, epoch: 11 })`.
- After the fix, the regression test passed.
- After the fix, `cargo test -p hammer-adapter --test buffer` passed with 42/42 tests green.

# Hammer Infra Timer Wheel Design

- **Date:** 2026-06-12
- **Status:** Draft
- **Scope:** `hammer-infra` reusable timer wheel, VPP-inspired, Rust-native API

## Goal

Add a **reusable high-performance timer wheel** to `crates/hammer-infra` that follows the core shape of VPP `vppinfra/tw_timer_template.*`, while exposing a Rust-style API suitable for later TCP worker use.

This design is for the **data structure only**:

- single-threaded
- tick-driven
- no `tokio`
- no `Instant`
- no control-thread callback coupling

## What Is Actually Borrowed From VPP

The following parts are intentionally aligned with VPP:

1. **Wheel geometry is fixed**
   - VPP instantiates a specific geometry such as `2t_1w_2048sl`.
   - Hammer will expose reusable core logic, but wheel geometry remains fixed per concrete type.

2. **Entries live in a pool**
   - Timers are not heap-allocated one-by-one.
   - Handles refer to pooled entries.

3. **Buckets are intrusive lists**
   - A slot is not a `Vec<T>`.
   - A slot owns a list head and timer entries link through `next` / `prev`.

4. **Expiry is odometer-style cascading**
   - Fast wheel advances every tick.
   - Upper wheels cascade downward only on wrap.

5. **Batch expiry output**
   - The wheel produces expired timers in batches.
   - It does not force one callback invocation per timer.

## What Changes For Rust

The following parts intentionally diverge from VPP to fit this codebase better:

1. **No C macro template instantiation**
   - Use const generics and concrete type aliases instead of preprocessor codegen.

2. **No raw pool index handles**
   - Use `slot + generation` handles in the style of existing `hammer-infra` descriptors/pools.

3. **No callback inside the wheel**
   - Caller provides an output buffer and pulls expired items.
   - This keeps `hammer-infra` pure and reusable.

4. **Generic payload values**
   - VPP stores a compact user handle.
   - Hammer stores a caller-provided payload `T`, because later TCP use wants richer timer state.

## First Version Shape

### File

- `crates/hammer-infra/src/timer_wheel.rs`

### Public Types

```rust
pub struct TimerHandle {
    slot: u32,
    generation: u32,
}

pub struct ExpiredTimer<T> {
    pub handle: TimerHandle,
    pub deadline_tick: u64,
    pub value: T,
}
```

### Reusable Core

```rust
pub struct TimerWheel<T, const WHEELS: usize, const SLOTS: usize, const SHIFT: usize> {
    // internal pooled entries + wheel slots
}
```

### Concrete Alias

First concrete alias:

```rust
pub type TimerWheel2t1w2048<T> = TimerWheel<T, 1, 2048, 11>;
```

The name mirrors VPP geometry naming, but the core remains reusable.

## Public API

First version API:

```rust
impl<T, const WHEELS: usize, const SLOTS: usize, const SHIFT: usize>
    TimerWheel<T, WHEELS, SLOTS, SHIFT>
{
    pub fn new(now_tick: u64) -> Self;

    pub fn now_tick(&self) -> u64;

    pub fn len(&self) -> usize;

    pub fn is_empty(&self) -> bool;

    pub fn schedule(&mut self, deadline_tick: u64, value: T) -> TimerHandle;

    pub fn cancel(&mut self, handle: TimerHandle) -> Option<T>;

    pub fn reschedule(&mut self, handle: TimerHandle, deadline_tick: u64) -> bool;

    pub fn advance_to(
        &mut self,
        now_tick: u64,
        expired: &mut hammer_infra::vec::Vec<ExpiredTimer<T>>,
    );
}
```

Semantics:

- `schedule`: absolute deadline in ticks
- `cancel`: removes timer if still live
- `reschedule`: remove + reinsert if handle still valid
- `advance_to`: expires all timers up to `now_tick`, appending into `expired`

## Internal Layout

### Entry Pool

Each timer entry stores:

- intrusive `next`
- intrusive `prev`
- `deadline_tick`
- generation / liveness
- upper-wheel carry offsets needed for cascade
- payload `T`

### Slot Heads

Each slot stores only a list-head handle/index, matching VPP’s bucket shape.

### Handle Model

`TimerHandle` uses:

- `slot`
- `generation`

This avoids stale-handle reuse after cancellation/expiration.

## First Version Limits

First version is intentionally narrow:

1. single-threaded only
2. no duplicate-stop special mode
3. no overflow vector yet
4. no fast-wheel bitmap hint yet
5. no multi-timer-per-object packing trick

Those are optimization follow-ups, not part of the first landing.

## Why This Scope

The first landing should prove the core shape:

- pooled entries
- intrusive buckets
- cascade logic
- reusable geometry
- Rust-safe handles

That is the minimum useful subset needed before wiring it into TCP worker runtime logic.

## Testing Strategy

Tests live under `crates/hammer-infra/tests/`.

First pass should cover:

1. schedule + expire in-order
2. cancel before expiry
3. reschedule to later slot
4. wrap-around on a single wheel
5. multiple timers in same slot
6. stale handle rejection after cancel/expire
7. `advance_to` with multiple ticks in one call

## Non-Goals

Not in this change:

- wall-clock wrappers
- async runtime integration
- callback APIs
- TCP-specific timer enums
- lock-free / cross-thread access
- VPP’s overflow vector and bitmap hint optimizations

## Implementation Follow-Up

If this design looks right, next step is:

1. write failing `hammer-infra` tests first
2. implement `timer_wheel.rs`
3. run focused `hammer-infra` tests only

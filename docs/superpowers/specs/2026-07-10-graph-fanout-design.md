# VPP-Aligned Graph Fanout Design

## Problem Statement

Hammer graph nodes do not share one reliable fanout path. Simple nodes use `process_frame!`, while more involved nodes repeat Next Frame lookup, Index insertion, error fallback, input removal, Drop routing, and put semantics by hand. Those copies disagree about ownership and failure behavior. A node can remove an input Index without forwarding it or leaving it in a Frame for RAII cleanup, and protocol code currently knows resolved target node identities that should remain Graph Runtime policy.

The existing helper is also not VPP-shaped. It groups by resolved target node, linearly scans a fixed set of groups, caps the number of next arcs, and mixes packet processing with frame acquisition failures. Cross-worker Handoff is a separate problem and must not distort the worker-local fanout design.

## Solution

Deepen Graph Fanout in Graph Runtime into the single worker-local next-frame enqueue operation. A Graph Node supplies its current input Frame plus one typed current-node-local next value per Index. Graph Fanout resolves local slots, groups packets in first-unprocessed-slot order, gets the current appendable Next Frame for that arc, performs VPP-style stable mask compaction, retires those Index values from the input Frame, and immediately puts the group. If the current Next Frame lacks space, fanout fills and puts it, gets a fresh frame, and puts the remainder before selecting another arc.

Rust RAII remains at Frame scope. Index is a copyable pool identity, not an owner. Every normal failure is resolved before a group starts moving ownership; the group transfer itself is a no-allocation, no-callback, non-failing internal section. The node-facing operation has no result enum or caller-managed free path.

## VPP Reference

- `vlib_buffer_enqueue_to_next` selects the first unhandled next, gets that arc's current frame, stably compresses matching buffer indexes, puts the frame, and repeats.
- `vlib_get_next_frame_internal` returns the current appendable frame and rotates when it is full or cannot be appended to.
- `vlib_put_next_frame` makes the current frame pending immediately; fanout does not define a global cross-arc commit order.
- Session IO normal and custom TX share the remaining 128-packet allowance after existing pending output seeds the count. Control processing is outside that IO allowance, and pending TX flushes once at dispatch end.
- Feature control state compiles target nodes into predecessor-local next values. The packet path carries configuration progress and reads one local next per step.
- Handoff groups by target worker through a separate cross-thread queue and does not pass through Graph Fanout.

The references are the corresponding implementations in the vendored VPP tree under `third_party/vpp`.

## Implementation Decisions

### Interface

Graph Fanout belongs to `hammer-runtime`. Buffer and Frame remain data-plane primitives in `hammer-core`; generic vector and mask operations remain in `hammer-infra`.

The node-facing operation is:

```rust
pub fn enqueue_to_next<N: NodeNext>(
    &self,
    frame: &mut BufferFrame,
    nexts: &[N],
);
```

It accepts no target node, pool handle, release callback, scratch object, commit object, or protocol result.

`NodeNext` represents only a copyable typed value whose `slot()` returns a current-node-local `u16`. Static enum metadata remains inherent on generated enums. Dynamic session output values and `u16` implement the same trait. A batch that can mix static and dynamically compiled feature decisions normalizes both to `u16` before calling fanout. No second slot type or next-storage trait participates in fanout.

### Per-Group Enqueue

Fanout normalizes next values into fixed scratch. It then repeats this VPP-shaped loop:

1. Choose the first remaining local next value.
2. Compute its match mask and count.
3. Resolve the local next and get its current appendable Next Frame.
4. Preflight how many matching entries fit in that Frame.
5. Stably move that prefix of matching Index values while compacting unmatched Index and next values in place.
6. Put the current Next Frame.
7. If the group still has entries, get a fresh frame for the same arc and continue the group; otherwise select the first remaining next.

The implementation reuses `BufferFrame::rewrite_indices_batched` for stable extraction and source compaction. It does not expose Frame storage to Graph Runtime or add a Frame collection operation.

Fanout does not acquire every output Frame first, does not delay every put to a global visibility point, and does not promise cross-arc commit ordering. First occurrence determines group visitation, while an already-pending frame retains its scheduler position.

All fallible resolution, acquisition, and capacity checks for a pass occur before matching Index values leave the source Frame. The subsequent extract, exact append, and put path has no user callback, allocation, or recoverable error. An impossible internal invariant breach aborts rather than unwinding through an ownership transfer.

### Frame RAII

Before each extraction pass, the input Frame owns the entire remaining batch. Matching Index values move into the current Next Frame and unmatched entries stay source-owned. A failed preflight leaves the source unchanged. A put failure, if retained for internal diagnostics, drops the current Next Frame and releases only its transferred entries; later entries remain source-owned.

The short source-to-next move is an internal non-unwinding section because the existing interfaces cannot express strict unwind-safe subset transfer without adding another ownership surface. This section performs no allocation or external callback and propagates no `Result`.

Each processed Index must finish in exactly one state: retained in the input Frame for RAII release, transferred to another explicit owner such as Session or Handoff, or enqueued to a next arc. Drop is an ordinary available arc, not a mandatory detour for an Index whose final release is already owned by the input Frame.

### Index

Buffer and frame pools use one private-field identity:

```rust
#[repr(C)]
pub struct Index {
    pool_id: u64,
    slot: u32,
    generation: u32,
}
```

Pools construct Index. Copying it does not add a buffer reference. `repr(C)` locks the hot-path value to an asserted 16-byte layout; it is not an external ABI promise. The frame pool stores Index only inside Frame state and does not expose a public Frame index accessor. There are no aliases, generic index families, marker parameters, index traits, per-index owner contexts, or per-index `Drop` implementations.

Pool IDs come from one checked process-wide nonzero namespace. Exhausting that namespace aborts pool construction rather than reusing an ID. A slot whose generation reaches its maximum is retired rather than wrapped to an earlier generation.

Common validation uses structured `DataPlaneError` variants:

- `ForeignIndex` carries expected and actual pool IDs.
- `StaleIndex` carries slot, Index generation, and current slot generation.
- Out-of-bounds and free-slot variants carry the pool and slot facts needed to diagnose the invalid identity.
- Frame-only checked-out and occupied-slot states remain frame-pool implementation errors.

No buffer validation path returns a string-only internal error.

### Frame Storage And Capacity

`BufferFrame` stores Index values in the existing heap-backed `hammer_infra::vec::Vec<Index>`. Frame cleanup and compaction use that container directly; Frame defines no collection or iterator implementation of its own.

Graph frame capacity is a logical maximum of 256 even though the underlying generic vector can grow. Production insertion enforces that limit. The production frame-capacity setting is removed; smaller frames exist only through crate-private test construction.

Frame-pool and scheduled-frame queue storage are sized at worker/runtime construction (`frame_pool_size` / queue capacity). Production Graph Nodes never branch on pool or queue exhaustion on the Fanout path: exhaustion is an invariant breach (abort), matching VPP's preallocated frame pool rather than mid-datapath pool growth. Allocation failure follows the process allocation-failure policy.

### Graph Integration

`process_frame!` only executes packet logic, records typed local next decisions, and calls Graph Fanout once. Its target-node cache, group scan, capacity branch, and frame put loop are removed.

Only Graph Runtime maps local `u16` slots to target node identities. Generated resolved-node arrays and production current-node-next lookup surfaces are removed where fanout makes them redundant. Dynamic arc registration returns a checked `u16`.

Production Graph Nodes use Graph Fanout for worker-local enqueue. Direct frame acquisition, Index insertion, and put remain limited to Graph Runtime internals, Handoff, and focused low-level tests.

Delivery sequencing: Graph Runtime ships `enqueue_to_next`, internal appendable state, and worker scratch first (`#29`) without changing `process_frame!`. Macro and production-node migrations follow in later tickets. Multiple Fanout calls in one dispatch are allowed; callers that remove Index values before Fanout (Handoff/Session) must pass `nexts` matching the remaining Frame length. Frame-pool slots remain a construction-time size (VPP-style preallocation), not a hot-path grow.

### Session Queue

The existing `SessionQueueOutput` is deepened rather than replaced. It uses the current driver Frame as RAII backing for generated buffers, records one typed next per generated entry, and invokes Graph Fanout once at dispatch end.

Output already pending after transport time and timer update seeds the packet count. Normal and custom IO TX share the remaining allowance up to 128. Control processing is not charged to that IO allowance. IO budget is reserved before allocation or transport mutation, and unserved work remains scheduled.

Transport state is committed before Session Queue makes buffers graph-visible. Graph Fanout does not interpret transport results, TCP state, session delivery, or payload ownership. Generated TCP ACK and control output joins the existing Session Queue custom-output path instead of allocating and immediately putting one-buffer frames from TCP input nodes.

### Feature Arc

The control plane may retain target node identities while compiling an ordered feature chain under a barrier. Each compiled transition registers the predecessor's dynamic arc and stores that current-node-local `u16` plus the next configuration index.

The packet path carries only configuration progress, advances it once per feature, preserves the caller's default when no feature applies, and never resolves a target node itself.

### Handoff

Graph Fanout remains worker-local and does not become thread-safe for Handoff. Handoff alone owns cross-worker grouping, queue capacity, rejection, and destination ownership establishment. Rejected entries are released by Handoff and do not enter a Drop arc.

## Interface Approval

| Final surface | Why existing surfaces are insufficient | Status |
| --- | --- | --- |
| One concrete `Index` | Parallel data-plane pool identities allow inconsistent validation and expose frame identity unnecessarily | Approved by this spec |
| Structured Index validation variants | Current buffer validation loses machine-readable facts in strings | Approved by this spec |
| `NodeNext::slot() -> u16` and `NodeNext for u16` | Static and dynamically compiled decisions need one local-slot representation | Approved by this spec |
| `DataPlaneRuntime::enqueue_to_next` | Grouping, ownership transfer, frame rotation, and put are currently repeated by callers | Approved by this spec |
| Dynamic arc registration returns checked `u16` | Packet-path next values must match VPP's local-next width | Approved by this spec |
| Internal appendable-next-frame state | Fresh one-shot frames cannot reproduce VPP append and rotate semantics | Approved by this spec |
| Generic infrastructure `u16` mask comparison | Existing infrastructure lacks the VPP-equivalent mask operation required by fanout | Approved by this spec |
| Internal deepening of `SessionQueueOutput` | Existing output immediately creates and puts frames and cannot enforce VPP IO budgeting | Approved by this spec |

No other new public type or operation is approved. Superseded owner, index, target-node routing, and fixed next-count surfaces are removed rather than wrapped.

## Testing Decisions

- The primary seam is Graph Runtime dispatch. Tests observe which node receives which Index values and in what per-next order; they do not inspect masks, scratch, or private pool storage.
- Cover one next, alternating nexts, sparse local slots, 256 distinct local slots, typed and dynamic slots above the old limit, stable per-next order, first-unhandled group visitation, append to a partial Next Frame, same-group frame rotation, Drop routing, and empty input. Do not assert a global cross-arc scheduler order.
- Start with a construction-time frame-pool sized for the scenario and verify Fanout delivers every group; do not expect mid-path frame-pool growth.
- Inject a group preflight failure before extraction and verify the input Frame still owns the complete group. Verify successful groups and remaining input are each released exactly once.
- Pool tests cover structured foreign-pool, stale-generation, out-of-range-slot, and free-slot errors, checked pool-ID exhaustion, and retirement of a maximum-generation slot.
- Session Queue tests verify existing pending output seeds the count, normal and custom IO share the remaining 128 allowance, control is not charged to that allowance, unserved IO work remains pending, one fanout flush occurs, and transport commits before graph visibility.
- Feature tests cover multi-step configuration progress, no-feature default behavior, compiled end-next behavior, and Graph Runtime resolution of each local next.
- Handoff tests verify worker-local fanout never enters the cross-worker queue and Handoff remains the only cross-thread ownership path.
- Architecture checks reject production service-node direct frame get/push/put sequences, target-node routing in protocol or feature packet paths, fixed next-count limits, custom Frame container implementations, and production frame-capacity configuration.
- Benchmarks compare single-next, alternating two-next, and multi-next 256-packet fanout. They report scalar and available architecture-specific paths and verify zero steady-state heap allocation in grouping.
- Verification runs focused crate tests, workspace formatting and lint, and the complete workspace test suite.

## Out Of Scope

- Redesigning Handoff or making ordinary Frame or Graph Fanout state cross-thread capable.
- Changing TCP state-machine, congestion-control, recovery, timer, or payload-retention semantics beyond migrating graph enqueue call sites.
- Changing the app/session copy boundary or introducing any new payload copy.
- Adding transport-specific buffer or runtime interfaces.
- Adding a public owner for one Index, a release context, an index alias, a generic index family, a marker-based index, or a second local-next type.
- Providing all-groups atomic graph visibility.
- Preserving deprecated graph-routing compatibility wrappers.

## Published Spec

GitHub Issue `#25`, **PRD: Deepen Graph Fanout with VPP-style next-frame enqueue**, is the tracker copy of this design.

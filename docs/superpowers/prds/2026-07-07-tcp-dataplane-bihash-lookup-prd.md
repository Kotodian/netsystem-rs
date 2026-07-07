# TCP Dataplane Bihash Lookup PRD

## Problem Statement

Hammer's TCP Dataplane Lookup still uses deprecated flat hash table surfaces in performance-sensitive packet-path lookup. This leaves TCP session routing, pending opens, listener lookup, listener pending state, and Fast Open cache lookup on a container that is not aligned with the VPP session lookup model and still carries outdated value/type assumptions.

The user needs the TCP hot path to use a mature VPP-style bihash design without expanding the Rust API surface with wrapper key types, generic business values, compatibility fallback tables, or control-plane-only migration work.

## Solution

Migrate TCP Dataplane Lookup to Hammer's VPP-style bihash. Existing TCP Lookup Key types remain the domain representation and gain bihash capability through trait implementations that provide hashing while relying on Rust `Eq` for equality. Bihash Value is fixed to opaque `u64` handles; business records remain owned by pools, session state, listener state, or existing cache owners.

Before TCP depends on bihash, mature the bihash implementation so its buckets, pages, freelists, and split working storage use Hammer infrastructure memory instead of standard vector storage. Add a generic bihash prefetch capability so the existing TCP prefetch path remains available without teaching TCP about bihash internals.

The migration covers every TCP dataplane lookup-related index previously held in the TCP lookup module: established route lookup, pending route lookup, listener snapshot lookup, listener pending lookup, listener pending counts, and TCP Fast Open cache lookup. Low-frequency control-plane bookkeeping maps are left alone.

## User Stories

1. As a Hammer dataplane maintainer, I want TCP Dataplane Lookup to use bihash, so that packet-path lookup aligns with VPP session lookup semantics.
2. As a Hammer dataplane maintainer, I want deprecated flat hash tables removed from TCP lookup, so that new TCP work does not build on a deprecated container.
3. As a Hammer TCP developer, I want existing TCP Lookup Key types to remain the lookup API, so that Rust domain modeling stays clear and type-safe.
4. As a Hammer TCP developer, I want bihash capability added to existing key types, so that lookup code does not manually pack raw words at call sites.
5. As a Hammer TCP developer, I want Bihash Value to be an opaque `u64`, so that bihash remains a VPP-style exact-match index instead of a business object owner.
6. As a Hammer TCP developer, I want route entries to stay in pools, so that bihash only maps keys to stable handles.
7. As a Hammer TCP developer, I want pending open entries to stay in pools, so that half-open semantics are indexed without changing ownership.
8. As a Hammer TCP developer, I want listener values stored behind handles, so that listener capabilities do not force bihash to store business structs.
9. As a Hammer TCP developer, I want TCP Fast Open cache entries to remain in their existing owner storage, so that cache behavior is not mixed into the lookup container.
10. As a Hammer runtime developer, I want TCP listener pending state indexed by bihash, so that handshake packet processing uses the same exact-match lookup strategy as other TCP dataplane routes.
11. As a Hammer runtime developer, I want established and pending route lookup to preserve IPv4 and IPv6 behavior, so that migration does not change packet routing semantics.
12. As a Hammer runtime developer, I want mixed-family TCP tuple lookup to remain a miss, so that invalid packet-family combinations do not create accidental routes.
13. As a Hammer runtime developer, I want TCP prefetch behavior preserved, so that the input path keeps its current latency-hiding intent.
14. As a Hammer infra developer, I want bihash bucket storage to use Hammer infrastructure allocation, so that the container fits Hammer's memory model.
15. As a Hammer infra developer, I want bihash page allocation to use Hammer infrastructure allocation, so that table growth and split paths do not rely on standard vector storage.
16. As a Hammer infra developer, I want bihash split working storage to use Hammer infrastructure vectors, so that slow-path rehashing follows the same allocation policy.
17. As a Hammer infra developer, I want bihash prefetch to be a generic operation, so that TCP does not need table-specific bucket arithmetic.
18. As a Hammer infra developer, I want bihash values fixed to `u64`, so that the container surface stays VPP-style and avoids business value generics.
19. As a Hammer maintainer, I want no new TCP key wrapper types, so that the migration does not add names that duplicate existing domain concepts.
20. As a Hammer maintainer, I want no compatibility fallback tables, so that the migration has one source of truth.
21. As a Hammer maintainer, I want low-frequency control-plane maps left alone, so that the change remains scoped to TCP Dataplane Lookup.
22. As a Hammer maintainer, I want the ADR respected, so that future contributors understand why TCP lookup uses bihash and opaque handles.
23. As a Hammer maintainer, I want the glossary terms preserved, so that implementation and review use consistent domain language.
24. As a Hammer reviewer, I want focused tests for bihash infra behavior, so that the container can be trusted before TCP depends on it.
25. As a Hammer reviewer, I want focused TCP lookup tests, so that route, listener, pending, and Fast Open behavior are preserved.
26. As a Hammer reviewer, I want targeted test commands, so that verification avoids expensive workspace-wide runs during iteration.
27. As a Hammer reviewer, I want scans for forbidden symbols, so that deprecated tables and wrapper-key types are not accidentally left behind.
28. As a Hammer agent implementer, I want task boundaries that separate infra, key support, and TCP migration, so that subagents can work independently.
29. As a Hammer agent implementer, I want each task to have a clear acceptance test, so that review can gate progress without reading the whole migration at once.
30. As a Hammer performance maintainer, I want VPP-style lookup semantics without copying VPP's C API shape, so that Hammer stays idiomatic Rust while preserving dataplane intent.
31. As a Hammer performance maintainer, I want packet-path records owned outside the hash table, so that lookup remains an index rather than a storage abstraction.
32. As a Hammer performance maintainer, I want listener pending counts migrated with the rest of TCP lookup, so that the lookup module no longer mixes old and new table models.
33. As a Hammer performance maintainer, I want Fast Open cache lookup migrated too, so that "all TCP dataplane lookup" means the full agreed scope.
34. As a future Hammer contributor, I want the migration to avoid new business wrappers, so that I can identify the real domain state without peeling through helper types.
35. As a future Hammer contributor, I want implementation decisions recorded in a PRD and ADR, so that I can understand the trade-offs without replaying the design conversation.

## Implementation Decisions

- TCP Dataplane Lookup is the scope. It means exact-match packet-path lookup that routes a TCP packet tuple or listener endpoint to an existing session, pending open, listener path, listener pending state, or Fast Open cache path.
- Low-frequency control-plane bookkeeping maps are out of scope for this migration.
- Bihash is the selected lookup container for TCP Dataplane Lookup because it matches VPP's session lookup design and Hammer already has a VPP-style bihash module.
- Bihash Value is always an opaque `u64` handle.
- Business records are not stored directly in bihash. They remain owned by pools, session state, listener state, or existing cache owners.
- Existing TCP Lookup Key types remain the domain representation. The migration adds bihash hashing capability to those keys rather than adding wrapper key types; equality remains the standard Rust `Eq` contract.
- IPv4 and IPv6 lookup may use separate bihash instances where that matches key shape and VPP session lookup semantics.
- The design does not require API expansion for route-key wrappers or raw word plumbing in TCP code.
- Bihash infrastructure must be matured before TCP migrates to it. Buckets, pages, freelists, and split working storage should use Hammer infrastructure memory surfaces.
- Bihash gains generic prefetch support. TCP keeps its prefetch intent without knowing bihash bucket internals.
- The old value-generic bihash shape is replaced by a fixed `u64` value model.
- Established route lookup, pending route lookup, listener snapshot lookup, listener pending lookup, listener pending counts, and TCP Fast Open cache lookup are all in migration scope.
- The migration removes deprecated flat hash table usage from the TCP lookup module rather than leaving a compatibility path.
- The ADR "TCP dataplane lookup uses bihash" is the architectural decision record for this PRD.
- The glossary terms "TCP Dataplane Lookup", "Bihash Value", and "TCP Lookup Key" are canonical vocabulary for this work.
- Subagent task boundaries should follow deliverable seams: bihash infra maturity, existing key support, then TCP lookup migration.
- No workspace-wide test run is required during this migration unless explicitly requested.

## Testing Decisions

- The highest useful test seam is behavior at TCP Dataplane Lookup boundaries: route lookup, listener lookup, pending lookup, listener pending lifecycle, and Fast Open cache update/lookup.
- Bihash infrastructure should be tested directly before TCP migration, because TCP depends on the container's constructor, lookup, insert, remove, clear, iterator, split, and prefetch behavior.
- Existing TCP lookup and TCP input tests are the preferred prior art. They already exercise packet routing and route preservation behavior.
- Existing bihash tests are the preferred prior art for container behavior. They already cover deterministic hashing, bucket structure, insert, lookup, overwrite, split, remove, clear, aliases, and iteration.
- Tests should assert external behavior rather than internal bucket placement.
- TCP route tests should prove both IPv4 and IPv6 routes survive migration.
- Listener tests should prove a listener lookup returns the full listener value, including capabilities.
- Listener pending tests should prove tuple lifecycle, backlog accounting, refresh behavior, pruning, and removal remain correct.
- Fast Open tests should prove updating an existing tuple replaces the cached value rather than adding a duplicate visible entry.
- Prefetch tests should be smoke tests that prove empty and present keys are accepted without changing lookup results.
- Verification should include focused package tests for bihash, core protocol key behavior, TCP lookup, and TCP input.
- Verification should include symbol scans to ensure deprecated flat hash table surfaces, wrapper key names, standard vector storage in bihash implementation, and public free-marker traits are absent.

## Out of Scope

- Migrating unrelated flat hash table users outside TCP Dataplane Lookup.
- Migrating low-frequency listener control-plane bookkeeping.
- Adding TCP-specific bihash APIs in infrastructure.
- Adding new TCP key wrapper types.
- Storing TCP business records directly inside bihash.
- Running the full workspace test suite as part of normal iteration.
- Redesigning TCP session ownership, frame ownership, session FIFO, buffer ownership, or node scheduling.
- Changing TCP protocol semantics, congestion control, recovery behavior, or session runtime scheduling.
- Reintroducing app-ring, submission/completion, or io_uring-style dataplane surfaces.

## Further Notes

- The issue should be labeled `ready-for-agent`.
- The implementation plan already exists and decomposes the work into three agent-friendly tasks.
- The current local GitHub CLI authentication is invalid, so publishing may require re-authentication before the issue can be created.

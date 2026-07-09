# Runtime Ownership, Core Data-Plane Primitives, Adapter Removal, And iOS Retirement PRD

## Problem Statement

Hammer still carries an old crate boundary and product story from its iOS NetworkExtension origin. The Packet Graph runtime, data-plane buffer/frame primitives, graph identity vocabulary, platform adapter traits, and stale iOS/FFI documentation are split across crates in a way that no longer matches Hammer's current VPP-style daemon, CLI, session, and Packet Graph architecture.

The user needs the architecture to stop preserving compatibility with the old adapter and iOS support surfaces. Graph execution policy should live with the runtime main loop, shared packet-path primitives should live in core, and callers should depend directly on the actual owners instead of routing through a compatibility crate.

## Solution

Move Hammer to a direct core/runtime architecture:

- `hammer-core::data_plane` owns Data-Plane Primitives and Graph Identity.
- `hammer-runtime` owns Graph Runtime execution contracts and scheduling.
- `hammer-service`, `hammer-app`, generated graph code, daemon, CLI, and IPC use `hammer-core` and `hammer-runtime` directly.
- `hammer-adapter` is removed from the workspace instead of retained as a thin layer.
- iOS, NetworkExtension, Swift, FFI, xcframework, and generated iOS artifact support are retired and removed from code, build targets, docs, and tests.
- The project-facing name in docs becomes Hammer; repository renaming is not required by this PRD.
- No compatibility re-export period is allowed. Each implementation slice must leave the workspace compiling without using old adapter paths.

## User Stories

1. As a Hammer maintainer, I want Graph Runtime ownership in `hammer-runtime`, so that graph execution policy lives beside the main loop that drives it.
2. As a Hammer maintainer, I want Data-Plane Primitives in `hammer-core`, so that buffer and frame vocabulary has one shared owner.
3. As a Hammer maintainer, I want Graph Identity in `hammer-core`, so that node ids and next-arc labels can be used by buffers, traces, and runtime without depending on an adapter crate.
4. As a Hammer maintainer, I want executable graph contracts in `hammer-runtime`, so that node dispatch APIs do not leak into core primitives.
5. As a Hammer maintainer, I want the `hammer-adapter` crate removed, so that the old dependency seam cannot accumulate new responsibilities.
6. As a Hammer maintainer, I want iOS support retired, so that the project no longer preserves unsupported NetworkExtension, Swift, FFI, or xcframework surfaces.
7. As a Hammer maintainer, I want documentation to describe Hammer as a standalone VPP-style data-plane framework, so that new contributors do not optimize decisions around the retired iOS story.
8. As a Hammer runtime developer, I want `DataPlaneRuntime` to be owned by `hammer-runtime`, so that scheduling, readiness, dispatch, and runtime statistics are local to the runtime crate.
9. As a Hammer runtime developer, I want node execution traits to be owned by `hammer-runtime`, so that executable graph contracts can mention `DataPlaneRuntime` without creating dependency cycles.
10. As a Hammer runtime developer, I want node descriptors and process functions in `hammer-runtime`, so that node registration and dispatch stay in the executable graph layer.
11. As a Hammer runtime developer, I want Graph Runtime statistics in `hammer-runtime`, so that runtime observability follows the execution owner.
12. As a Hammer runtime developer, I want named Next Arc resolution in `hammer-runtime`, so that graph registration and runtime scheduling remain one cohesive subsystem.
13. As a Hammer runtime developer, I want pending-frame scheduling queues in `hammer-runtime`, so that scheduling state is not hidden inside a lower-level crate.
14. As a Hammer runtime developer, I want readiness and driver polling in `hammer-runtime`, so that the main loop drives graph state directly.
15. As a Hammer runtime developer, I want `put_next_frame` and `run_ready_nodes` in `hammer-runtime`, so that graph visibility and dispatch are runtime-owned operations.
16. As a Hammer core developer, I want data-plane buffers in `hammer-core::data_plane`, so that shared packet buffer semantics are available without an adapter dependency.
17. As a Hammer core developer, I want buffer indexes and frame indexes in `hammer-core::data_plane`, so that packet and frame identity are core data-plane primitives.
18. As a Hammer core developer, I want buffer frames in `hammer-core::data_plane`, so that frame vector ownership is not tied to runtime scheduling.
19. As a Hammer core developer, I want `Frame<Next>` and `Frame<Pending>` in `hammer-core::data_plane`, so that RAII frame ownership stays with buffer/frame primitives.
20. As a Hammer core developer, I want packet cursors in `hammer-core::data_plane`, so that parsed packet offset facts are shared domain primitives.
21. As a Hammer core developer, I want node error encoding in `hammer-core::data_plane`, so that buffer error metadata can refer to Graph Identity without an adapter crate.
22. As a Hammer core developer, I want node ids in `hammer-core::data_plane`, so that buffers, traces, runtime, and service code use one identity vocabulary.
23. As a Hammer core developer, I want node handles in `hammer-core::data_plane`, so that handoff and runtime lookup vocabulary remain core identity facts.
24. As a Hammer core developer, I want node kinds and states in `hammer-core::data_plane`, so that graph identity remains distinct from executable graph policy.
25. As a Hammer core developer, I want node registrations in `hammer-core::data_plane`, so that static graph metadata can be shared by macros and runtime.
26. As a Hammer core developer, I want Next Arc label traits in `hammer-core::data_plane`, so that static next tables are graph identity, not runtime execution policy.
27. As a Hammer infra developer, I want generic infrastructure to remain in `hammer-infra`, so that core does not absorb pools, slices, heaps, hash tables, timers, SIMD, prefetch, rings, or FIFOs.
28. As a Hammer service developer, I want service nodes to import graph execution APIs from `hammer-runtime`, so that the owner is visible at call sites.
29. As a Hammer service developer, I want service nodes to import buffer/frame and Graph Identity APIs from `hammer-core`, so that packet-path primitives have one obvious owner.
30. As a Hammer app developer, I want app-facing code to use core/runtime directly, so that app/runtime integration no longer depends on an adapter layer.
31. As a Hammer macro maintainer, I want generated graph code to reference `hammer-core` and `hammer-runtime` directly, so that macros cannot revive adapter paths.
32. As a Hammer macro maintainer, I want macro output tests to reject `hammer-adapter` paths, so that generated code respects the new crate owners.
33. As a Hammer reviewer, I want no compatibility re-exports, so that old boundaries cannot quietly survive the migration.
34. As a Hammer reviewer, I want every vertical slice to compile without adapter compatibility, so that migration progress is real rather than hidden behind aliases.
35. As a Hammer reviewer, I want source guard tests for removed adapter surfaces, so that future changes cannot reintroduce them.
36. As a Hammer reviewer, I want source guard tests for retired iOS surfaces, so that unsupported platform artifacts do not return by accident.
37. As a Hammer reviewer, I want tests at the workspace boundary, so that crate ownership and dependency direction are verified from the highest useful seam.
38. As a Hammer reviewer, I want targeted runtime graph tests, so that scheduling, dispatch, readiness, stats, and named Next Arc behavior survive the move.
39. As a Hammer reviewer, I want targeted core data-plane tests, so that buffer/frame primitives survive the move without behavioral regressions.
40. As a Hammer reviewer, I want service packet graph tests to keep passing, so that service behavior does not change while ownership moves.
41. As a Hammer reviewer, I want app/session tests to keep passing, so that removing adapter does not break app/runtime session behavior.
42. As a Hammer reviewer, I want docs updated in the same product direction, so that README and agent guidance do not contradict the code.
43. As a future contributor, I want one canonical module path for Data-Plane Primitives, so that I can find buffer/frame definitions quickly.
44. As a future contributor, I want one canonical module path for Graph Runtime contracts, so that I can find node execution APIs quickly.
45. As a future contributor, I want stale iOS references removed, so that I do not design for a platform the project no longer supports.
46. As a future contributor, I want ADRs for the boundary changes, so that I can understand why the adapter and iOS paths disappeared.
47. As an implementation agent, I want large but compiling vertical slices, so that I can migrate direct owners without a compatibility phase.
48. As an implementation agent, I want acceptance scans for forbidden symbols, so that I can verify deletion without manually auditing every import.
49. As an implementation agent, I want the project identity clarified as Hammer, so that documentation updates are consistent.
50. As a maintainer, I want GitHub repository renaming out of scope, so that this code refactor does not depend on external project administration.

## Implementation Decisions

- Graph Runtime is owned by `hammer-runtime`.
- Graph Runtime includes `DataPlaneRuntime`, executable node traits, node descriptors, node process functions, graph registration entries, scheduling queues, readiness, dispatch, named Next Arc resolution, driver polling, current-node execution context, graph runtime statistics, `put_next_frame`, and `run_ready_nodes`.
- Data-Plane Primitives are owned by `hammer-core::data_plane`.
- Data-Plane Primitives include data-plane buffers, buffer indexes, frame indexes, buffer frames, frame pool ownership, packet cursors, frame RAII ownership states, and buffer node error metadata.
- Graph Identity is owned by `hammer-core::data_plane`.
- Graph Identity includes node ids, node handles, node kinds, node states, node registrations, and next-arc label/storage traits.
- `hammer-infra` remains the owner of generic infrastructure data structures and memory utilities.
- Executable graph contracts are not moved into `hammer-core` because they depend on `DataPlaneRuntime` and belong to Graph Runtime policy.
- `hammer-adapter` is removed from the workspace rather than kept as a compatibility or facade crate.
- External adapter traits for inbound, outbound, endpoint, platform, socket protection, OS-facing contracts, FFI decoupling, and runtime decoupling are deleted rather than migrated as adapter surfaces.
- Service, app, daemon, CLI, IPC, and generated graph code use `hammer-core` and `hammer-runtime` directly.
- Generated graph code must use direct owner paths: core for Data-Plane Primitives and Graph Identity, runtime for executable graph contracts and `DataPlaneRuntime`.
- No compatibility re-export phase is allowed.
- Each migration slice may be large, but it must leave the workspace compiling without old adapter aliases.
- `hammer_core::data_plane` is the canonical module namespace for data-plane primitives and graph identity. It may have internal buffer, frame, and graph submodules, with top-level re-exports for common types.
- iOS, NetworkExtension, Swift, FFI, xcframework, generated iOS output, and `dist/ios` conventions are retired and deleted.
- iOS support is not a future goal for this project.
- Documentation should identify the project as Hammer, not as an iOS-first project.
- Renaming the GitHub repository is out of scope.
- Existing ADRs for Graph Runtime ownership, Data-Plane Primitive ownership, adapter removal, and iOS retirement are the architectural decisions for this PRD.

## Testing Decisions

- The highest useful test seam is the workspace boundary: the workspace should compile and test without the `hammer-adapter` crate or any iOS support surface.
- Good tests should assert external behavior and crate ownership boundaries, not private implementation details such as exact internal struct layout.
- Core data-plane tests should prove buffer allocation, free, frame ownership, packet cursor behavior, buffer-chain behavior, trace-related buffer metadata, node error metadata, and frame RAII behavior survive the move.
- Runtime graph tests should prove graph registration, named Next Arc resolution, driver node scheduling, pending-frame dispatch, readiness, current-node context, runtime statistics, node error counters, handoff draining, and `put_next_frame` behavior survive the move.
- Service-level packet graph tests should remain the behavioral guard for IP, ICMP, TCP, UDP, session queue, TUN/TAP, lookup, output, and reassembly nodes after imports move.
- App/session tests should verify app/runtime session behavior after direct core/runtime dependencies replace adapter dependencies.
- Macro tests should verify generated graph code uses `hammer-core` and `hammer-runtime` owner paths and does not generate `hammer-adapter` paths.
- Workspace dependency tests should verify `hammer-adapter` is not a workspace member and no crate depends on it.
- Source guard tests should reject reintroduction of adapter-owned graph/runtime/buffer symbols.
- Source guard tests should reject iOS support surfaces, including NetworkExtension, Swift bindings, xcframework packaging, iOS FFI, generated framework output, and `dist/ios` conventions.
- Documentation guard tests or scans should reject stale project identity claims that Hammer is iOS-first or that iOS packaging is supported.
- Existing prior art includes adapter buffer tests, adapter node runtime tests, runtime constructor surface tests, service packet graph tests, frame owner cleanup tests, session runtime tests, and macro expansion tests. These should be moved or rewritten at their new owners rather than discarded.
- Focused crate tests are preferred while iterating; a workspace test run is required before the migration is considered complete.
- Formatting and linting should run after the final vertical slice.

## Out of Scope

- Renaming the GitHub repository or changing remote issue tracker identity.
- Preserving `hammer-adapter` as a compatibility facade.
- Adding compatibility re-exports from adapter, core, or runtime to hide old paths.
- Reintroducing a runtime owner trait to keep frame ownership in the old crate location.
- Merging `hammer-infra` into `hammer-core`.
- Redesigning Graph Fanout behavior beyond what is necessary to move ownership.
- Changing TCP, UDP, IP, ICMP, session, lookup, congestion control, recovery, or timer semantics.
- Adding new platform support surfaces.
- Restoring iOS support later as part of this work.
- Keeping stale iOS build, packaging, or documentation surfaces as deprecated features.

## Further Notes

- This PRD synthesizes the agreed grilling session and the new ADRs for Graph Runtime ownership, core data-plane primitives, adapter removal, and iOS retirement.
- The migration intentionally favors direct ownership over compatibility shims.
- The expected implementation style is a sequence of large vertical slices, each preserving compilation without adapter re-exports.
- The issue should be labeled `enhancement` and `ready-for-agent`.

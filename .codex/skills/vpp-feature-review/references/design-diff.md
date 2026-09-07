# VPP Design Diff

Use this workflow for a proposed Hammer ADR or refactor before implementation.
Its completion criterion is an auditable design in which every material change
has VPP evidence, an explicit decision, and an executable verification case.

## Inputs

Establish these inputs from the request and repository:

- the Hammer subsystem, owner, and affected crates;
- the current Hammer types, APIs, call sites, tests, and lifecycle;
- the proposed ADR or refactor behavior;
- the closest VPP types, functions, call paths, and tests;
- compatibility, ABI, persistence, plugin, and rollout constraints.

If the requester names only a broad VPP concept, find the concrete analog in
the vendored tree before evaluating the design. Read
[vpp-review-areas.md](vpp-review-areas.md) for search starting points.

## Workflow

1. **Bound the slice.** State what decision is being made and what adjacent
   behavior is outside the current comparison. List the Hammer owners and
   public or cross-DSO surfaces that can change.
2. **Establish the Hammer baseline.** Read definitions and representative call
   sites. Record who allocates, mutates, publishes, transfers, and releases each
   value, plus current errors and tests.
3. **Trace the VPP analog.** Read definitions, initialization, mutation,
   dispatch, teardown, and at least one normal call path. Search tests and
   debug assertions. Capture exact paths and symbols for every claimed fact.
4. **Build the three-way diff.** Compare current Hammer, proposed Hammer, and
   VPP across ownership, scheduling, lifecycle, data movement, layout/ABI,
   errors, synchronization, and tests. Include only dimensions that affect the
   bounded slice.
5. **Resolve decisions.** Align semantics and ownership with VPP unless a
   Hammer constraint requires a documented divergence. Use Rust borrowing,
   visibility, and RAII to express the result without importing unnecessary C
   pointer surfaces or names.
6. **Inventory the change.** List every added, modified, and removed type and
   API, including transitive callers, compatibility, migration, and rollout.
   Use the repository's design change inventory format when present.
7. **Derive the test matrix.** Map every material decision and divergence to an
   executable test. Use compiled layout probes for C ABI facts, real dynamic
   loading for plugin contracts, and observable lifecycle tests for ownership
   and scheduling. Source-text matching is not behavioral proof.
8. **Audit the document.** Remove behavior unsupported by VPP or Hammer policy,
   stale alternatives, duplicated ownership models, and open questions already
   answered by evidence. Confirm that the prose, inventory, and test matrix
   describe the same final design.

## Evidence Ledger

Use a compact ledger while researching:

| ID | Source class | Path and symbol | Verified behavior | Design consequence |
| --- | --- | --- | --- | --- |
| E1 | VPP | exact vendored path and symbol | behavior established by definition and call path | decision constrained by this fact |
| H1 | Hammer | exact path and symbol | current behavior and owner | migration required |

`Source class` is `VPP`, `Hammer`, or `Repository contract`. Keep inference out
of the verified-behavior column. Put uncertain interpretation in the open
questions section.

## Three-Way Diff

| Dimension | Current Hammer | Proposed Hammer | VPP evidence | Decision and impact |
| --- | --- | --- | --- | --- |
| Ownership | current owner and release point | proposed owner and release point | evidence IDs | aligned change or explicit divergence |

Cover each material dimension exactly once. A missing VPP error branch or test
is evidence only after a scoped search; record the search and describe tests as
derived from VPP implementation when no upstream test exists.

## Decision Record

Give decisions stable IDs so tests and inventory entries can refer back to
them:

| ID | Final decision | Alignment | Rationale | ADR location |
| --- | --- | --- | --- | --- |
| D1 | concrete behavior and owner | Aligned or intentional divergence | evidence IDs and Hammer constraint | heading or paragraph |

A decision is incomplete when it names only a data structure without its
owner, transition boundary, release behavior, failure behavior, and affected
API surface.

## Test Matrix

| Test | Level and setup | Required assertions | Decision IDs | VPP evidence |
| --- | --- | --- | --- | --- |
| domain-named test | unit, integration, subprocess, DSO, or C probe | observable behavior and unchanged state | D1 | E1 |

The matrix must cover normal transitions, boundary conditions, failure
atomicity, lifecycle completion, and each intentional divergence. Do not invent
an error or fatal test for a state VPP merely excludes through scheduling or
ownership; test the establishing boundary instead.

## Design Report

Write or update the ADR, then report:

- scope and affected owners;
- VPP analog and evidence ledger;
- three-way semantic diff;
- final decisions and intentional divergences;
- added, modified, and removed types/APIs;
- test matrix;
- open questions and the exact missing evidence;
- verdict: `Aligned`, `Needs decisions`, or `Rejected`.

An `Aligned` verdict requires every material difference to have a final
decision and every decision to have verification. It is a design verdict, not a
claim that the current implementation already conforms.

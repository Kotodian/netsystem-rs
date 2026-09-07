---
name: vpp-feature-review
description: Compare proposed or completed Hammer VPP-style changes with vendored VPP. Use when drafting or reviewing an ADR or refactor described as VPP-aligned, and after implementing data-plane graph, buffer, FIFO, session, transport, runtime, barrier, IPC, or plugin changes; produces source evidence, a semantic diff, decisions, a test matrix, and completion findings.
---

# VPP Feature Review

## Modes

- **Design diff:** Before approving a non-trivial VPP-aligned ADR or refactor,
  read [references/design-diff.md](references/design-diff.md) and produce the
  evidence, differences, decisions, change inventory, and test matrix it
  requires.
- **Completion review:** After implementation, run the completion workflow
  below. Do not declare the feature complete until the review is written and
  every blocking finding is fixed or explicitly accepted.

For either mode, read root `CONTEXT.md`, `AGENTS.md`, and relevant `docs/adr/`.
Search `third_party/vpp/` first and use external VPP sources only when the
required code is absent from the vendored tree.

## Shared Evidence Rules

- Record exact VPP paths, symbols, and relevant call sites. A type definition
  alone does not establish ownership, scheduling, or lifecycle semantics.
- Record the current Hammer owner, API, call sites, and tests before judging a
  proposal or diff.
- Separate verified Hammer facts, verified VPP facts, design decisions,
  inferences, and unresolved evidence. Never present an inference as source
  behavior.
- Support claims that VPP lacks a behavior, error branch, API, or test with a
  scoped repository search. If VPP has no dedicated test, derive Hammer tests
  from implementation behavior and say that explicitly.
- Treat VPP as the semantic and ownership reference, not a 1:1 Rust API,
  data-structure, or naming template. Record every intentional semantic
  divergence with its reason, owner, and regression test.
- When VPP evidence determines the design, resolve it in the document. Leave an
  open question only when the remaining choice is Hammer product policy or the
  evidence is genuinely incomplete.

## Completion Workflow

1. Define the scope. Identify the changed crates and files, the feature contract, and the closest VPP analog.
2. Collect VPP evidence. Use `rg` to find the counterpart paths, types, functions, and call sites in `third_party/vpp/`. Record exact evidence for each comparison.
3. Compare the implementation with VPP. Check ownership, scheduling, data movement, lifecycle, error handling, API boundaries, and tests.
4. Write findings. Use `Blocking` or `Non-blocking`, each with VPP evidence, Hammer evidence, the gap, the impact, and the action.
5. Resolve blocking findings. Fix them, rerun focused tests, and update the review before rerunning the gate.
6. Verify the diff. Run `cargo fmt --all -- --check`, `git diff --check`, focused `cargo test -p <crate>`, and the broader test suite when the feature touches shared behavior.

## Comparison Checklist

Compare the feature with VPP on these dimensions:

- Semantics and ownership: Does Hammer own the same state in the same owner as VPP? State must be worker-owned or barrier-owned; do not add locks, atomics, or shared observers around worker-owned protocol state.
- Scheduling: Does the graph node, session runtime, or worker execute the same transition at the same boundary? Do not let a transport or congestion controller schedule nodes.
- Data movement: Is the app/session boundary the only payload copy point? Does TX flow through session-owned FIFO bytes without intermediate payload vectors or private copies?
- Buffer semantics: Does buffer sharing follow VPP `attach_clone`/refcount and chain-header behavior? Do not add feature-specific buffer ownership or runtime copy helpers.
- Session and app boundary: Are app-session messages delivered through the exact target session using FIFO plus message-queue semantics? Reject root-session scans, `AppRing`/SQE/CQE surfaces, and generic chain traversal.
- Lifecycle and errors: Are expected packet failures classified at the node/protocol, are control-plane failures typed, and is mutation failure-atomic? Do not turn programmer bugs into recoverable errors.
- API and ABI: Is the change inside the crate dependency graph, free of `dyn` in production, and free of unapproved new public APIs?
- Tests: Do tests exercise real behavior and concrete variants? Do not use source-text assertions or display-string-only error tests.

## Review Report

Produce a concise report with this structure:

- Feature and changed surface
- VPP analog and evidence
- Verdict: `Aligned`, `Needs changes`, or `Rejected`
- Findings, ordered by severity
- Commands run

Add the report to the PR or `docs/superpowers/sdd/` according to project convention.

## Reference Map

Read `references/vpp-review-areas.md` when selecting VPP counterparts for graph, buffer, session, TCP/TLS, SVM, vppinfra, and runtime barrier work.

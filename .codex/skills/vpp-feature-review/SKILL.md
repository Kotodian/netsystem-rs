---
name: vpp-feature-review
description: Run a mandatory post-completion review of Hammer features against vendored VPP before declaring a feature done. Use after completing or changing data-plane graph nodes, buffers, FIFOs, app/session boundaries, session events, TCP/TLS transport, runtime scheduling, barriers, IPC/plugins, or other VPP-style architecture work; use it to compare semantics, ownership, lifecycle, error handling, and tests with third_party/vpp and to produce an approval-ready review.
---

# VPP Feature Review

## Gate

Treat this as a completion gate, not an optional audit. Do not declare a feature complete until the review is written and every blocking finding is fixed or explicitly accepted.

Read root `CONTEXT.md`, `AGENTS.md`, and relevant `docs/adr/` before reviewing. Search `third_party/vpp/` first and use external VPP sources only when the required code is absent from the vendored tree.

## Workflow

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

Treat VPP as a semantic and ownership reference, not a 1:1 API or naming template. If Hammer intentionally diverges, state the rationale explicitly in the report.

Add the report to the PR or `docs/superpowers/sdd/` according to project convention.

## Reference Map

Read `references/vpp-review-areas.md` when selecting VPP counterparts for graph, buffer, session, TCP/TLS, SVM, vppinfra, and runtime barrier work.

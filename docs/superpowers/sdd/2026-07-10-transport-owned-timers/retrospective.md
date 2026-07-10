# Transport-Owned Timers Retrospective

## Decision-Critical Observations

### 1. The session/transport boundary is a state-machine boundary, not only a module boundary

- **Source:** Current project.
- **Evidence:** The first implementation moved TCP storage and dispatch behind `TcpWorker`, but review still found incorrect app-first and transport-first close ordering. Session lifecycle and TCP protocol state can each be locally valid while their cross-boundary transition is wrong.
- **Impact:** Import cleanup and trait extraction alone cannot prove the architecture. A future QUIC worker would amplify this because one connection may drive several stream sessions through independent close and deletion phases.
- **Action:** Treat production-seam lifecycle tests as architecture tests. Exercise queued app close, real transport close, delayed transport deletion, stale generation, and final app cleanup through `SessionQueueNode` and transport notifications.

### 2. Static dispatch still needs runtime identity discipline

- **Source:** Current project.
- **Evidence:** The generic session transport set removes `dyn`, but worker queue registration still crosses a type-erased `NodeRuntimeData` boundary. Review identified that cache identity must include the runtime, concrete congestion controller, and session backend to avoid reinterpreting leaked storage as the wrong monomorphized queue type.
- **Impact:** Generics provide compile-time safety only until a raw or erased runtime handle is cached. A mismatch here can become unsound even though every public trait signature is statically dispatched.
- **Action:** Keep typed construction and backend selection inseparable, restrict generic registration entry points, key or recreate cached storage using runtime plus concrete type identity, and test sequential runtimes and Local/SVM mismatch behavior.

### 3. Timer ownership migration exposes hot-path cost that boundary tests will not catch

- **Source:** Current project.
- **Evidence:** Review found receive paths cloning `TcpConnection` to synchronize timers and publish lookup state. Cloning recovery state rebuilds pools, trees, and outstanding samples, potentially more than once per packet.
- **Impact:** The ownership refactor can be behaviorally correct while introducing allocation-heavy work on the packet hot path. That contradicts Hammer's worker-local, lock-free, VPP-style data-plane goals.
- **Action:** Split borrows around `TcpWorker` fields so timer synchronization and lookup publication operate on the owned connection in place. Add a code-quality gate that rejects deep connection clones in input, established, and receive-processing paths.

## Skill Candidates

### 1. `vpp-boundary-refactor`

- **Source:** Current project for the repeated workflow; general usefulness is an inference.
- **Trigger:** A request moves TCP, QUIC, session, runtime, recovery, or buffer policy across a Hammer module boundary and asks to align semantics with VPP.
- **Input:** Requested ownership move, affected files, current ADRs and `CONTEXT.md`, relevant VPP source references, and proposed new types/APIs.
- **Output:** Boundary contract, VPP semantic comparison, forbidden dependency searches, approval table for new APIs, production-seam tests, and an implementation plan.
- **SKILL.md fit:** Yes. The workflow and invariants repeat, while protocol-specific evidence can remain task input.

### 2. `typed-transport-state-machine`

- **Source:** Current project; recurrence across future QUIC work is an inference that should be validated after the first QUIC slice.
- **Trigger:** Session lifecycle, transport lifecycle, protocol close states, generation-safe indexes, or static transport dispatch are being added or refactored.
- **Input:** State vocabulary, legal events, index-retention rules, cleanup ownership, and compile-time transport set.
- **Output:** Transition table, type-state shape, invalid-state audit, stale-generation cases, and seam-level RED tests for every observable transition order.
- **SKILL.md fit:** Yes, provided it stays domain-focused and does not prescribe one universal Rust typestate encoding.

### 3. `dataplane-refactor-review`

- **Source:** Current project for clone/type-erasure findings; frequency across other changes needs more history to verify.
- **Trigger:** A refactor passes functional tests but touches packet input, worker-local state, buffer ownership, TLS caches, pools, or runtime handles.
- **Input:** Diff, hot-path files, ownership ADRs, architecture guardrails, and focused benchmark or allocation evidence when available.
- **Output:** Review ordered by soundness, ownership regression, payload-copy/deep-clone cost, stale-handle risk, and missing end-to-end tests.
- **SKILL.md fit:** Probably. Confirm against several prior refactors before freezing exact checks; otherwise it risks encoding one incident as a general rule.

## Confidence And Follow-Up

- **Current-project conclusions:** The lifecycle seam, erased runtime-handle risk, current deep-clone cost, and the concrete actions above are grounded in this branch's code and review findings.
- **Inferences:** QUIC will increase lifecycle fan-out pressure; the VPP boundary workflow is reusable beyond this task; typed transport review will recur.
- **Needs more history:** Whether deep connection cloning and runtime type-erasure are recurring repository-wide patterns, and whether `typed-transport-state-machine` deserves a standalone skill instead of being a section of `vpp-boundary-refactor`.

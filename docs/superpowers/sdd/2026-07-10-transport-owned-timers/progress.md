# Transport-Owned Timers Progress

- Parent: https://github.com/Kotodian/hammer-ios-rs/issues/41
- Branch: `codex/hammer-app-ring-zero-copy`
- Test seam: SessionQueueNode/static transport set for scheduling, lifecycle, and TX; private TcpWorker timer seam for exact timer policy.

## Tasks

- [x] Triage issue and publish/persist agent brief.
- [x] Persist design PRD and ADR vocabulary.
- [x] Verify `cargo test -p hammer-service` baseline.
- [x] Task 1: Static transport seam, typed lifecycle, and TCP-owned connections.
- [x] Task 2: Private exact TCP timer engine.
- [ ] Task 3: Migrate all TCP timer policy into TcpWorker.
- [ ] Task 4: Delete legacy surfaces and lock architecture guardrails.
- [ ] Task 5: Final reviews, full verification, push, and `target` cleanup.

## Global Constraints

- Follow VPP timer ownership and exact-dispatch semantics, not VPP's C API shape.
- Use generics and static dispatch; no `dyn`, protocol enum in session, or TLS type erasure.
- Session code contains no TCP/QUIC timer semantics.
- Use `hammer_infra::pool::Index` directly; field names describe roles and do not mirror type names mechanically.
- No new hammer-infra API, timer action carrier, timer epoch/binding type, payload copy, `TcpQueue`, or `Live` wrapper.
- Each implementation task follows RED -> GREEN, then spec review, then code-quality review before the next task.

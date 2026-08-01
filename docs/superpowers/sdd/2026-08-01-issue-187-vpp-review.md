# VPP Feature Review: Adaptive Session Worker readiness

## Feature and changed surface

Issue #187 adds VPP-shaped adaptive Session Worker scheduling to the private
Application MQ path. The worker now has `Polling`, `Interrupt`, and `Idle`
states, a state-derived deadline, and state transitions after Session Queue
dispatch. `appsl-rx-mqs-input` wakes `session-queue` only while the worker is in
`Interrupt`; pending Application MQs still self-wake the appsl node.

The changed production surface is:

- `hammer-runtime::file`: generation-safe `Deadline` registrations, callback
  dispatch, one-shot rearming, and typed deadline errors. Linux uses a
  `timerfd` observed through the existing `io_uring` poller; macOS uses a
  one-shot `kqueue` `EVFILT_TIMER`.
- `hammer-service::session::runtime`: Session Worker state/deadline ownership,
  state-derived node scheduling, deadline installation/removal, and
  failure-atomic worker installation rollback.
- `hammer-service::session::node`: state-gated Session Queue wakeup and the
  existing node-local error counter path.
- `hammer-runtime/tests/file_readiness.rs` and Session Worker tests: deadline
  lifecycle, expiry, disarm, state transitions, and appsl wake behavior.

## VPP analog and evidence

- VPP defines `Polling`, `Interrupt`, and `Idle` in
  `third_party/vpp/src/vnet/session/session.h:56-61`. Hammer's
  `SessionWorkerState` is the worker-owned counterpart in
  `crates/hammer-service/src/session/runtime.rs:48-73`.
- VPP derives the worker timeout from state and updates its timerfd in
  `third_party/vpp/src/vnet/session/session_node.c:43-78`. Hammer maps
  `Interrupt` to 1 ms, `Idle` to 100 ms, and `Polling` to disarmed in
  `crates/hammer-service/src/session/runtime.rs:45-73`; `FileMain` owns the
  platform registration in `crates/hammer-runtime/src/file/mod.rs:230-348`.
- VPP transitions state after Session Queue dispatch using pending event-list
  count, the last-vector metric, and session-pool emptiness
  (`third_party/vpp/src/vnet/session/session_node.c:1995-2029`). Hammer applies
  the same transition shape in
  `crates/hammer-service/src/session/runtime.rs:802-865`. Hammer intentionally
  uses `SessionQueueOutput::io_count()` as its current worker-local proxy for
  `vlib_last_vectors_per_main_loop()`; this is an implementation difference,
  recorded below.
- VPP's appsl input drains a bounded MQ snapshot, re-adds non-empty queues as
  postponed, self-wakes while pending remains, and wakes Session Queue only in
  `SESSION_WRK_INTERRUPT`
  (`third_party/vpp/src/vnet/session/application.c:374-419`). Hammer's
  `AppRxMqEntry` snapshot/postponed handling is in
  `crates/hammer-service/src/session/runtime.rs:214-250`, and the state-gated
  wake is in `crates/hammer-service/src/session/node.rs:83-107`.
- VPP registers timerfd readiness as a File and makes the callback only mark
  Session Queue interrupt-pending
  (`third_party/vpp/src/vnet/session/session_node.c:2180-2217`). Hammer's
  deadline callback has the same observer/consumer split in
  `crates/hammer-service/src/session/runtime.rs:3835-3845`; Session Queue does
  the actual work after readiness.
- VPP keeps the timerfd and File index in the worker structure
  (`third_party/vpp/src/vnet/session/session.h:107-141`) and disables Session
  Queue during worker exit under a barrier
  (`third_party/vpp/src/vnet/session/session_node.c:2220-2233`). Hammer keeps
  the deadline index in the worker-owned `SessionWorker` and uses the existing
  worker runtime/File ownership boundary.

Hammer intentionally differs from VPP in two implementation details. VPP uses
a repeating timerfd; Hammer uses a one-shot deadline and rearms it only after
the callback (`crates/hammer-runtime/src/file/mod.rs:497-511`). This preserves
the same state-derived wake cadence while preventing a periodic source from
accumulating an extra wake during dispatch. Hammer also has only Data Workers,
not VPP's separately scheduled Main Thread Session Worker, so it uses the Data
Worker timeout values directly.

## Verdict

**Aligned.** No blocking VPP semantic, ownership, scheduling, error, or test
finding was found in the Issue #187 surface.

## Findings

### Blocking

None.

### Non-blocking

1. **Worker teardown leaves an observational deadline index.** A Data Worker
   owns and drops its own `DataPlaneRuntime`/`FileMain` when the worker loop
   exits (`crates/hammer-runtime/src/data_plane.rs:218-251` and
   `crates/hammer-runtime/src/spawn.rs:237-248`), while the `SessionMain`
   `ThreadOwned<SessionWorker>` may still retain `state_deadline_file`
   (`crates/hammer-service/src/session/runtime.rs:193-212`). A later cleanup
   call can therefore observe `DeadlineIndexInvalid`. The accepted lifecycle
   contract now requires readiness/deadline cleanup while `FileMain` is alive;
   after worker exit the retained Session Worker slot is observational only and
   is not re-entered or restarted. The contract is exercised by
   `session_worker_teardown_removes_deadline_before_runtime_drop`. A future
   worker-restart design would need an explicit teardown hook before changing
   that contract.

2. **The adaptive throughput input is a deliberate proxy.** VPP uses
   `vlib_last_vectors_per_main_loop()` (`third_party/vpp/src/vnet/session/session_node.c:2001-2013`),
   while Hammer passes the current Session Queue output count from
   `crates/hammer-service/src/session/runtime.rs:2751-2764`. The thresholds and
   transitions are aligned, but the metric is not identical when other graph
   work contributes to a Data Worker loop. Validate this proxy with runtime
   measurements or add a worker-owned last-vector metric before tuning the
   thresholds for production load.

3. **Documentation has one stale predecessor statement and Linux coverage is
   CI-dependent.** `docs/adr/0026-session-side-tls-and-crypto-update-ownership.md`
   still said Session Queue must remain in `Polling` until a deadline source
   equivalent to VPP's timerfd exists, while Issue #187 now supplies that
   source; `CONTEXT.md` and ADR-0027 already describe the new state model.
   ADR-0026 now records the adaptive deadline and the deliberate `io_count()`
   metric proxy, and ADR-0027 is accepted. The Darwin run compiles and
   exercises the kqueue backend; the Linux `timerfd`/io_uring backend was
   source-reviewed but the local target check was attempted and could not run
   because `x86_64-unknown-linux-gnu` is not installed. Retain Linux CI
   coverage for the platform-specific backend.

## Commands run

- Vendored VPP inspection with `rg` and numbered source inspection across
  `vnet/session/session_node.c`, `application.c`, and `session.h`.
- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo check -p hammer-runtime --tests`
- `cargo test -p hammer-runtime --test file_readiness` (10 passed)
- `cargo check -p hammer-runtime --target x86_64-unknown-linux-gnu` (not run:
  target `core` is not installed locally)
- `cargo test -p hammer --test session_queue_readiness` (4 passed)
- `cargo test -p hammer-service --lib session::runtime::tests` (14 passed)
- `cargo test --workspace --quiet` (passed)
- `cargo clippy --workspace --all-targets` (passed with existing warnings).
- Linux target check was attempted but could not compile because
  `x86_64-unknown-linux-gnu` is not installed in the local Rust toolchain.

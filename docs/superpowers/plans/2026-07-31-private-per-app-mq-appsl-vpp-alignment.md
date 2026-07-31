# Private per-app MQ and appsl-rx-mqs-input VPP alignment

Parent decision: ADR-0027

## Goal

Introduce private per-Application message queues, align `appsl-rx-mqs-input` with VPP, and remove the app-event `session_evt_q` hop. External Application performance and MQ isolation are the primary constraints.

## Constraints

- VPP already defines the appsl pending MQ, POSTPONED, direct event-lane drain, worker-state, and timerfd behavior; implement those semantics rather than inventing new ones.
- `DriverNode` is Hammer's INPUT equivalent; `appsl-rx-mqs-input` remains a Driver node.
- `ApplicationMain` owns per-app MQ resources; Data Workers only hold worker-local registrations.
- Transport TimerWheel stays transport-owned and continues to be advanced by session-queue `update_time`.
- No new `dyn` dispatch, no business wrappers, no reintroduction of `AppRing`/io_uring app-session surfaces.

## Task 1: Add generic linked list to hammer-infra

Files:
- `crates/hammer-infra/src/linked_list.rs`
- `crates/hammer-infra/src/lib.rs`
- `crates/hammer-infra/tests/linked_list.rs`

Build `LinkedList<T>` backed by the existing `Pool`. Support multiple `Head` handles over one node store, `add_tail`, `remove`, `move_to_back`, iteration, and generation-safe handles.

Acceptance:
- Multi-head list operations are covered by tests.
- Stale handles cannot remove a replacement node.
- `move_to_back` preserves tail ordering.

## Task 2: Add app MQ capacity config

Files:
- `crates/hammer-service/src/session/config.rs`

Add `app_mq_capacity` to `[network.session]` with default 2048 and minimum 128.

Acceptance:
- Config parsing and validation tests cover default, explicit value, and invalid zero/small values.

## Task 3: ApplicationMain owns per-app MQ resources

Files:
- `crates/hammer-service/src/session/application.rs`
- `crates/hammer-service/src/session/mod.rs`

Add `attach_external`/`attach_local` and `ApplicationMqResources`. Create one `SessionMsgQueue` per Data Worker, all with signal endpoints. Require SessionMain/Data Workers to be ready before attach returns; register all workers with failure rollback.

Acceptance:
- Attach rollback removes partially installed worker registrations.
- Detach drains remaining MQ events, removes worker entries, then drops MQ/segment.
- Local and external Applications both own per-app MQs.

## Task 4: Worker-side AppRxMqEntry and FileMain registration

Files:
- `crates/hammer-service/src/session/app.rs`
- `crates/hammer-service/src/session/runtime.rs`

Add `AppRxMqEntry` with `ApplicationId`, queue Arc, FileMain index, and PENDING/POSTPONED flags. Store entries in `LinkedList<AppRxMqEntry>` with an application-slot index for O(1) removal. FileMain callbacks add the entry to pending and wake `appsl-rx-mqs-input`.

Acceptance:
- One FileMain registration per Application per Data Worker.
- Pending list insertion is idempotent via PENDING flag.
- Detach removes the FileMain registration before dropping the queue.

## Task 5: Upgrade attach protocol and AppClient mapping

Files:
- `crates/hammer-runtime/src/attach.rs`
- `crates/hammer-runtime/src/attach/application.rs`
- `crates/hammer-runtime/src/app/layout.rs`
- `crates/hammer-app/src/attach.rs`
- `crates/hammer-app/src/session.rs`

Version the attach protocol. Publish `rx_mqs_segment` and one write descriptor per Data Worker. `AppClient` maps `worker_index -> SessionMsgQueue`. `accept()` no longer receives per-session TX event MQ descriptors.

Acceptance:
- Protocol version mismatch is rejected.
- Variable descriptor counts are handled safely.
- Session construction uses the per-app MQ selected by `SessionHandle.worker_index()`.

## Task 6: appsl-rx-mqs-input drains pending MQs into event lanes

Files:
- `crates/hammer-service/src/session/node.rs`
- `crates/hammer-service/src/session/runtime.rs`

Rewrite `appsl-rx-mqs-input` to iterate pending MQs, read signal fds unless POSTPONED, snapshot-drain each MQ, classify events into ctrl/new/old lanes, re-add non-empty MQs as POSTPONED, self-wake when pending remains, and wake `session-queue` only in Interrupt state.

Acceptance:
- App events do not pass through `session_evt_q`.
- Continuous producers are handled by snapshot drain + POSTPONED.
- One full Application MQ does not prevent other pending MQs from draining.

## Task 7: SessionWorker event lanes and dispatch order

Files:
- `crates/hammer-service/src/session/runtime.rs`
- `crates/hammer-service/src/session/node.rs`
- `crates/hammer-service/src/session/node/tests.rs`

Replace app-event staging with ctrl/new/old lanes. `session-queue` order becomes update time, drain internal MQ, control lane, new IO lane, old IO lane, flush TX.

Acceptance:
- Control lane dispatches before new IO.
- New IO dispatches before old IO.
- Existing transport-facing scheduling APIs keep equivalent behavior.

## Task 8: SessionWorker state machine and timerfd

Files:
- `crates/hammer-runtime/src/file/mod.rs`
- `crates/hammer-runtime/src/file/linux.rs`
- `crates/hammer-runtime/src/file/macos.rs`
- `crates/hammer-service/src/session/runtime.rs`

Add `Polling/Interrupt/Idle` worker state and a FileMain timer/deadline file abstraction. Linux uses timerfd; macOS uses the kqueue backend or an equivalent fallback. Timer values follow VPP state-derived timeouts.

Acceptance:
- Appsl only wakes `session-queue` in Interrupt.
- Worker state transitions are tested.
- Transport TimerWheel is still advanced by `update_time`; SessionWorker does not own transport timers.

## Task 9: Documentation and verification

Update `CONTEXT.md`, ADR-0027, and integration tests. Run `cargo test --workspace`, `cargo clippy --workspace --all-targets`, and `cargo fmt --all -- --check`.

Acceptance:
- No source-text assertion tests are used for architecture claims.
- External attach/accept, isolation, detach, and appsl ordering are covered by executable tests.

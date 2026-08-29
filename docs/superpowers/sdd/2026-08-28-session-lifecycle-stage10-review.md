# Session Lifecycle Stage 10 Review

## Scope

Issue #272 stage 10 closes the Session, Transport, and Application lifecycle
around listen/unlisten, connect/connect-stream, accept/accepted-reply,
close/reset/half-close, transport close, cleanup, and migration. Each path
must target the exact owner, return an owner-local typed error, and leave no
partially published sibling state after failure.

## Issue Evidence

- Complete listen/unlisten, connect/connect-stream, accept/accepted-reply,
  close/reset/half-close, transport-close, cleanup, and migration paths.
- Use exact-owner routing, typed errors, rollback/failure atomicity, and
  exactly-once cleanup for success, failure, and races.
- Unlisten must block new lookup before transport stop and retain a published
  listener when stop fails so the operation can be retried.

## Vendored VPP Evidence

- `third_party/vpp/src/vnet/session/session_node.c` routes ACCEPTED_REPLY and
  transport notifications by the exact Session handle and target worker.
- `third_party/vpp/src/vnet/session/session.c` performs listener teardown and
  transport close through the owning listener/transport records, with app
  notification before final Session free.
- `third_party/vpp/src/vnet/session/application.c` and
  `application_worker.c` remove application-owned listener/connection state
  only after the corresponding Session-side transition is resolved.

## Findings

- **Non-blocking**: the lifecycle changes are statically aligned with the
  vendored VPP ownership and transition order. The workspace build, tests,
  clippy, plugin-load checks, and final repository cleanup are intentionally
  deferred to issue #272 stage 12.
- **Non-blocking**: older best-effort cleanup expressions outside the changed
  owner transitions remain for the later full-repository audit; this stage did
  not add new dynamic dispatch, Main ownership, or compatibility surfaces.

## Changes

- Removed the Session transport action error and duplicate worker-action
  surface. `SessionWorker::open_stream`, `reset_stream`, `stop_sending`, and
  `close_connection` now derive the protocol from the target Session and call
  the single registered `TransportVft` owner shim. Missing entries use the
  existing Session/Transport typed errors; callback failures retain their
  `RuntimeError` source.
- `TransportStartListen` now returns the owner transport's `u32
  connection_index`, and `TransportStopListen` accepts that index. Session
  listener records retain the index only for the Main Thread unlisten path;
  TCP, UDP, QUIC, and HTTP callbacks no longer receive a `SessionHandle` for
  transport teardown.
- QUIC and HTTP listener authorities publish direct owner indexes from lower
  connection index to listener context. Teardown resolves those indexes in
  O(1), removes the lower Session first, then the Application listener and
  owner context, preserving the published context when lower unlisten fails.
- Removed the `with_contexts` permission/borrow helper. Each owner operation
  now performs its Main Thread/barrier check at the call site before accessing
  its own context storage.
- `SessionMain::unlisten` now marks the private listener record as no longer
  accepting under the Main Thread worker barrier before invoking the transport
  stop callback. New listener lookup/accept paths reject that record while the
  transport teardown is in progress.
- A failed transport stop restores the listener's accepting state and returns
  the typed `SessionError::TransportOpFailed`, preserving the published
  Session/Application listener pair for retry. A successful stop removes the
  Session listener exactly once.
- Session control dispatch now routes `ACCEPTED_REPLY` through the exact
  `SessionHandle.thread_index`, propagates the first worker/control failure
  after draining later MQ elements, and resolves accepted-listener metadata
  with a typed Application lookup instead of a silent `Option` fallback.
- `CONNECT_STREAM` validates the parent worker before publishing the
  Application connection record. Transport Session construction removes the
  just-inserted Session (and an external AppSession when present) if transport
  creation cannot finish.
- A rejected `ACCEPTED_REPLY` schedules disconnect for the exact Session so the
  transport owns final deletion, matching VPP's `vnet_disconnect_session`
  path. Migrated Session installation removes its partial Session on creation
  failure and preserves publication/cleanup errors together.
- Close/reset/half-close transitions record the state guard before invoking
  transport actions, and cleanup callbacks run before the owning Session slot
  is freed. Main-thread control callbacks resolve the global Session authority
  without capturing an `Arc<SessionMain>`.

## Verification

- Static audit confirms zero definitions, imports, exports, or test references
  for the removed action/error symbols and zero `with_contexts` permission
  helpers. Listener teardown call sites all use `u32 connection_index`.
- `cargo fmt --all` and `git diff --check` pass. Workspace compilation,
  tests, clippy, plugin loading, and the final full-repository VPP review are
  intentionally deferred to stage 12.

## Commands

- `cargo fmt --all`
- `cargo fmt --all -- --check`
- `git diff --check`
- `gh issue view 272 --repo Kotodian/hammer-ios-rs --comments`
- No `cargo check`, `cargo build`, `cargo test`, or `cargo clippy` was run;
  those gates belong to stage 12.
- `cargo fmt --all`, `cargo fmt --all -- --check`, and `git diff --check` are
  the only checks run before stage 12. Workspace build, clippy, unit/integration
  tests, plugin loading, and the final vendored-VPP review remain deferred to
  issue #272 stage 12.

## Verdict

Stage 10 implementation and deletion mapping are complete pending the final
workspace build, tests, clippy, plugin-load checks, and vendored-VPP review
required by stage 12.

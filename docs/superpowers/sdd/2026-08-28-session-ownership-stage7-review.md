# Session Ownership Stage 7 Review

## Feature and Changed Surface

Issue #272 stage 7 establishes the Session ownership boundary: `SessionMain`
publishes Main-side listener/policy state, while each Data Worker owns its
established and half-open Sessions, FIFOs, event scheduling, and migration or
control queues. The runtime and TCP/UDP transport paths now use one typed
runtime-thread-to-worker conversion, reject Main Thread worker access, and
validate listener and Session handles by exact owner.

## Issue Evidence

- `SessionMain` is limited to the Main-side policy/listener namespace.
- Main Thread must not run a `SessionWorker`.
- Data Workers exclusively own established/half-open Session state, FIFO/event
  scheduling, and migration/control queues.
- Listener handles use `thread_index == 0`.
- Routing must target the exact owner; Sessions must not be borrowed across
  workers.

These points are taken from the restored #272 TODO comment for stage 7.

## Vendored VPP Evidence

- `third_party/vpp/src/vnet/session/session.c` defines the process-global
  `session_main` and routes session access through the worker selected by the
  handle's `thread_index`.
- `third_party/vpp/src/vnet/session/session.h` implements
  `session_get_if_valid(handle.session_index, handle.thread_index)` and keeps
  the owner in the handle.
- `third_party/vpp/src/vnet/session/session_node.c` sends control and migration
  events to the target worker thread and keeps listener/control operations on
  thread 0.

## Changes

- `DataWorkerId::thread_index` and `TryFrom<u32>` are the single conversion
  boundary between worker slots and runtime thread indices.
- `SessionMain::with_worker_mut`, `Engine::data_worker_id`, TCP worker access,
  and UDP worker access reject thread 0 and no longer hand-write subtraction.
- `SessionMain::with_listener` rejects any non-zero listener thread index before
  consulting the listener pool.
- Ordinary connect tests verify the Session-selected transport control worker;
  listener and Main Thread ownership tests cover the rejected paths.
- `SessionWorker::session_id_from_handle` continues to require the current
  worker's exact runtime thread index before checking the local Session pool.
- `SessionWorker` is concrete; `SessionEntry`, `SessionType`, and the
  lifecycle state chain carry direct `u32` pool indexes with no identity-only
  type parameter.
- Migration queues and shutdown phase state are held by the worker migration
  authority, while `SessionMain` retains only the routing/listener policy
  surface. Closed notifications convert listener thread indexes through
  `DataWorkerId::try_from` before selecting a worker queue.

## Verification

- Static `rg` audit for remaining stage-7 hand-written worker decoding in the
  affected Session/transport/runtime paths.
- Static `rg` audit confirms no `SessionWorker<...>`, `SessionEntry<...>`,
  `SessionType<...>`, `SessionState<...>`, or identity-only lifecycle state
  generic remains in Session or plugin code.
- The queue unit test `closed_notifications_use_data_worker_slots` verifies
  the thread-index-to-worker-slot routing boundary.
- `git diff --check` passed.
- Build and test commands are intentionally deferred to issue #272 stage 12,
  per repository test-timing rules.

## Verdict

Stage 7 implementation is complete. Final workspace build, tests, clippy,
plugin load, and vendored-VPP review remain the stage 12 verification gate.

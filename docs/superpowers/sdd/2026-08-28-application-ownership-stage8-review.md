# Application Ownership Stage 8 Review

## Scope

Issue #272 stage 8 moves Application lifetime and its owned mappings into the
process-global `ApplicationMain`. The attach path publishes per-Application
message queues only after the Application record and Session workers accept the
resources; detach removes Session state first and then removes the Application's
owned listeners, connections, and MQ resources.

## Issue Evidence

- Replace the placeholder `Pool<()>` with a real Application record.
- Do not keep listeners, connections, or MQ resources in parallel containers
  indexed independently from the Application record.
- Attach and worker registration must be failure-atomic.
- Detach must clean workers, listeners, segments, and half-open state through
  the owning Application.
- Remove the old `ApplicationRegistration` RAII surface and retained Main
  references.

## Vendored VPP Evidence

- `third_party/vpp/src/vnet/session/application.c` allocates an
  `application_t` from the global application pool.
- `application_free` releases the application's worker maps, RX MQ segment,
  listener-related state, and remaining application-owned state before putting
  the application back into the pool.
- `application_detach_process` performs worker cleanup before the final
  application removal.
- `third_party/vpp/src/vnet/session/application_worker.c` maintains the
  application-to-worker map and cleans worker listener state during worker
  removal.

## Changes

- `ApplicationState.applications` now stores `Application`, not `()`.
- `Application` owns its Data Worker mapping, listener indexes, connection
  indexes, and optional `ApplicationMqResources`.
- MQ resources no longer carry a duplicate Application identity or live in a
  parallel `Vec<Option<_>>`; shared segment names use a process-local counter.
- Listener and connection registration/removal updates the owning Application
  mapping under the Main Thread's WorkerBarrier.
- Attach removes the newly allocated Application when MQ creation, Session MQ
  installation, or publication storage fails.
- Detach asks SessionMain to remove Session-side state, then removes exactly the
  indexes recorded by the Application and drops its MQ resources with the pool
  record.
- `ApplicationMain::with_state_mut` was removed; mutation sites now make the
  Main Thread and WorkerBarrier boundary explicit.
- Focused tests cover attach cleanup, Application-owned listener/connection
  mappings, worker mapping, and detach cleanup.

## Verification

- `cargo fmt --all`
- `cargo fmt --all -- --check`
- `git diff --check`
- Static audit confirms no `with_state_mut`, `ApplicationRegistration`, or old
  `ApplicationMqResources::create_local(application, ...)` remains in the
  Application ownership path.
- Build and test commands remain deferred to issue #272 stage 12, per the
  repository test-timing rule.

## Verdict

Application ownership and global Main production paths are implemented. Stage
8 remains open until the service and plugin test fixtures stop constructing
injected `ApplicationMain`/`SessionMain` owners; final workspace build, tests,
clippy, plugin-load checks, and vendored-VPP review remain stage 12 gates.

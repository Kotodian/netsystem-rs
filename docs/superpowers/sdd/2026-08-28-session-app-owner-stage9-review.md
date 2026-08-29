# Session App Owner Registration Stage 9 Review

## Scope

Issue #272 stage 9 replaces the runtime Session App registration/context
inventory with owner-defined plugin state and monomorphized registration.
Session workers retain only the selected numeric app slot and dispatch through
the published callback table; concrete protocol state remains in the owning
plugin's Data Worker state.

## Issue Evidence

- Owner-defined plugin-local concrete state and monomorphized shims replace
  runtime Session App registration, context storage, and install loops.
- `SessionAppRegistration`, `SessionAppContexts`, `session_app`, generated
  statics, runtime app inventory, and old callback/context aliases are deleted.
- Protocol state is owned and advanced by the Data Worker through `&mut`
  access, and a protocol may access only its adjacent FIFOs.

## Vendored VPP Evidence

- `third_party/vpp/src/vnet/session/application.c` binds a concrete callback
  table to the application at attach time rather than storing a generic
  runtime protocol inventory.
- `third_party/vpp/src/vnet/session/application_worker.c` keeps worker-owned
  application state and routes callbacks on the target worker.
- `third_party/vpp/src/vnet/session/application_interface.h` defines the
  callback-table shape used by Session event dispatch.

## Changes

- `hammer-service::session::protocol` owns `SessionAppVft` and the narrow
  `register_session_app(application, vft)` entry point. Each plugin submits
  its already-monomorphized static VFT directly; there is no trait, dynamic
  dispatch, or runtime state erasure around the callback table.
- `ApplicationMain` publishes the owner-defined VFT policy during ordered
  initialization and `SessionWorker` resolves the selected slot for exact-
  Session event dispatch. `SessionMain` no longer owns a Session App inventory
  and `SessionWorker` has no callback override or plugin state container.
- TLS stores `Connection` values in its own worker-indexed `ThreadOwned<Pool<_>>`
  and its VFT shims resolve and mutate that state before touching adjacent
  Session FIFOs. `Connection` lifecycle methods are inherent owner methods,
  not implementations of a service-side callback trait.
- QUIC and HTTP keep their context pools in their own plugin worker authorities
  and submit their static VFTs through the same narrow entry point. QUIC
  cleanup now relies on the existing owner transport-close/finalize path; its
  unused parallel destroy entry was removed.
- The old Session App proc-macro expansion template, service-side callback
  trait, generic callback defaults, and obsolete runtime Session App control
  errors were removed.

## Verification

- Static symbol audit finds no `SessionAppRegistration`, `SessionAppContexts`,
  `SessionAppContext`, `SessionAppCallbacks`, `session_app` proc-macro,
  generated `__SESSION_APP` symbols, SessionMain VFT inventory, worker
  callback override, runtime install loop, callback trait, or obsolete
  `RemovedApp*` surface.
- `cargo fmt --all`, `cargo fmt --all -- --check`, and `git diff --check` are
  the only checks run in this stage. Workspace compilation, tests, clippy,
  plugin loading, and the vendored-VPP final review remain deferred to issue
  #272 stage 12.

## Verdict

Stage 9 owner registration and callback-state migration is complete. Workspace
build, tests, clippy, plugin-load checks, and the vendored-VPP final review
remain the stage 12 gates for the overall issue.

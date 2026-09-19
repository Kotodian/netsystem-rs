---
name: hammer-init-function
description: Use Hammer config_function and init_function macros to parse plugin configuration, initialize owner Main state, order lifecycle dependencies, and publish it without changing hammer-runtime.
---

# Hammer Main Initialization

Use the existing component-macro lifecycle. Do not add a runtime initializer,
manual startup registry, or a new global lifecycle phase.

## Two phases

`config_function` receives exactly one by-value serde config value and may also
receive `&mut DataPlaneMain`. It returns `RuntimeResult<()>` and owns section
deserialization and validation. If a later init function needs the parsed
value, the owning plugin stores it in its own concrete state, normally an
owner-local `OnceLock<Config>`.

`init_function` runs in the topologically ordered normal-init chain. It accepts
either no argument or one `&mut DataPlaneMain`, and returns
`RuntimeResult<()>`. It initializes the plugin-owned Main, normally in a
`OnceLock<Main>`.

Use `runs_after`/`runs_before` only with real registrations in the same
lifecycle category, such as `runtime_worker_config`, `transport_main_init`, or
`session_init`. Graph materialization is not an init registration: after all
normal init and config callbacks complete, `main_loop::run` directly calls
`DataPlaneMain::init_graph_from_declarations`.

The exact owner publication mechanism remains plugin-owned. A Main initializer
must be idempotence-safe (reject duplicate installation or use the existing
single-owner cell), return typed errors, and validate all inputs before
publishing. Do not put a `WorkerBarrier` field in the Main; runtime owns the
barrier and later control updates use its existing boundary.

## Evidence

- `crates/hammer-component-macros/src/lib.rs`: `config_function` and
  `init_function` callback signatures and `config_function` section decoding.
- `crates/hammer-runtime/src/init.rs`: `ConfigFunction` and `InitFunction`
  dispatch and duplicate-call tracking.
- `crates/hammer-runtime/src/main_loop.rs`: normal init/config complete before
  graph materialization.
- `crates/hammer-plugins/transport/tcp/src/lib.rs`: owner-local config and Main
  publication.

Do not move this work into `hammer-runtime`; the plugin owns its Main type and
its initialization policy.

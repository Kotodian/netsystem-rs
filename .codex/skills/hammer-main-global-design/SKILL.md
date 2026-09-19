---
name: hammer-main-global-design
description: Design Hammer plugin Main state as owner-local process globals, using OnceLock and existing init_function macros.
---

# Plugin Main Global Design

Hammer plugin `Main` values are process-global authorities owned by the plugin.
They are not fields added to `GlobalMain`, and they are not generic runtime
registry entries.

## Storage choice

- Use `OnceLock<T>` for Main state installed exactly once. Store the Main by
  value, as current `IP4_MAIN`, `IP6_MAIN`, and `TCP_MAIN` do.
- Keep replaceable forwarding data behind the Main owner and publish it at the
  runtime barrier. Do not use a second global snapshot primitive for Main
  ownership.
- Keep mutable control-plane contributions inside the Main owner. Do not put a
  `WorkerBarrier` inside the Main and do not add a runtime-owned Main wrapper.

## Initialization with existing macros

`config_function` receives one by-value serde config, validates it, and returns
`RuntimeResult<()>`. When `init_function` needs the value later, store it in an
owner-local `OnceLock<Config>`.

`init_function` accepts either no argument or one `&mut DataPlaneMain`. It
constructs and installs the owner-local Main through the owner's existing API.
Do not introduce an `init`, `global`, installer, or access wrapper merely to
satisfy the lifecycle macro; use the concrete owner surface already present.

Construct and validate the complete Main before `MAIN.set(main)`. Never
publish a partially built Main and never use a relaxed scalar to publish its
fields.

## Consumers and updates

`graph_node(init = ...)` callbacks access the owner-local Main through its
existing concrete API and pass required state into the node constructor. They
do not construct or install Main state. Control-plane updates first prove the
existing main-thread/barrier precondition, then mutate owner state while
workers are stopped. Workers only read the global Main; they do not initialize
it.

## Evidence

- `crates/hammer-plugins/transport/tcp/src/lib.rs`: `TCP_CONFIG`, `TCP_MAIN`,
  and `init_tcp` with `#[init_function]`.
- `crates/hammer-plugins/net/ip/src/lookup.rs`: IP Main publication and graph
  node initialization callbacks.
- `crates/hammer-runtime/src/main_loop.rs`: init/config ordering before graph
  materialization.
- `crates/hammer-component-macros/src/lib.rs`: macro injection and ordering.

Do not modify `hammer-runtime` to accommodate a plugin Main. The plugin owns
the global, its initialization, its update API, and its failure semantics.

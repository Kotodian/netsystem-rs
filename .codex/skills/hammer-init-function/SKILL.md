---
name: hammer-init-function
description: Use Hammer config_function and init_function macros to parse plugin configuration, initialize owner Main state, inject dependencies, and publish it without changing hammer-runtime.
---

# Hammer Main Initialization

Use the existing component-macro lifecycle. Do not add a runtime initializer,
manual startup registry, or a new global lifecycle phase.

## Two phases

`config_function` receives the startup document and a mutable `GlobalMain`.
It owns section deserialization and validation. If a later init function needs
the parsed value, return `RuntimeResult<Arc<Config>>`; the macro stores that Arc
in `GlobalMain::registry()`. Do not construct worker-visible Main state here
unless the existing plugin deliberately uses config-time publication.

`init_function` runs in the topologically ordered init chain. It may receive
`&mut GlobalMain` and injected `Arc<T>` config values. The plugin Main exposes
`init(...)` and `global()`; `init` installs a concrete `Main` in the
plugin-owned `OnceLock<Main>`, and `global` returns `&'static Main`.
Use `runs_after`/`runs_before` for dependencies such as
`runtime_worker_config`, `session_init`, and `install_packet_graph`.

```rust
#[config_function(name = "ip_config", section = "network", early = true)]
fn configure_ip(config: NetworkIpConfig)
    -> RuntimeResult<Arc<NetworkIpConfig>>
{
    config.validate()?;
    Ok(Arc::new(config))
}

#[init_function(
    name = "ip_init",
    runs_after = ["runtime_worker_config"],
    runs_before = ["install_packet_graph"],
)]
fn init_ip(
    engine: &mut GlobalMain,
    config: Arc<NetworkIpConfig>,
) -> RuntimeResult<()> {
    IpMain::init(config.as_ref(), engine)
}
```

The exact owner publication mechanism remains plugin-owned. A Main initializer
must be idempotence-safe (reject duplicate installation or use the existing
single-owner cell), return typed errors, and validate all inputs before
publishing. Do not put a `WorkerBarrier` field in the Main; runtime owns the
barrier and later control updates use its existing boundary.

## Evidence

- `crates/hammer-component-macros/src/lib.rs`: `config_function` and
  `init_function` accept `&mut GlobalMain`, inject `Arc<T>`, and topologically
  order callbacks.
- `crates/hammer-runtime/src/init.rs`: `ConfigFunction` and `InitFunction`
  dispatch and duplicate-call tracking.
- `crates/hammer-plugins/transport/tcp/src/lib.rs`: config returns an Arc and
  `init_tcp` constructs the Main.

Do not move this work into `hammer-runtime`; the plugin owns its Main type and
its initialization policy.

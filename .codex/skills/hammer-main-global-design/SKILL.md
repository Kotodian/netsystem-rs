---
name: hammer-main-global-design
description: Design Hammer plugin Main state as owner-local process globals, using OnceLock and existing init_function macros.
---

# Plugin Main Global Design

Hammer plugin `Main` values are process-global authorities owned by the plugin.
They are not fields added to `GlobalMain`, and they are not generic runtime
registry entries.

## Storage choice

- Use `OnceLock<T>` for Main state installed exactly once (`IP_MAIN`,
  `TCP_MAIN`, `HTTP_MAIN`, `QUIC_MAIN`, `UDP_MAIN`). Store the Main by value.
- Keep replaceable forwarding data behind the Main owner and publish it at the
  runtime barrier. Do not use a second global snapshot primitive for Main
  ownership.
- Keep mutable control-plane contributions inside the Main owner. Do not put a
  `WorkerBarrier` inside the Main and do not add a runtime-owned Main wrapper.

```rust
pub static IP_MAIN: OnceLock<IpMain> = OnceLock::new();
pub static TCP_MAIN: OnceLock<TcpMain> = OnceLock::new();
```

## Initialization with existing macros

`config_function` deserializes and validates the plugin section. If `init_function`
needs the parsed config, return `RuntimeResult<Arc<Config>>`; the macro stores
that config Arc in the existing `GlobalMain::registry()`.

`init_function` calls the Main `init` entry point. The Main `init` function
constructs and installs the owner-local global; the Main `global` function
returns the published `&'static Main`. The macro may inject `Arc<T>` config
values, but the Main itself is stored by value in `OnceLock<Main>`.

```rust
#[config_function(name = "tcp_config", section = "plugin.tcp", early = true)]
fn configure_tcp(config: TcpPluginConfig)
    -> RuntimeResult<Arc<TcpPluginConfig>>
{
    config.validate()?;
    Ok(Arc::new(config))
}

#[init_function(
    name = "tcp_init",
    runs_after = ["transport_main_init", "session_init"],
    runs_before = ["install_packet_graph"],
)]
fn init_tcp(
    engine: &mut GlobalMain,
    config: Arc<TcpPluginConfig>,
) -> RuntimeResult<()> {
    TcpMain::init(config.as_ref(), engine)
}
```

The required public shape is:

```rust
impl TcpMain {
    pub fn init(config: &TcpPluginConfig, engine: &mut GlobalMain)
        -> RuntimeResult<()>;
    pub fn global() -> RuntimeResult<&'static Self>;
}
```

Construct and validate the complete Main before `MAIN.set(main)`. Never
publish a partially built Main and never use a relaxed scalar to publish its
fields.

## Consumers and updates

`graph_node(init = ...)` callbacks call `Main::global()` and pass concrete
handles into the node constructor. They do not construct or install Main
state. Control-plane updates first prove the existing main-thread/barrier
precondition, then mutate owner state while workers are stopped. Workers only
read the global Main; they do not initialize it.

## Evidence

- `crates/hammer-plugins/transport/tcp/src/lib.rs`: `TCP_MAIN`, config Arc,
  and `init_tcp` with `#[init_function]`.
- `crates/hammer-plugins/ip/src/lookup/mod.rs`: `IP_MAIN` and IP config/init
  callbacks.
- `crates/hammer-runtime/src/global_main.rs`: `GlobalMain` owns runtime
  control/barrier state, not plugin Main values.
- `crates/hammer-component-macros/src/lib.rs`: macro injection and ordering.

Do not modify `hammer-runtime` to accommodate a plugin Main. The plugin owns
the global, its initialization, its update API, and its failure semantics.

# Hammer statically linked plugin system

Date: 2026-07-14  
Issue: #85  
Parent: #72

## Summary

Hammer uses a VPP-shaped **statically linked** plugin model with stronger Rust compile-time controls:

1. **Cargo `plugin-<name>` features** decide what enters the binary catalog.
2. **`#[plugin(name = "...")]`** lexical ownership tags every lifecycle/graph contribution.
3. **TOML `plugins = [...]`** selects a loaded subset of the **compiled** catalog at startup.
4. Required **`Arc<T>` DI** remains; optional injection is not used for feature activation.

No `dlopen`, unload, `Plugin` trait, `PluginId`, activation token, or per-plugin graph slice.

## Compile-time

### Features

| Feature | Plugin name | Typical deps |
|---|---|---|
| `plugin-device` | `device` | — |
| `plugin-interface` | `interface` | `plugin-device` |
| `plugin-tun` | `tun` | `plugin-device`, `plugin-interface` |
| `plugin-ip` | `ip` | `plugin-interface` |
| `plugin-transport` | `transport` | `plugin-ip` |
| `plugin-session` | `session` | `plugin-transport` |
| `plugin-tcp` | `tcp` | `plugin-session`, `plugin-transport` |
| `plugin-udp` | `udp` | `plugin-transport` |

Daemon (`hammer`) enables the product set explicitly. Uncompiled plugins never appear in `PLUGIN_REGISTRATIONS` or lifecycle slices.

### `#[plugin]`

- Applied to an external module; children inherit ownership.
- Emits `PluginRegistration { name, load_after }`.
- Rejects nested plugins; restores poison scope; preserves `cfg` / `cfg_attr`.
- Service declarations outside a plugin module are compile errors (trybuild).

### DI

- Init/config adapters take required `Arc<T>` and may publish `Arc<T>`.
- Feature deps ensure provider **types** exist when a consumer plugin is compiled.
- Startup still fails if a loaded plugin never published a required `Arc` (ordering / empty config).

## Runtime

1. Validate `Config.plugins`: unique, each ∈ compiled catalog, `load_after` acyclic and only references loaded plugins (never auto-load).
2. Filter every phase (`EARLY_CONFIG`, `CONFIG`, `INIT`, `GRAPH_NODES`, `WORKER_INIT`, `MAIN_LOOP_*`) by loaded set before DAG / `init_graph`.
3. One global `GRAPH_NODES`; one main-thread `init_graph`; workers clone topology only.
4. Early-config decodes `[plugin.<name>]` via `Config::plugin_config::<T>` into `RuntimeRegistry`.
5. Config may create zero instances for a loaded plugin with an empty section.

## Catalogs

- `PLUGIN_REGISTRATIONS: [PluginRegistration]`
- `GRAPH_NODES: [NodeEntry]` (replaces `SERVICE_GRAPH_NODES` / `TUN_GRAPH_NODES` / `TCP_WORKER_GRAPH_NODES`)
- Existing init slices gain `plugin: Option<&'static str>` (`None` = runtime builtin)
- `MAIN_LOOP_CALLBACKS` becomes owner-bearing records (not bare `fn()`)

## Non-goals

- Dynamic loading / hot reload
- `Plugin` trait / activation objects
- Config alone enabling an uncompiled plugin
- Typestate marker types (`TunPlugin`) unless a second consumer needs Interface approval

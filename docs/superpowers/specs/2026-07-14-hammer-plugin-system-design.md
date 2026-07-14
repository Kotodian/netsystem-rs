# Hammer dynamic plugin system

Date: 2026-07-14
Issue: #96
Supersedes: #85 static plugin assembly

## Ownership

Hammer follows VPP's split between library registration and main-thread plugin
authority:

- `hammer-core` owns canonical Frame, Buffer, Index, and graph identity types.
- `hammer-runtime::PluginRegistration` is the one registration block exported
  by each dynamic library as `hammer_plugin_registration`.
- `hammer-runtime::PluginMain` corresponds to VPP's `plugin_main_t`. It owns the
  plugin path, every library handle, dependency order, and all imported
  executable contributions.
- `Engine` and every worker share one `Arc<PluginMain>`. Runtime graph state and
  Process Node futures are destroyed before the final library handle.

There is no Registrar, parallel data-plane type, second inventory export, weak
registration symbol, or static/dynamic dual mode.

## Loadable libraries

The loadable set is `tun`, `ip`, `tcp`, and `udp`:

- `tun` is a device-driver plugin.
- `ip`, `tcp`, and `udp` own their protocol graph and lifecycle contributions.
- `device`, `interface`, `transport`, and `session` remain shared
  `hammer-service` infrastructure and are not plugin roots.
- TCP uses shared session infrastructure. UDP remains transport-only.

Linux uses `libhammer_plugin_<name>.so`; macOS uses
`libhammer_plugin_<name>.dylib`. `HAMMER_PLUGIN_DIR` selects the directory; the
default is the daemon executable directory.

## Registration and startup

`declare_plugin!` generates the library's single strong registration export.
The returned registration refers directly to that DSO's private `linkme`
slices for init, config, worker init, main-loop hooks, Graph Nodes, Node
Function variants, and Process Nodes.

Startup proceeds on the main thread:

1. Read configured root names from `plugins = [...]`.
2. `PluginMain` opens each root and transitive `load_after` dependency, checks
   the exported name and host SemVer, and rejects duplicates or cycles.
3. For every phase, keep host records with `plugin = None` and import only DSO
   records whose `plugin` equals that library's registration name. Builtins in
   the DSO's private runtime copy are never imported.
4. Run config/init ordering, build the graph once on the main thread, and clone
   the resolved graph when workers are forked.
5. Start Process Nodes on the main thread's Tokio `LocalSet`.

An empty root list opens no libraries and creates no plugin instances. Missing
libraries fail with the plugin name and resolved platform path. Plugin TOML
remains owned by `[plugin.<name>]` and is parsed by the plugin's lifecycle code.

## Safety boundary

- The registration symbol is called only while its `libloading::Library` is
  held by `PluginMain`.
- Imported slices and function pointers are used only by engines sharing that
  same `PluginMain`.
- Init calls are caught at the runtime dispatch boundary.
- Normal shutdown joins workers and Process Nodes before dropping plugin
  handles.
- Runtime disable/unload must drain and rebuild the graph before replacing the
  shared `PluginMain`; it is tracked separately from startup loading.

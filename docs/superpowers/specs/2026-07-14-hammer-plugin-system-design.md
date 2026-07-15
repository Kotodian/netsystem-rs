# Hammer dynamic plugin system

Date: 2026-07-14
Issue: #96
Supersedes: #85 static plugin assembly

## VPP reference and ownership

Hammer follows VPP's registration split as implemented in the vendored source:

- `third_party/vpp/src/vlib/init.h` and `node.h` attach builtin and plugin
  registrations to the single `vlib_global_main` from load constructors and
  unlink them from unload destructors.
- `third_party/vpp/src/vlib/main.h` owns that process-wide runtime authority.
- `third_party/vpp/src/vlib/unix/plugin.h` and `plugin.c` keep plugin metadata,
  load order, and DSO handles in `plugin_main_t`; they do not merge a second
  service-owned executable inventory.

The Hammer ownership mapping is therefore:

- `hammer-core` owns canonical Frame, Buffer, Index, and graph identity types.
- `hammer-runtime` owns the one process-wide executable registration authority.
- Every registration-bearing link image owns private `linkme` slices and one
  hidden `RegistrationImage`. Its load constructor publishes those slices to
  the runtime authority. Runtime builtins, `hammer-service`, and plugin DSOs use
  exactly the same mechanism.
- `PluginRegistration` exports metadata only: name, version requirement, and
  `load_after` dependencies.
- `PluginMain` owns the plugin path, dependency order, and DSO handles only.
  It does not own, filter, or merge executable registrations.
- `Engine` and workers retain the same `Arc<PluginMain>`, so dynamically
  installed function pointers cannot outlive their provider handles.

There is no service registration export, Registrar, second runtime authority,
weak registration symbol, or static/dynamic merge path.

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

`__declare_registration_image!` is a documentation-hidden implementation seam.
It declares the current link image's private slices, one `RegistrationImage`,
and its constructor/destructor pair. Registration proc macros always emit into
those image-local slices; they do not accept a plugin owner argument.

Startup proceeds on the main thread:

1. Process-start constructors publish the runtime and service images.
2. Read configured plugin roots from `plugins = [...]`.
3. `PluginMain` calls `dlopen`. The DSO constructor publishes its image before
   `dlopen` returns.
4. Read the metadata export, validate its name and host SemVer, then load each
   transitive `load_after` dependency in dependency order.
5. Lifecycle, graph, Node Function, and Process Node consumers read the one
   runtime authority. They do not consult `PluginMain` for executable records.
6. Build the graph once on the main thread, clone the resolved graph for Data
   Workers, and start Process Nodes on the main thread's Tokio `LocalSet`.

An empty root list opens no plugin DSOs. Missing libraries report the requested
name and resolved platform path. Plugin TOML remains under `[plugin.<name>]` and
is parsed by that plugin's lifecycle code.

## Failure rollback and lifetime

Loading is transactional before activation. If metadata validation or a
dependency load fails, the partially built `PluginMain` drops its DSO handles.
Each unload destructor removes its `RegistrationImage` before the image is
unmapped, so a later runtime collection cannot observe stale function pointers.

After successful activation Hammer exposes no generic unload, replacement, or
inventory-filtering operation. Workers and Process Nodes stop and graph state
is dropped before the retained `PluginMain` handles are released during normal
process shutdown.

## Layer isolation contract

- Registration-bearing crates may invoke the hidden image declaration and
  write only to their generated image-local slices.
- They must not mutate the runtime list directly, export executable slices, or
  route records through `PluginMain` or `hammer-service`.
- Runtime lifecycle/graph/process consumers may collect from the authority;
  they must not infer registration ownership from plugin metadata.
- `PluginMain` may call the metadata symbol and retain library handles; it must
  not install executable records.
- No new supported public registration API is introduced. The sole cross-crate
  seam is the documentation-hidden `RegistrationImage` declaration/link/unlink
  mechanism required by constructor code generated in dependent images.

## Safety boundary

- The runtime list is serialized while images link, unlink, or are collected.
- An image initializes immutable inventories before publishing its list node.
- An unload destructor unlinks while its code and static data are still mapped.
- Plugin loading and activation remain main-thread lifecycle operations; no
  consumer may race activation with a failed-load handle rollback.
- Workspace linking uses the shared Hammer dynamic libraries. A plugin that
  embeds another Hammer runtime would create a second authority and is invalid.
- Runtime graph state and Process Node futures are destroyed before the final
  plugin handle.

Focused verification:

```bash
cargo build --workspace
cargo test --workspace
cargo test -p hammer-runtime --test plugin_loader
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
```

# Task #39 Report: Delete hammer-adapter and old adapter contracts

## What I implemented

- Added `crates/hammer-runtime/tests/adapter_deletion_guard.rs` to reject:
  - `crates/hammer-adapter` as a workspace member.
  - active `hammer-adapter` crate directory presence.
  - `hammer-adapter` manifest dependencies.
  - current source references to `hammer_adapter`.
- Removed `crates/hammer-adapter` from the workspace, manifests, `Cargo.lock`, and filesystem.
- Deleted old adapter-owned contracts and OS-facing surfaces:
  - inbound/outbound/platform/certificate/connection/network/service/wakeup contracts by deleting the crate.
  - `hammer_runtime::adapter` compatibility module.
  - `hammer_service::adapter` compatibility re-export.
  - `RuntimePlatform` and `SocketProtector`.
  - TCP connector platform/protector hook; it now creates and connects a `TcpSocket` directly.
- Moved the small component metadata surface still emitted by `#[hammer_component]` into `hammer-runtime`:
  - `ComponentMeta`
  - `ComponentMetadata`
  - `ComponentMetricsMeta`
- Updated `hammer-component-macros` to emit `::hammer_runtime::...` component metadata paths.
- Updated current project docs/comments and source guards that treated `hammer-adapter` as active.

## What I tested and results

- `cargo test -p hammer-runtime --test adapter_deletion_guard` - passed, 2 tests.
- `cargo test -p hammer-component-macros` - passed, 5 unit tests; doctests ignored as before.
- `cargo test -p hammer-runtime` - passed.
- `cargo test -p hammer-service` - passed.
- `cargo test -p hammer-app` - passed.
- `cargo test -p hammer-ipc` - passed.
- `cargo test --workspace --no-run` - passed.
- `cargo fmt --all -- --check` - passed after running `cargo fmt --all`.
- Source/active-surface scan:
  - `rg -n "hammer-adapter|hammer_adapter|RuntimePlatform|SocketProtector|PlatformInterface|pub mod adapter|hammer_runtime::adapter" Cargo.toml Cargo.lock README.md AGENTS.md CONTEXT.md docs/agents crates ...`
  - Result: no active-surface matches outside historical guard-test string literals excluded from the scan.
- Directory check:
  - `test -d crates/hammer-adapter && printf 'adapter_dir_exists\n' || printf 'adapter_dir_missing\n'`
  - Result: `adapter_dir_missing`.

## TDD Evidence

### RED

Command:

```bash
cargo test -p hammer-runtime --test adapter_deletion_guard
```

Output summary:

- Failed as expected with 0/2 passing.
- `hammer_adapter_is_not_an_active_workspace_crate_or_dependency` reported:
  - workspace members still included `crates/hammer-adapter`
  - `crates/hammer-adapter` still existed
  - `crates/hammer-adapter`, `hammer-app`, `hammer-runtime`, and `hammer-service` manifests still declared `hammer-adapter`
- `current_source_surfaces_do_not_reference_hammer_adapter` reported:
  - `hammer-component-macros/src/lib.rs` emitted `::hammer_adapter`
  - `hammer-runtime/src/lib.rs` re-exported `hammer_adapter`
  - runtime TCP/socket-protector sources imported `PlatformInterface`

### GREEN

Command:

```bash
cargo test -p hammer-runtime --test adapter_deletion_guard
```

Output summary:

- Passed with 2/2 tests.
- Confirmed no active workspace member/dependency/directory/source references to `hammer-adapter` or `hammer_adapter` under the new guard.

## Files changed/deleted

- Modified:
  - `AGENTS.md`
  - `Cargo.lock`
  - `Cargo.toml`
  - `README.md`
  - `crates/hammer-app/Cargo.toml`
  - `crates/hammer-component-macros/src/lib.rs`
  - `crates/hammer-core/src/config/worker.rs`
  - `crates/hammer-runtime/Cargo.toml`
  - `crates/hammer-runtime/src/lib.rs`
  - `crates/hammer-runtime/src/protocol/server_tcp.rs`
  - `crates/hammer-service/Cargo.toml`
  - `crates/hammer-service/src/lib.rs`
  - `crates/hammer-service/tests/frame_owner_cleanup_hot_path.rs`
- Added:
  - `crates/hammer-runtime/src/component.rs`
  - `crates/hammer-runtime/tests/adapter_deletion_guard.rs`
- Deleted:
  - `crates/hammer-adapter/Cargo.toml`
  - `crates/hammer-adapter/src/certificate.rs`
  - `crates/hammer-adapter/src/component.rs`
  - `crates/hammer-adapter/src/connection.rs`
  - `crates/hammer-adapter/src/lib.rs`
  - `crates/hammer-adapter/src/network.rs`
  - `crates/hammer-adapter/src/platform.rs`
  - `crates/hammer-adapter/src/service.rs`
  - `crates/hammer-adapter/src/wakeup.rs`
  - `crates/hammer-adapter/tests/buffer_inline_layout.rs`
  - `crates/hammer-adapter/tests/buffer_layout.rs`
  - `crates/hammer-runtime/src/macros.rs`
  - `crates/hammer-runtime/src/socket_protector.rs`

## Self-review findings

- The new guard fails for both workspace/manifest/dir state and current-source `hammer_adapter` references, then passes after deletion.
- No compatibility crate, compatibility `adapter` module, `RuntimePlatform`, `SocketProtector`, `PlatformInterface`, or replacement OS-facing abstraction remains in active sources.
- `#[hammer_component]` metadata moved to runtime ownership without creating a new generic adapter namespace.
- `hammer-service` protocol code was not redesigned; touched service code is limited to removing compatibility exports and updating a guard scan root.
- `Cargo.lock` no longer contains a `hammer-adapter` package or dependencies on it.

## Concerns

- Verification commands still emit existing warnings unrelated to this task, especially deprecated `FlatHashTable`/`FlatHashKey` warnings and some pre-existing unused/private-interface warnings.
- Historical guard tests still contain `hammer_adapter` string literals by design; the new deletion guard excludes those guard tests while scanning current source surfaces.

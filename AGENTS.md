# Repository Guidelines

## Project Structure & Module Organization

This Rust workspace implements an iOS NetworkExtension VPN engine. Crates live in `crates/`:

- `hammer-core`: shared config schema, errors, lifecycle, metrics, logs, and network primitives.
- `hammer-adapter`: cross-crate traits and platform contracts.
- `hammer-component-macros`: proc macros for runtime registration.
- `hammer-runtime`: protocols, routing, DNS, TUN handling, and service orchestration.
- `hammer-ffi`: UniFFI-facing service API and `src/hammer.udl`.
- `hammer-uniffi-bindgen`: binding generation helper binary.

Integration tests live in crate-local `tests/` directories, for example `crates/hammer-runtime/tests/`. Patched dependencies live under `third_party/`. Generated iOS output goes to `dist/ios/`.

## Build, Test, and Development Commands

- `cargo build --workspace`: build all workspace crates.
- `cargo test --workspace`: run all Rust tests.
- `cargo test -p hammer-runtime`: run tests for one crate.
- `cargo fmt --all`: format the workspace with rustfmt.
- `cargo clippy --workspace --all-targets`: run lint checks.
- `make ios-lib` or `make xcframework`: build the iOS `Hammer.xcframework` and Swift glue.
- `make clean-ios-lib`: remove generated iOS artifacts from `dist/ios/`.

For profiling-friendly iOS builds, use `PROFILE=release-perf ./scripts/build-xcframework.sh`.

## Coding Style & Naming Conventions

Use Rust 2024 conventions and rustfmt defaults: 4-space indentation, `snake_case` for modules/functions, `PascalCase` for types and traits, and `SCREAMING_SNAKE_CASE` for constants. Keep dependency direction consistent: `hammer-ffi -> hammer-runtime -> {hammer-adapter, hammer-core}` and `hammer-adapter -> hammer-core`. Group modules by protocol or subsystem, matching paths such as `src/protocol/hysteria2/` and `src/config/`.

## Testing Guidelines

Add integration tests near the crate whose behavior changes. Use descriptive file names like `dns_runtime.rs`, `service_lifecycle.rs`, or `config_parse.rs`. Prefer focused tests for config parsing, lifecycle behavior, routing, and protocol edge cases. Run `cargo test --workspace` before a PR; use `cargo test -p <crate>` while iterating.

## Commit & Pull Request Guidelines

Recent commits use scoped messages such as `hammer-runtime(Feat): add proxy inbounds` and `wireguard(Feat): scaffold amneziawg 2.0 feature`. Follow `<scope>(<Type>): <imperative summary>`, with types like `Feat`, `Fix`, `Refactor`, `Debug`, or `docs`.

PRs should include a behavior summary, affected crates, test commands run, and any iOS packaging impact. Link related issues when available. Include generated artifact notes only when Swift/iOS output changes.

## Security & Configuration Tips

Do not commit real VPN credentials, server addresses, certificates, or generated framework output. Keep example TOML values synthetic, and document feature flags when enabling optional protocols such as WireGuard or DoH.

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

## VPP Refactor Principles

When working on VPP-related refactors in this repository:
- Always research and reference VPP for dataplane, session, transport, and TCP design decisions before proposing or changing architecture.
- Use data structures from the `hammer-infra` crate by default. If `hammer-infra` lacks a required generic API, add the API there instead of falling back to `std` or creating local one-off utilities.
- Reuse existing APIs before adding new wrappers, helpers, or types. Add new API surface only when reuse is not technically viable.
- Utility or tool types must remain generic and must not contain business concepts. Business state names must describe the domain state directly; do not use names such as `Cursor`, `Helper`, or `Util` for business records.
- Do not introduce underscore-prefixed variable names such as `_value`. If a parameter or pattern slot is intentionally unused, use the bare `_` pattern. If a local binding is unused, delete it and the work that produced it.
- Enforce Rust architectural boundaries with visibility, traits, and narrow re-exports instead of comments or convention.
- Non-trivial Rust designs must document the layer isolation contract: what each layer may call, what it must not call, which APIs cross the boundary, and which commands verify the boundary.

## Testing Guidelines

Add integration tests near the crate whose behavior changes. Use descriptive file names like `dns_runtime.rs`, `service_lifecycle.rs`, or `config_parse.rs`. Prefer focused tests for config parsing, lifecycle behavior, routing, and protocol edge cases. Run `cargo test --workspace` before a PR; use `cargo test -p <crate>` while iterating.

## Commit & Pull Request Guidelines

Recent commits use scoped messages such as `hammer-runtime(Feat): add proxy inbounds` and `wireguard(Feat): scaffold amneziawg 2.0 feature`. Follow `<scope>(<Type>): <imperative summary>`, with types like `Feat`, `Fix`, `Refactor`, `Debug`, or `docs`.

PRs should include a behavior summary, affected crates, test commands run, and any iOS packaging impact. Link related issues when available. Include generated artifact notes only when Swift/iOS output changes.

## Security & Configuration Tips

Do not commit real VPN credentials, server addresses, certificates, or generated framework output. Keep example TOML values synthetic, and document feature flags when enabling optional protocols such as WireGuard or DoH.

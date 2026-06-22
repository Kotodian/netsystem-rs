# Repository Guidelines

## Project Structure & Module Organization

This Rust workspace implements an iOS NetworkExtension VPN engine. Crates live in `crates/`:

- `hammer-core`: shared config schema, errors, lifecycle, metrics, logs, and network primitives.
- `hammer-adapter`: cross-crate traits and platform contracts.
- `hammer-component-macros`: proc macros for runtime registration.
- `hammer-runtime`: routing, TUN handling, endpoint/outbound runtime, and service orchestration.
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

Use Rust 2024 conventions and rustfmt defaults: 4-space indentation, `snake_case` for modules/functions, `PascalCase` for types and traits, and `SCREAMING_SNAKE_CASE` for constants. Keep dependency direction consistent: `hammer-ffi -> hammer-runtime -> {hammer-adapter, hammer-core}` and `hammer-adapter -> hammer-core`. Group modules by protocol or subsystem, matching paths such as `src/transport/tcp/` and `src/config/`.

## VPP Refactor Principles

When working on VPP-related refactors in this repository:
- Always research and reference VPP for dataplane, session, transport, and TCP design decisions before proposing or changing architecture.
- Treat VPP as a semantic and ownership reference, not as a 1:1 API, data-structure, or naming template. Hammer's app/session boundary is io_uring-style (`AppRingHandle`, SQE, CQE), so do not replace it with VPP `svm_fifo` shapes or names.
- Use data structures from the `hammer-infra` crate by default. If `hammer-infra` lacks a required generic API, add the API there instead of falling back to `std` or creating local one-off utilities.
- Reuse existing APIs before adding new wrappers, helpers, or types. Add new API surface only when reuse is not technically viable. When a missing capability is shared by multiple use cases, add one generic primitive at the owning layer instead of adding per-feature APIs. Any new type or API in non-trivial VPP/TCP work must state the final result, explain why existing surfaces cannot satisfy the need, and receive explicit user approval before implementation.
- Utility or tool types must remain generic and must not contain business concepts. Business state names must describe the domain state directly; do not use names such as `Cursor`, `Helper`, or `Util` for business records.
- Do not introduce underscore-prefixed variable names such as `_value`. If a parameter or pattern slot is intentionally unused, use the bare `_` pattern. If a local binding is unused, delete it and the work that produced it.
- Enforce Rust architectural boundaries with visibility, traits, and narrow re-exports instead of comments or convention.
- Non-trivial Rust designs must document the layer isolation contract: what each layer may call, what it must not call, which APIs cross the boundary, and which commands verify the boundary.

### Hammer/VPP TCP Standards

For TCP, session, dataplane buffer, and recovery work:
- Session runtime owns node scheduling. Congestion control must not schedule nodes, and current code must not introduce a congestion-control sibling/node unless explicitly approved.
- Congestion control remains transport-agnostic and is owned through the TCP connection generic (`TcpConnection<S, C>`). It is updated through typed TCP events; it must not special-case ownership of TCP session/runtime state.
- Session owns TX byte retention and the app/session copy boundary. TCP owns sequence, ACK, loss, recovery, and timer decisions. TCP output owns TCP header prepending. Session/runtime must not know TCP header fields or TCP segment internals.
- App-to-session data may be copied because the app boundary is designed for future cross-process operation. TCP must not retain app-ring descriptors or private payload copies for recovery; retransmit packetizes from session-owned TX FIFO bytes.
- The app/session boundary is the only place where payload bytes may be copied into session ownership. After bytes enter the session TX FIFO, session/TCP/recovery/output/buffer/runtime/congestion-control code must not create intermediate payload `Vec`s or private payload copies; pass FIFO offsets, `BufferIndex`, buffer-chain links, timer tokens, or typed TCP facts instead.
- Normal TCP TX must follow VPP's session path: session keeps TX bytes in a FIFO, session runtime prepares dataplane buffers from session-owned payload storage, TCP transport/output prepends headers, and ACK cleanup drops bytes from the session FIFO. Do not redesign normal TX around per-feature payload selection helpers or temporary payload copies.
- For no-copy buffer sharing outside the normal TCP TX path, follow VPP's buffer semantics: a buffer chain is represented by buffer-header state (`current_data`, `current_length`, `NEXT_PRESENT`, `next_buffer`, total chain length), and sharing is represented by `attach_clone`/refcount behavior. Do not introduce feature-specific buffer ownership APIs.
- Buffer and runtime APIs must remain transport-neutral. Do not add TCP-specific buffer/runtime APIs, TCP-specific headroom allocation, or runtime TCP copy/rebuild helpers. Generic headroom is user/dataplane buffer policy, not TCP-owned state.
- `TcpSegment` is the TCP output intent and must be constructed through its constructor or an approved replacement constructor. It is consumed by the TCP output node to prepend headers; it is not a recovery record, receive ordering record, or externally hand-built struct.
- Timer expiry must dispatch the exact timer token/kind supplied by runtime. Do not scan all `TcpConnectionTimerKind` values to discover expired work.
- Do not add or reintroduce TCP-specific runtime chain-copy APIs, extra TCP output carriers, buffer-chain owner wrappers, new single-buffer owner wrappers, or builder-style TCP node constructors for required dependencies such as session queues.
- Recovery accounting records, if needed, must be private to the recovery module or narrowly visible inside TCP. Do not expose public construction of sent-segment records or hide the same design behind a rename.
- Plans for VPP/TCP work must include an approval section for every proposed new type/API and must call out any cleanup of existing bad surfaces rather than leaving them in place.

## Testing Guidelines

Add integration tests near the crate whose behavior changes. Use descriptive file names like `service_lifecycle.rs`, `config_parse.rs`, or `tcp_output.rs`. Prefer focused tests for config parsing, lifecycle behavior, routing, and protocol edge cases. Run `cargo test --workspace` before a PR; use `cargo test -p <crate>` while iterating.

## Commit & Pull Request Guidelines

Recent commits use scoped messages such as `hammer-runtime(Feat): add proxy inbounds` and `wireguard(Feat): scaffold amneziawg 2.0 feature`. Follow `<scope>(<Type>): <imperative summary>`, with types like `Feat`, `Fix`, `Refactor`, `Debug`, or `docs`.

PRs should include a behavior summary, affected crates, test commands run, and any iOS packaging impact. Link related issues when available. Include generated artifact notes only when Swift/iOS output changes.

## Security & Configuration Tips

Do not commit real VPN credentials, server addresses, certificates, or generated framework output. Keep example TOML values synthetic, and document feature flags when enabling optional protocols such as WireGuard.

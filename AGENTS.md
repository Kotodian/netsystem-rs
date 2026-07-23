# Repository Guidelines

## Project Overview

Hammer is a VPP-style high-performance network data plane framework written in Rust. It is a standalone packet-processing framework modeled on VPP's node/graph/session architecture, with daemon and CLI surfaces for local operation.

The framework centers on a **packet graph runtime**: data-plane work is organized into graph nodes processing frames of buffers, worker-owned state, lock-free hot paths, and VPP-style barrier synchronization between control and data planes.

## Agent skills

### Issue tracker

Issues and PRDs are tracked in GitHub Issues for `Kotodian/hammer-ios-rs`; external PRs are not a triage request surface by default. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the default role labels: `bug`, `enhancement`, `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repo: read root `CONTEXT.md` and relevant ADRs under `docs/adr/`. See `docs/agents/domain.md`.

## Project Structure & Module Organization

Workspace root: `crates/`. Dependency direction is strictly one-way to avoid cycles: `hammer → {hammer-runtime, hammer-service, hammer-ipc, hammer-core, hammer-component-macros}`, `hammer-app → {hammer-runtime, hammer-core, hammer-infra}`, `hammer-service → {hammer-runtime, hammer-core, hammer-infra, hammer-component-macros}`, `hammer-ipc → {hammer-core, hammer-runtime}`, `hammer-runtime → {hammer-core, hammer-component-macros}`, `hammer-infra → (external only)`, `hammer-core → hammer-infra`.

| crate | role |
|---|---|
| `hammer-infra` | Bottom-layer infrastructure — the process-global fixed-capacity Main Heap authority plus lock-free data structures and memory primitives (cache-aligned). FIFO with OOO delivery, `Pool<T>` with generation counters, `TimerWheel1t2w2048sl`, VPP-style Bihash, `RbTree`, `Segment` (Local heap / Svm shared-memory mmap), internet checksum, SIMD primitives, ring buffers. Analogous to VPP's `vppinfra`. |
| `hammer-core` | Minimal cross-DSO packet-graph ABI — Node/Frame/Buffer/Index/Next primitives and errors intrinsic to them. |
| `hammer-component-macros` | Proc macros: `#[graph_node]`, `#[init_function]`, `#[worker_init_function]` for declarative packet-graph node registration via `linkme` distributed slices. |
| `hammer-runtime` | Runtime engine — worker thread spawning, engine main loop with VPP fixed-schedule step order, barrier synchronization (`control_call_with_barrier`), `RuntimeRegistry` (typed service registry), session/app handle types. |
| `hammer-service` | Protocol-neutral network infrastructure — interface, session, device, and feature-arc contracts used by independent plugins. |
| `hammer-app` | Application-plane interface — app/session boundary for local and cross-process (shared-memory) sessions. `AppClient` (Unix socket + SCM_RIGHTS) returns `AppSession<Svm>`; independent apps use its async methods. Echo helpers for testing. |
| `hammer-ipc` | Daemon ↔ CLI IPC protocol — length-prefixed frame format, request/reply message types, `#[ipc_handler]` registration via `linkme`, sync `IpcClient`. |
| `hammer` | Daemon binary (analogous to VPP's `vpp`). Loads TOML config, initializes runtime engine + worker graph, binds IPC TCP socket (default `127.0.0.1:7299`, overridable via `HAMMER_IPC_ADDR`), runs the data-plane main loop. |
| `hammerctl` | CLI control tool (analogous to `vppctl`). Subcommands: `Pause`, `Wake`, `ResetNetwork`, `Shutdown`, `Status`, `Send` (raw handler dispatch). |

Patched dependencies live under `third_party/`. Design docs live in `docs/superpowers/` (`specs/` for architecture specs, `plans/` for dated implementation plans, `sdd/` for task execution tracking).

## Build, Test, and Development Commands

```bash
cargo build --workspace              # build all crates
cargo build --workspace --release    # release build (thin LTO)
cargo test --workspace               # run all Rust tests
cargo test -p hammer-runtime         # run tests for one crate
cargo fmt --all                      # format with rustfmt
cargo fmt --all -- --check           # check formatting without writing
cargo clippy --workspace --all-targets  # lint

make build        # = cargo build --workspace
make build-release
make run          # cargo run -p hammer -- -c startup.toml
make ctl          # cargo run -p hammerctl --
make test         # cargo test --workspace
make clippy
make fmt
make clean
```

## Coding Style & Naming Conventions

Use Rust 2024 conventions and rustfmt defaults: 4-space indentation, `snake_case` for modules/functions, `PascalCase` for types and traits, and `SCREAMING_SNAKE_CASE` for constants. Keep dependency direction consistent (see graph above). Group modules by protocol or subsystem, matching paths such as `src/transport/tcp/` and `src/config/`.

### Rust-specific rules

- Do **not** introduce underscore-prefixed variable names such as `_value`. If a parameter or pattern slot is intentionally unused, use the bare `_` pattern. If a local binding is unused, delete it and the work that produced it.
- Enforce architectural boundaries with visibility, traits, and narrow re-exports instead of comments or convention.
- Non-trivial designs must document the layer isolation contract: what each layer may call, what it must not call, which APIs cross the boundary, and which commands verify the boundary.

## VPP Refactor Principles

When working on VPP-related refactors in this repository:
- Always research and reference VPP for dataplane, session, transport, and TCP design decisions before proposing or changing architecture.
- Use the vendored VPP source under `third_party/vpp/` as the first and default VPP reference. Search it locally before consulting any network source; use external VPP sources only when the required code is absent from the vendored tree.
- Treat VPP as a semantic and ownership reference, not as a 1:1 API, data-structure, or naming template. Hammer's app/session boundary is VPP-style session FIFO + message queue (`svm_fifo`/`svm_msg_q`) semantics; do not reintroduce io_uring-style `AppRing`, SQE, CQE, submission, or completion surfaces for dataplane app/session exchange.
- Use data structures from the `hammer-infra` crate by default. If `hammer-infra` lacks a required generic API, add the API there instead of falling back to `std` or creating local one-off utilities.
- Route all ordinary Rust and third-party production allocations through the process-global fixed-capacity Hammer Main Heap. Standard collections are the ordinary collection family; use explicit `hammer-infra` primitives when their VPP/data-plane semantics are required. SVM regions and Buffer Arena packet storage are the only allocation backends exempt from the Main Heap, and neither may fall back to it.
- Reuse existing APIs before adding new wrappers, helpers, or types. Add new API surface only when reuse is not technically viable. When a missing capability is shared by multiple use cases, add one generic primitive at the owning layer instead of adding per-feature APIs. Any new type or API in non-trivial VPP/TCP work must state the final result, explain why existing surfaces cannot satisfy the need, and receive explicit user approval before implementation.
- Utility or tool types must remain generic and must not contain business concepts. Business state names must describe the domain state directly; do not use names such as `Cursor`, `Helper`, or `Util` for business records.

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

Add integration tests near the crate whose behavior changes. Test files live in crate-local `tests/` directories (e.g. `crates/hammer-runtime/tests/`, `crates/hammer-core/tests/`). Use descriptive file names like `service_lifecycle.rs`, `config_parse.rs`, `tcp_output.rs`, or `fifo_ooo.rs`. Prefer focused tests for config parsing, lifecycle behavior, routing, TCP protocol edge cases, and data-structure correctness.

Do not write source-text assertion tests that read `.rs`, `Cargo.toml`, or other implementation files and use `contains`, regular expressions, or string matching to claim behavioral or architectural correctness. Such tests do not prove that code compiles, symbols are registered, dynamic libraries export the required inventory, generic dispatch is preserved, or runtime state is installed. Verify those properties through compile-time type checks, real `dlopen`/`dlsym` integration tests, callable lifecycle hooks, and observable runtime graph/state assertions. Source inspection is allowed only in dedicated repository-policy tooling when the property is inherently textual and cannot be expressed through compilation or behavior; it must not substitute for an executable test.

The project follows a TDD rhythm (RED → GREEN → commit) documented in `docs/superpowers/plans/`. Run `cargo test --workspace` before a PR; use `cargo test -p <crate>` while iterating.

## Commit & Pull Request Guidelines

Recent commits use scoped messages such as `hammer-runtime(Feat): per-node error counters` and `hammer-infra(Feat): add SIMD primitives`. Follow `<scope>(<Type>): <imperative summary>`, with types like `Feat`, `Fix`, `Refactor`, `Debug`, `Test`, or `docs`.

PRs should include a behavior summary, affected crates, test commands run, and any daemon, CLI, or protocol impact. Link related issues when available.

## Security & Configuration Tips

Do not commit real VPN credentials, server addresses, certificates, or generated artifacts. Keep example TOML values synthetic and document feature flags for optional protocols.

## Documentation

- `docs/superpowers/specs/` — architecture design specs (node-next traits, shared app ingress registry, timer wheel, TCP complete echo design, TCP worker driver node, L5 app session layer).
- `docs/superpowers/plans/` — dated implementation plans with checkboxes, file maps, and public-interface additions.
- `docs/superpowers/sdd/` — per-task execution tracking (progress, briefs, reports, review diffs).
- `README.md` — high-level architecture overview for the standalone data-plane framework.

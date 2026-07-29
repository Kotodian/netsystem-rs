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
| `hammer-runtime` | Runtime engine — worker thread spawning, engine main loop with VPP fixed-schedule step order, VPP-style worker barriers and synchronization primitives, `RuntimeRegistry` (typed service registry), session/app handle types. |
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
- Do not introduce `thread_local!` state. Thread-bound state must be owned
  directly by the runtime, worker, Graph Node, or other value that owns that
  thread lifecycle. Use `hammer_infra::thread_owned::ThreadOwned` only when a
  shared main structure needs indexed access to worker-owned `T: Send` values;
  do not add locks, weaken its `Send`/`Sync` contract, or force a thread-bound
  value to implement `Send` merely to fit that container.
- Express access to an existing value with Rust's ownership and borrowing
  primitives: `&T`, `&mut T`, slices, iterators, and guards. Do not introduce a
  wrapper type merely to observe, borrow, or re-expose another value, or to
  cache pointers and offsets into storage owned elsewhere.
- Do not introduce `dyn` trait objects or dynamic dispatch in production code.
  Cross-plugin cryptographic exchanges must retain concrete, monomorphized
  protocol state and use registration functions with explicit ownership; they
  must not erase state behind a trait object.
- App Session protocol state is owned by its Data Worker and advanced through
  `&mut` access. Protocol Chains and protocol tests must not add locks, atomics,
  or shared observer state around that state. A protocol may access only the
  source and destination `Fifo` adjacent to its layer; it must borrow source
  segments and transform directly into a destination write reservation. It
  must not receive an entire `AppSession`, allocate an intermediate payload,
  or copy payload through a stack buffer, `Vec`, private record, or Data-Plane
  Buffer. Destination commit precedes source consumption, and an error leaves
  both visible FIFO positions unchanged.
- Non-trivial designs must document the layer isolation contract: what each layer may call, what it must not call, which APIs cross the boundary, and which commands verify the boundary.

### Synchronization rules

- Prefer ownership over synchronization. Keep packet-path state worker-owned and
  pass `&T`/`&mut T`; use `ThreadOwned<T>` only for indexed access to values
  that remain owned by individual workers. Do not add a lock merely to satisfy
  `Send` or `Sync`.
- Project-owned generic synchronization primitives live in
  `hammer_runtime::sync`. Do not define or re-export another generic spin lock,
  reader-writer lock, fence wrapper, or barrier in another module or crate.
- Use `WorkerBarrier` when the main/control thread must stop every Data Worker
  before publishing graph, topology, registry, or other worker-visible state.
  The barrier acknowledgement is the synchronization boundary: mutate the
  barrier-owned value directly while workers are stopped. Do not add a
  `Mutex`, `RwLock`, `AtomicPtr`, publication handle, or a second completion
  protocol around that value. The only separate completion counter permitted
  is for work that continues after release, such as VPP-style graph refork.
- Use atomics for independent scalar state such as flags, counters, sequence
  numbers, and queue indices when the complete invariant fits the atomic
  transition. `Relaxed` is for statistics or thread-local ordering only;
  publication normally uses a release operation paired with an acquire
  operation, and read-modify-write transitions use `AcqRel` when both sides are
  required. Document the value being published and the matching observer. An
  atomic pointer is not an ownership model and must not replace a barrier-owned
  value.
- Use `compiler_barrier`, `release_fence`, `store_barrier`, and
  `memory_barrier` only inside a documented low-level publication, device, FFI,
  non-temporal-store, or lock-free protocol. State which accesses the fence
  orders and identify the matching atomic/device operation. A compiler barrier
  does not synchronize CPUs, and a hardware fence does not make a Rust data
  race valid.
- Use `SpinLock<T>` only for a bounded critical section between OS threads when
  sleeping would be more expensive than the protected work. While holding it,
  do not await, perform I/O or syscalls, invoke an unknown callback, allocate
  unpredictably, or enter a worker barrier. Do not put an always-contended spin
  lock on the normal packet hot path; partition state by worker first.
- Use runtime `RwLock<T>` only for short, read-dominant access with rare bounded
  writes. It is a reader-preferring spin lock matching VPP and can starve a
  writer, so it is not suitable for lifecycle transitions, long readers, or
  work that may block. It must not be layered around worker-barrier publication.
- Use `std::sync::Mutex`/`RwLock` for control-plane or lifecycle state whose
  critical section may outlast a short spin, accepting OS blocking and poison
  handling. Never hold a synchronous guard across `.await`. Use a Tokio lock
  only when shared async state genuinely must remain guarded across `.await`;
  it is forbidden in the Data Worker packet loop.
- Prefer bounded channels, lock-free rings, or handoff queues when ownership
  moves between threads. The producer must stop accessing an item after
  transfer, and backpressure/drop behavior must be part of the interface.
- Keep critical sections local and use RAII guards. Do not expose manual unlock
  methods, return references that outlive a guard, or nest synchronization
  primitives without a documented global acquisition order.

### Naming rules

- Every word in an identifier must express a domain role, fact, operation,
  state, policy, protocol, or invariant at the layer that owns it. This rule
  applies equally to production code, tests, benchmarks, examples, and test
  doubles. Do not name an item after a testing trick, data representation, or
  implementation technique when that is not the domain concept being modeled;
  names such as `PrefixLayer`, `ByteTagProtocol`, `Foo`, `Helper`, and `Wrapper`
  are forbidden for domain state. A test double must use the domain role it
  stands in, such as `TlsProtocol`, `HttpProtocol`, or `TcpTransport`.
- Follow the Rust API Guidelines naming conventions and RFC 430. Let the module
  path provide domain context instead of repeating it in every item name. Keep
  word order consistent with related standard-library and crate-local names.
  References: <https://rust-lang.github.io/api-guidelines/naming.html> and
  <https://rust-lang.github.io/rfcs/0430-finalizing-naming-conventions.html>.
- Name types and values by what they are in the domain, not by how or when the
  implementation produced them. Prefixes such as `Configured`, `Registered`,
  `Resolved`, `Parsed`, `Dynamic`, and `Runtime` are forbidden unless two
  simultaneously valid domain states with different behavior require that
  distinction.
- Use role suffixes with one fixed meaning: `Config` is parsed input, `Policy`
  is a validated published decision, `State` is mutable state-machine data,
  `Handle` is a cloneable identity or capability, `Registration` is a
  declaration awaiting installation, `Controller` actively makes domain
  decisions, and `Error` is a typed failure. Do not use a role suffix merely to
  make a name unique.
- A selection enum names the selected domain concept, normally `Algorithm`,
  `Mode`, `Backend`, or `State`. Do not use `Kind` or `Type` when a precise
  domain noun exists. Concrete implementations use their established algorithm
  names, such as `Bbr` and `Cubic`.
- Trait names describe the capability or domain role they require. Concrete
  types describe the state they own. Do not encode `Impl`, `Dyn`, `Generic`, or
  a trait name plus an implementation-detail adjective into a concrete type.
- Constructors and conversions follow the Rust API Guidelines: `new` for the
  general constructor, `with_*` for additional input, `from_*` for conversion,
  and `as_*`/`to_*`/`into_*` according to ownership and cost. Getters omit a
  `get_` prefix unless `get` is the established checked lookup operation.
- Name a value by its domain role, owner, and lifecycle phase, not by the order in
  which an implementation happened to observe it. Use names such as
  `startup_error`, `graph_update_error`, `worker_error`, and `unwind_payload`.
  Do not prefix retained failure state with vague chronological adjectives.
- Use `first` and `last` only when ordering is part of the domain contract, such
  as a packet range, sequence interval, or ordered collection operation. Include
  the ordered subject in the name; `first_sequence` is meaningful while
  `first_result` is not.
- Distinguish failure mechanisms in names. A returned `Error`, a panic's unwind
  `payload`, a cancellation, and an exit `status` are not interchangeable. Do
  not call all of them `failure`, `result`, or `outcome` when the concrete state
  is known.
- Name cleanup precedence explicitly. Use `primary_error` and `cleanup_error`
  only when the API defines that precedence; otherwise name each error after the
  operation that produced it. Cleanup must not replace the primary operation's
  error.
- Function and type names must state the domain operation or owned state. Avoid
  generic suffixes such as `Helper`, `Util`, `Manager`, `Handler`, `Thing`,
  `Data`, `Context`, `Kind`, `Type`, or `View` unless that term is the
  established domain concept. `View` is forbidden for borrowed access,
  observation, or a bundle of cached pointers/offsets; use Rust borrows. If a
  distinct value is required, name it by the domain fact it owns, not by the
  operation or lifecycle phase that produced it. Do not add a wrapper merely
  to create a place for a vague name.

### Error handling rules

Hammer follows VPP's separation between packet-processing errors, recoverable
control-plane failures, and programmer bugs. Before changing an error path,
identify the failure class, its owning module, the caller that can act on it,
and the state that must remain unchanged or be rolled back.

- Expected per-packet failures belong to the Graph Node or protocol that
  classifies them. Record a typed node error/drop/punt counter and choose the
  appropriate next arc; do not allocate an error, format a string, or return a
  control-plane `Result` for every packet. This follows VPP's
  `vlib_register_errors`, buffer error assignment, and node counters.
- Invalid configuration, resource exhaustion, OS failures, rejected lifecycle
  transitions, stale external handles, and plugin input are recoverable only
  when a defined caller can respond. Represent them with an owner-local typed
  `Result<T, E>` whose variant carries the relevant facts and, for underlying
  failures, the original `#[source]`.
- Absence uses `Option<T>` only when absence is an ordinary successful outcome.
  Do not use `None`, sentinel values, empty strings, or a logged warning to hide
  a failure that the caller must handle.
- An impossible state produced solely by a bug in the owning module is a local
  assertion or panic with the violated condition and relevant identity facts.
  Do not turn programmer bugs into recoverable errors and let execution
  continue over corrupted graph, pool, frame, queue, or ownership state. Never
  unwind across an FFI or plugin ABI boundary; contain it at the owning runtime
  boundary or terminate that execution scope according to its lifecycle.
- Do not define or use catch-all production variants such as
  `Invariant { detail: String }`, `Internal(String)`, `Other`, `Message`, or a
  generic `Subsystem { name, source }`. Renaming one of these does not make it
  typed. Every recoverable variant must name one actionable failure category;
  display text is presentation, not semantic identity.
- Error types live with the authority that owns the failed operation:
  `hammer-infra` owns generic allocation/mapping/queue failures,
  `hammer-core` owns failures intrinsic to packet-graph ABI values,
  `hammer-runtime` owns graph execution, FileMain, worker, barrier, plugin-load,
  and lifecycle failures, `hammer-service` owns interface/device/session
  failures, and each plugin owns its protocol or device failures. Do not move a
  business error downward merely to share its type or create a dependency
  cycle for error conversion.
- Translate an error at most once at a real crate, process, or ABI seam. Preserve
  both its category and source chain; use `#[from]` only when the semantic
  category is unchanged, otherwise construct the destination variant
  explicitly. Do not use `to_string()`, `format!("{error}")`, message matching,
  or repeated wrapper enums as propagation.
- `Box<dyn Error>`, `anyhow`, and ABI carriers such as `RBoxError` may carry an
  otherwise unnameable source at a process or DSO boundary, but they are not a
  domain error model. The receiving owner must immediately attach the source to
  a concrete typed category that states which operation failed. Do not expose a
  universal boxed-error helper to internal callers.
- Error context must be structured. Include identities needed to diagnose or
  retry, such as node, slot, generation, worker, lifecycle stage, path, protocol,
  or requested capacity, without embedding secrets or duplicating the source's
  display text. Variant names and fields, not prose, are the stable contract.
- Fallible mutation must be failure-atomic. Validate every participant before
  mutating shared graph/topology/registry state; after the first mutation,
  either complete infallibly or use an owner-scoped rollback guard. Never leave
  sibling tables, pending registrations, descriptor interest, worker state, or
  plugin publication partially updated after returning `Err`.
- Do not discard errors with `let _ =`, `.ok()`, a wildcard arm, or log-only
  handling unless dropping that exact failure is part of the documented domain
  semantics. Cleanup paths must retain the primary operation's error while
  still attempting all required releases; secondary cleanup failures must be
  observable without replacing the primary cause.
- At async, thread, worker, and plugin boundaries, distinguish cancellation,
  timeout, channel closure, panic, worker exit, and the inner operation's typed
  failure. These states have different recovery behavior and must not collapse
  into `Lifecycle(String)` or another message-only category.
- Tests must match concrete variants and relevant fields, verify preserved
  `Error::source()` chains at translation seams, and assert failure atomicity or
  retry behavior. A display-string assertion alone is not an error-contract
  test. Expected panics are tested only for genuinely impossible internal
  states; malformed input and resource failures must return typed errors.
- Before adding a new public error type, variant family, conversion, helper, or
  result alias in non-trivial VPP/TCP work, state the owner, recovery action,
  boundary consumers, and why an existing owner-local type is insufficient,
  then obtain the same explicit approval required for other new APIs.

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
- Congestion control remains transport-agnostic and is owned by one concrete
  `TcpConnection` as private connection-local state. Configuration selects an
  immutable algorithm operations table once; connections retain that table and
  fixed aligned private state. Congestion control is updated through typed TCP
  events and must not parameterize TCP workers or Graph Nodes, select Session
  backends, or own TCP Session/runtime state.
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

TUN/TCP lab integration is CI-only. The GitHub Actions workflow owns creation,
configuration, diagnostics, and cleanup of the host-side utun/TUN interface;
agents must not create the host interface or run the daemon/lab locally. Keep
focused compile, lint, unit, integration, and lab jobs separate so a host
capability failure cannot hide a Rust or graph-behavior failure.

## Commit & Pull Request Guidelines

Branch names use `bug/<issue>`, `feature/<issue>`, or `enhance/<issue>`.
Use the issue number as the path component and do not add agent, author, or
implementation-detail prefixes.

Recent commits use scoped messages such as `hammer-runtime(Feat): per-node error counters` and `hammer-infra(Feat): add SIMD primitives`. Follow `<scope>(<Type>): <imperative summary>`, with types like `Feat`, `Fix`, `Refactor`, `Debug`, `Test`, or `docs`.

PRs should include a behavior summary, affected crates, test commands run, and any daemon, CLI, or protocol impact. Link related issues when available.

## Security & Configuration Tips

Do not commit real VPN credentials, server addresses, certificates, or generated artifacts. Keep example TOML values synthetic and document feature flags for optional protocols.

## Documentation

- `docs/superpowers/specs/` — architecture design specs (node-next traits, shared app ingress registry, timer wheel, TCP complete echo design, TCP worker driver node, L5 app session layer).
- `docs/superpowers/plans/` — dated implementation plans with checkboxes, file maps, and public-interface additions.
- `docs/superpowers/sdd/` — per-task execution tracking (progress, briefs, reports, review diffs).
- `README.md` — high-level architecture overview for the standalone data-plane framework.

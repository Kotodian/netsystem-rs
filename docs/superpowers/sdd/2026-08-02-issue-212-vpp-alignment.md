# Align app/session protocol layers with VPP session_cb_vft

## Parent

- #209

## Goal

Replace the `AppSessionProtocol` abstraction with a VPP-shaped Session App
callback table. Do not introduce `AppSessionProtocol`, `call`, `Response`,
`Association`, or a generic protocol relationship registry. This issue defines
the replacement seam and migrates the current TLS plugin onto it. It does not
implement HTTP/2 or HTTP/3, but the resulting seam must let those plugins use
the same Session callback surface.

## Why the previous design is rejected

- A one-method `call` plus a generic `Response` enum is not VPP's model. VPP
  separates session lifecycle callbacks, transport callbacks, and builtin app
  RX/TX callbacks.
- A protocol returning `Accept`, `Open*`, `Output`, `Ready`, `Reset`, or
  `Close` forces Session to interpret protocol policy. VPP lets the protocol
  plugin call Session APIs directly and keep its own worker-local state.
- The current code still stores one fixed lower/upper `SessionHandle` pair in
  `AppSessionProtocolConnections`. It cannot represent HTTP/2 or HTTP/3
  connection-scoped state.

## VPP model and evidence

- `third_party/vpp/src/vnet/session/application_interface.h:15-75` defines
  `session_cb_vft_t`: accept, connected, disconnect, reset, transport closed,
  cleanup, builtin RX, and builtin TX callbacks.
- `third_party/vpp/src/vnet/session/session_input.c:119-245` dispatches
  exact session events to the owning app's callback table.
- `third_party/vpp/src/vnet/session/session_node.c:1745-1917` resolves the
  exact `session_index` before dispatching control and IO events.
- `third_party/vpp/src/vnet/tls/tls.c:588-601` registers TLS as a builtin
  Session app over TCP and creates the upper app Session when the TLS session
  is ready.
- `third_party/vpp/src/plugins/http/http.c:1004-1016` registers HTTP as a
  builtin Session app. HTTP keeps private connection state and request mapping.
- `third_party/vpp/src/plugins/http/http_private.h:365` keeps
  `req_by_stream_id` private to the HTTP/2 connection; HTTP/3 distinguishes
  request, control, and QPACK streams internally.

## Ownership model

```text
Session
  owns session pool, FIFOs, message queues, lifecycle, publication,
  and exact event routing

Session App callback table
  concrete static registration owned by one plugin
  receives the exact Session and its opaque plugin context
  calls Session-owned allocation/publication/stream APIs directly

Session App context
  worker-owned concrete protocol state, private to the plugin

Transport
  owns TCP/QUIC connections and QUIC stream state
  retains opaque Session identity only
```

Important consequences:

- A session entry stores the registered Session App identity and an opaque
  plugin context, matching VPP `session_t.app_wrk_index` plus `session_t.opaque`.
- Plugin state lives in the plugin, not in Session and not behind a generic
  protocol trait.
- A protocol layer allocates its upper Session by calling Session APIs from
  its callback. Session validates, allocates, publishes, and routes that upper
  Session.
- No public protocol `Stream`, relationship registry, chain scan, recursive
  drain, or protocol-private work queue is introduced.
- Transport and Session App never select each other's policy. The application
  endpoint selects one Session App and transport configuration.

## Required Hammer surface

The following public surface is proposed and requires explicit approval before
code changes.

- `SessionAppId`: compact `u32` identity for a registered Session App.
- `SessionAppRegistration`: runtime registration-list entry, following the
  existing `SessionTransportRegistration` pattern; `declare_plugin!` carries it
  through `RegistrationImage::session_apps`.
- `SessionAppCallbacks`: service-side concrete callback table with all 19 VPP
  callbacks as `Option<fn>`. It is installed by plugin `worker_init`, matching
  how TCP installs its Session Queue dispatch attachment.
- `SessionApp` trait: plugin-facing trait with default no-op methods for the
  same 19 callbacks. `#[session_app]` generates the static
  `SessionAppRegistration` and `SessionAppCallbacks` glue; plugin state remains
  in the plugin.
- `SessionAppId` is attached to the existing listener/connect endpoint
  configuration. The endpoint still carries transport and crypto/ALPN facts;
  no `SessionAppSelection` or ordered protocol-chain type is added.
- `SessionEntry` fields: `app: Option<SessionAppId>` and
  `app_session: u64` opaque. No `app_flags` is added initially. Remove
  `SessionType::AppSessionProtocol` and
  `SessionApplication::AppSessionProtocol`.
- `hammer-service::session::protocol`: exports the replacement callback table
  surface. `AppSessionProtocol`, `AppSessionProtocolEntry`,
  `AppSessionProtocolConnections`, `AppSessionProtocolRole`,
  `AppSessionProtocolConnectionId`, `AppSessionPolicy`,
  `AppSessionProtocolSelection`, and the `app_session_protocol` macro path are
  removed.

## Session Queue dispatch

- `RxEnq`, `TxEnq`, `RxDeq`, `TxDeq`, and `ProtocolOutput` resolve the exact
  session and call the exact Session App callback selected by that session.
- `accept`, `connected`, `disconnect`, `reset`, `transport_closed`, and
  `cleanup` are separate Session App callbacks, not variants of a generic
  response.
- All 19 VPP callbacks are present in `SessionAppCallbacks` and in the
  plugin-facing `SessionApp` trait defaults. Unimplemented callbacks remain as
  `None` or no-op defaults; they are not deleted because they are not yet
  implemented.
- When a Session App needs a new upper Session, lower stream, publication, or
  lifecycle transition, its callback calls a Session-owned operation through
  `&mut SessionWorker<PoolIndex>`. Session validates every participant before
  mutation and remains failure-atomic.
- When the Session Queue work budget is exhausted, unfinished work is
  re-enqueued as one exact `SessionEvt` targeting the exact session. No
  protocol poll loop or protocol-local event queue is added.

## FIFO and failure-atomicity contract

- A callback may borrow only the adjacent FIFOs obtained through
  `SessionWorker` for the exact operation. It does not receive separate `&Fifo`
  arguments, avoiding borrow conflicts with Session-owned mutations.
- Ingress and egress transform source segments directly into a destination
  write reservation, commit the destination, then consume the source.
- On error, both visible FIFO positions remain unchanged.
- No intermediate payload `Vec`, stack copy, private payload record,
  Data-Plane Buffer staging, whole `AppSession`, or cached foreign FIFO
  reference is allowed.

## TLS migration

- `hammer-plugin-tls` moves from implementing `AppSessionProtocol` to
  registering `SessionAppCallbacks`.
- Its concrete `Connection` remains worker-owned and is addressed by the
  opaque field in the Session entry.
- `accept`/`connected` creates or advances the TLS context and calls
  Session to allocate the upper app Session only after enough handshake input
  is available.
- `builtin_rx`/`builtin_tx` operate only on adjacent FIFOs and follow the FIFO
  contract.
- `disconnect`, `reset`, `transport_closed`, and `cleanup` own TLS
  shutdown and context release; Session owns the exact Session lifecycle
  transition.

## Files expected to change

- `crates/hammer-runtime/src/app/policy.rs`
- `crates/hammer-runtime/src/app/protocol.rs` (replaced)
- `crates/hammer-runtime/src/app/mod.rs`
- `crates/hammer-runtime/src/registration.rs`
- `crates/hammer-component-macros/src/lib.rs`
- `crates/hammer-service/src/session/application.rs`
- `crates/hammer-service/src/session/runtime.rs`
- `crates/hammer-service/src/session/protocol.rs`
- `crates/hammer-service/src/session/mod.rs`
- `crates/hammer-service/src/lib.rs`
- `crates/hammer-app/src/lib.rs`
- `crates/hammer-app/src/session.rs`
- `crates/hammer-plugins/app-session/tls/src/lib.rs`
- `crates/hammer-runtime/tests/app_session_protocol_registration.rs`
- `crates/hammer-plugins/transport/tcp/src/session_driver_tests.rs`

## Required tests

- Session Queue dispatches each event to the exact Session App callback and
  exact opaque context.
- TLS -> HTTP/1 shape is expressible through callbacks without a generic
  protocol response.
- HTTP/2-shaped private stream mapping is expressible in plugin state without
  a Session relationship registry.
- HTTP/3-shaped multiple lower streams and internal control/QPACK streams are
  expressible without exposing them as AppSessions.
- Partial input, destination-full retry, notification coalescing, bounded
  re-entry, and exact-session retry are covered.
- Injected transform errors leave source and destination visible positions
  unchanged.
- Registration/plugin-load tests prove concrete static callback tables and the
  absence of `dyn` and of `AppSessionProtocol`.
- No source-text assertion tests are used as architectural proof.

## Rejected designs

- `AppSessionProtocol`, `AppSessionProtocol::call`, `Response`, or any generic
  protocol result enum.
- A protocol method for every Session event plus a one-method dispatcher that
  encodes those events as data.
- Generic `Association`, relationship registry, public `Stream`, chain scan,
  recursive traversal, or protocol-local work queue.
- Session-owned protocol state, fixed lower/upper handle pairs, and ordered
  protocol-chain policy as the layering mechanism.
- `dyn`, locks, atomics, connection cloning, FIFO-reference caching, payload
  buffers, and wrapper objects whose only purpose is to re-expose Session
  borrows.

## Acceptance criteria

- [x] `AppSessionProtocol` is removed from the public API and registration path.
- [x] A registered Session App is a concrete static callback table without
      `dyn` protocol state.
- [x] Session entries store only `SessionAppId`, opaque context, and
      Session-owned state.
- [x] Session Queue dispatches exact session events to exact Session App
      callbacks.
- [x] TLS migrates to the callback table and retains adjacent-FIFO-only
      behavior.
- [x] HTTP/2 and HTTP/3 shapes are testable without implementing HTTP/2 or
      HTTP/3.
- [x] Application endpoint selection replaces the ordered
      `AppSessionProtocolSelection` chain with a registered `SessionAppId`
      plus transport/crypto configuration.
- [x] Focused behavior, failure-atomicity, and no-forbidden-allocation tests
      pass.

## Out of scope

- Implementing HTTP/2.
- Implementing QUIC or HTTP/3; #209 consumes this seam.
- Changing transport plugin ownership or `SessionTransport` lower-layer
  behavior beyond removing the `AppSessionProtocol` seam.
- Adding a second transport, buffer, allocator, or synchronization subsystem.

## VPP review

Feature: Session App callback seam replaces `AppSessionProtocol`.

VPP analog and evidence:

- `third_party/vpp/src/vnet/session/application_interface.h:15-75` defines
  `session_cb_vft_t` with 19 callbacks.
- `third_party/vpp/src/vnet/session/session_input.c:119-245` dispatches exact
  Session IO and lifecycle events through `app->cb_fns`.
- `third_party/vpp/src/vnet/session/session_node.c:1745-1917` resolves the
  exact `session_index` before transport and app dispatch.
- `third_party/vpp/src/vnet/tls/tls.c:588-601` registers a static
  `session_cb_vft_t` and owns opaque TLS context in `session_t.opaque`.

Hammer implementation:

- `SessionAppRegistration` is carried in `RegistrationImage::session_apps` and
  installed per worker through a concrete static callback table.
- `SessionWorker` stores `app: Option<SessionAppId>` and
  `app_session: SessionAppContext`, with no generic protocol trait, `dyn`, or
  ordered protocol chain.
- `SessionQueue` dispatches `RxEnq`/`RxDeq`, `TxEnq`/`TxDeq`,
  `ProtocolOutput`, and lifecycle events to the exact Session App callback.
- TLS registers `SessionAppCallbacks`, creates its worker-owned context in
  `accept`/`connected`, publishes the upper App Session through Session APIs,
  and transforms only adjacent FIFOs.

Verdict: `Aligned`.

Non-blocking notes:

- `connected`/`accept` currently publishes the upper App Session immediately;
  the TLS plugin can later defer publication until the handshake becomes ready.
  The seam already supports that because the callback owns context and calls
  `SessionWorker::create_upper_session`.
- Full workspace tests were intentionally not run per user instruction; focused
  tests are listed under Commands run.

Commands run:

- `cargo check --workspace --all-targets`
- `cargo test -p hammer-runtime --test app_session_protocol_registration`
- `cargo test -p hammer-runtime --lib app::session_app`
- `cargo test -p hammer-service --lib session::runtime`
- `cargo test -p hammer-plugin-tls --test fifo_connection`
- `cargo test -p hammer-plugin-tcp --lib session_driver_tests`

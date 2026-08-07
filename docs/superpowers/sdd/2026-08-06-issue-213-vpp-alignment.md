# Issue 213 VPP Alignment Review

## Feature and changed surface

This review covers the shared Session listener seams required by the QUIC
listener design and the TCP/UDP migration to that seam:

- `hammer-runtime` carries the transport-neutral listener callback contract,
  including the opaque Session listener identity, Application identity, and
  optional Application configuration fact.
- `hammer-service` owns Application listener configuration publication,
  Session App name-to-id resolution, listener creation/removal, and the outer
  worker-barrier transaction.
- TCP and UDP register their own Session transport callbacks. Their plugins
  own endpoint lookup and worker state; they do not publish a second generic
  listener registry.
- `hammer-plugin-quic` is limited to the VPP-shaped listener skeleton: it
  owns the lower UDP listener relationship, plugin-local listener/config
  identity, and Session App registration. It does not yet implement QUIC
  packet processing, connection/stream Session fan-out, or timer delivery.

## VPP analog and evidence

- `third_party/vpp/src/vnet/session/application.c:1276-1320` implements
  `vnet_listen` on the main thread with the worker barrier, allocates the
  application listener, and delegates protocol setup to
  `app_worker_start_listen`.
- `third_party/vpp/src/vnet/session/session.c:1464-1487` implements
  `session_listen`: Session calls `transport_start_listen` first, then attaches
  the returned transport identity to the listening Session.
- `third_party/vpp/src/vnet/session/session.c:1496-1514` implements
  `session_stop_listen`: it removes the Session lookup before calling the
  transport stop operation.
- `third_party/vpp/src/vnet/session/application_worker.c:231-323` creates the
  local/global listening Sessions and adds their endpoint lookup only after
  transport setup succeeds. `:466-490` removes worker listener state and then
  cleans up the application listener.
- `third_party/vpp/src/vnet/session/transport.c:317-327` shows that each
  protocol owns its concrete transport VFT and registers it with Session.
  TCP and UDP register at `third_party/vpp/src/vnet/tcp/tcp.c:1718-1722` and
  `third_party/vpp/src/vnet/udp/udp.c:705-709`.
- `third_party/vpp/src/plugins/quic/quic.c:267-360` starts QUIC by opening a
  lower connected UDP listener and publishing the QUIC listener context in the
  lower UDP Session's opaque field. `:363-383` stops that lower listener before
  freeing the QUIC listener context. Its transport registration is at
  `:899-919` and `:953-970`.

## Ownership and barrier

`SessionMain::with_control_barrier` is the single service-owned listener
barrier authority. Its direct control-thread `listen`/`unlisten` methods
acquire `WorkerBarrier` around Session pool mutation and the plugin callback;
the Application Session Message Queue listen/unlisten wrappers enter the same
authority. `BinaryApiConnection` is the analogous root owner for one
synchronous Binary API method. `ApplicationMain`, Session's private pool
mutation, and the QUIC configuration registry reuse an already pending barrier
and acquire one only for a standalone Main Thread operation; they do not add an
unconditional nested synchronization scope. The root scope remains the
publication boundary.

Transport callbacks call `ensure_main_thread_with_barrier` before mutating
plugin listener lookup. TCP publishes its immutable lookup snapshot through
its existing control-plane publication path. UDP mutates its main-thread
listener collection under the same barrier. Data Workers retain only the
opaque `SessionListenerId` in transport-owned worker state.

The `ApplicationMain::update_listener_config` seam is intentionally generic:
QUIC can publish its own listener context identity into an existing lower
Application listener while the barrier is held. `session_app_id` resolves the
plugin's registered name to the service-owned compact identity; no QUIC type,
callback table, or plugin state crosses into runtime or service.

QUIC's listener and configuration pools remain plugin-owned. The creator Main
Thread owns both pools; the configuration registry uses its own owner-local
`UnsafeCell` because its Main Thread check and WorkerBarrier phase are the
synchronization contract rather than a generic runtime wrapper. The Session
listener callback proves the worker barrier before accessing the listener pool.
A configuration operation either runs under the Binary API
barrier or enters that barrier itself. No Data Worker reads the registry in
this skeleton; future connection setup must retain immutable configuration in
the worker-owned connection context rather than borrow the registry.

## Lifecycle and failure atomicity

On listen, Session allocates its listener identity before invoking the
transport. A missing callback or transport error removes only that new
Session listener. The caller-owned Application listener remains available for
the enclosing Application transaction to roll back.

On unlisten, the transport callback removes the plugin lookup and transport
state first. Session removes its generic listener only after the callback
succeeds. If the callback fails, both the plugin lookup and Session identity
remain available for retry. This is the Hammer equivalent of VPP's lookup
deletion-before-transport-stop ordering while preserving a typed error and
failure-atomic control-plane state.

Application listen/unlisten wraps the complete outer transaction and removes
the Application listener only after the Session operation succeeds. Existing
Session listener identities remain generation checked, so a stale transport
callback cannot target a replacement listener.

## Findings

### Non-blocking: TCP configuration migration is now an explicit follow-up

The former TCP plugin-owned `[plugin.tcp].listen` list was removed and TCP now
binds only from the Session transport registration callback. This matches VPP's
Session-to-transport registration model, but the remaining control-plane and
documentation consumers should be tracked separately so future TCP work cannot
reintroduce a second bind path. Follow-up issue #231 tracks that migration
contract.

### Non-blocking: stop callback combines VPP's lookup removal and transport stop

VPP exposes lookup deletion in Session and transport destruction through the
transport VFT. Hammer's plugin callback owns both because the plugin's lookup is
its private representation. The callback boundary is still ordered and
barriered, and the generic service layer retains only the opaque identity.

### Non-blocking: QUIC data plane remains a separate milestone

The listener skeleton intentionally stops before VPP's
`quic_udp_session_accepted_callback` (`third_party/vpp/src/plugins/quic/quic.c:541-577`).
Lower UDP FIFO to Quinn sans-I/O RX/TX, worker-owned connection/stream context
creation, Session fan-out, exact timer-token delivery, and FIFO-only stream
payload ownership remain required before QUIC is a usable transport.

## Barrier follow-up

The generic `hammer_runtime::Barrier<T>` has been removed. Runtime graph,
graph-error, and worker-statistics publication now use the runtime-owned
`WorkerPublication` slots with explicit ownership phases; QUIC configuration
storage uses its plugin-owned `UnsafeCell` and the existing Main Thread plus
`WorkerBarrier` contract. No generic value wrapper, lock, or atomic pointer was
added in its place.

The runtime `WorkerBarrier` now keeps `wait_at_barrier` and
`workers_at_barrier` in separate 64-byte cache lines, matching VPP's aligned
allocations at `third_party/vpp/src/vlib/threads.c:605-608`. Its worker
acknowledgement and release sequence follows
`third_party/vpp/src/vlib/threads.h:297-361` and
`third_party/vpp/src/vlib/threads.c:1396-1408,1479-1488`: the main thread
publishes under the barrier, workers acknowledge with a release operation, and
the main thread observes completion with acquire loads before reading slots.
Its `sync` adapter accepts only the operation closure; it does not borrow or
store a generic value. Callers capture or borrow state from the owner that
actually owns the publication, matching VPP's separate barrier and state
responsibilities.

### Barrier verdict

**Aligned.** The generic synchronization type is gone, the remaining barrier
owns only synchronization state, and each transferred value has an explicit
owner and completion event. The separate completion counter remains only for
VPP-style graph refork work that continues after barrier release.

## Verdict

**Aligned for the shared listener seam, QUIC listener skeleton, and barrier
publication change.** The shared contract follows VPP's ownership and ordering
without moving QUIC-specific state below the plugin boundary. This is not a
claim that the QUIC transport is feature complete.

## Commands run

- `cargo check -p hammer-plugin-quic --lib`
- `cargo test -p hammer-plugin-quic --lib`
- `cargo test -p hammer-service --lib session::runtime`
- `cargo test -p hammer-service --test application_listener`
- `cargo test -p hammer-service --test binary_api`
- `cargo test -p hammer-plugin-tcp --all-targets`
- `cargo test -p hammer-plugin-udp --all-targets`
- `cargo clippy -p hammer-plugin-quic --lib --tests --message-format=short`
- `cargo test -p hammer-runtime --lib`
- `cargo test -p hammer-runtime barrier::tests --lib`
- `cargo check -p hammer-runtime -p hammer-plugin-quic`
- `cargo check --workspace`
- `cargo test --workspace`
- `cargo clippy -p hammer-service --test binary_api --message-format=short`
- `cargo clippy --workspace --all-targets`
- `cargo fmt --all -- --check`
- `git diff --check`

Workspace checking and the full workspace test pass, including the runtime
barrier tests and all QUIC tests. Workspace-wide clippy still reports existing
`mut_from_ref` errors at `quic/src/listener.rs:66`,
`udp/src/input.rs:555`, and `udp/src/worker.rs:92`; none of those files are part
of this barrier change.

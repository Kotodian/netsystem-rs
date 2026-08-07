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
- `hammer-plugin-quic` owns the lower UDP listener relationship, plugin-local
  listener/config identity, Session App registration, and the worker-local
  QUIC data path. The worker now reuses 16 fixed RX datagram slots, 64 packet
  descriptors, and 10 TX slots; complete connection/stream Session fan-out,
  full timer delivery, and FIFO-only stream payload movement remain incomplete.

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
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:1357-1365` stops the
  current receive batch when packet decoding fails, while `:1834-1855`
  counts decoded packets and dispatches them in wire order. Fatal
  `quicly_receive` resource/state failures at `:1608-1628` close the
  connection. `third_party/vpp/src/plugins/quic/quic_error.def:26` names the
  packet-drop counter.
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly_crypto.c:933-1003`
  removes header protection and decrypts in place; a failed AEAD operation is
  dropped and a successful operation truncates the visible packet to
  `ptlen + aead_off`.

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

### Dataplane slice: TX reservation and publication

The current worker TX slice follows the VPP send path at
`third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:242-318`:

- the lower UDP TX FIFO is checked against a maximum-sized record and the
  VPP two-record backpressure threshold before polling Quinn;
- one maximum-sized FIFO reservation is established before each
  `poll_transmit`, so resource failure is returned before Quinn advances its
  send state;
- the actual Session datagram copies only `Transmit::size` bytes and commits
  one complete record; one `publish_tx_enqueue` follows the burst;
- the worker TX pending queue has fixed startup capacity and deduplicates one
  connection, so steady-state scheduling does not use `mem::take` to force a
  new allocation.

The post-poll copy/commit result is a local invariant assertion, matching VPP
`ASSERT(ret > 0)` after `svm_fifo_provision_chunks`; the pre-poll reservation
failure remains a typed `FifoError` source on `OutputReservationFailed`. Engine
and protocol-connection absence now remain typed worker errors, while the
timer-wheel bound is a local assertion matching VPP's invariant checks. The
timer-arm rollback preserves the primary timer error and only logs a secondary
cleanup failure.

### Non-blocking: fixed QUIC input scratch boundary is now present

The worker RX path at `quic/src/worker.rs:556-645` reuses 16 aligned datagram
slots and one 64-entry descriptor vector. The synchronous Quinn entry points
at `third_party/quinn/quinn-proto/src/endpoint.rs:152-184` and
`connection/mod.rs:459-525` borrow the caller-owned scratch, while
`PartialDecode` retains only the parsed header and packet range.

The remaining gap is data movement after decode: endpoint accept still copies
coalesced remainder bytes into `Incoming.rest`, and `PartialDecode::finish`
still creates owned `Packet` payload storage for frame processing.

### Resolved: duplicate STREAM frames can create a second stream Session

`quic/src/worker.rs:695-706` calls `create_stream_context_with_io` for every
STREAM frame before Quinn receives it, without checking whether the stream
already has a Session. `quic/src/stream_io.rs:68` then asserts that no previous
entry exists, so a duplicate or retransmitted STREAM frame panics instead of
reusing the same Stream Context and Session. VPP creates the stream Session once
when the stream is opened and `quic_quicly_on_receive` reuses it for overlap and
duplicate frames.

### Resolved: RX flow-control state can advance for bytes not accepted by FIFO

`quinn-proto/src/connection/streams/recv.rs:91-111` calls the Session FIFO
receive callback and then advances `self.end` and returns the full frame's
`new_bytes`, but `quic/src/stream_io.rs:190-201` returns success for a partial
`Fifo::enqueue`. VPP checks the available FIFO space first and does not advance
`quicly` receive state when the stream FIFO cannot retain the full frame.

### Resolved: `app_rx_evt` uses the wrong FIFO fact to advance QUIC credit

Session Runtime passes free RX capacity to `app_rx_evt`
(`crates/hammer-service/src/session/runtime.rs:3196-3200`), while
`quic/src/stream_io.rs:245-248` interprets it as remaining readable bytes and
returns `app_rx_data_len - free_capacity`. The correct VPP-shaped update is the
newly consumed dequeue delta computed from exact FIFO state, matching
`quic_quicly_ack_rx_data` (`third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:497-527`).

### Resolved: active QUIC connect is not implemented

The QUIC `SessionTransportRegistration` declares no `connect`
(`quic/src/worker.rs:429-433`), and `connect_connection`
(`quic/src/worker.rs:498-519`) only inserts a pending Context. There is no
`Endpoint::connect` call, no first client Initial, and no client
`Connection` construction. Client config registration exists but is unused.

### Resolved: vendored Quinn owns per-packet input without scratch payload copies

`PartialDecode::finish` now borrows the caller-owned scratch through the
existing `Packet` type, and `frame::Iter`/`Frame` borrow the same decrypted
payload instead of creating a second owned payload. `Incoming` copies the
first datagram once and splits header, payload, and remaining bytes from that
existing buffer. `Connection::poll_transmit` writes through the existing
`BytesMut`-backed `BytesBuffer`, so no growable `Vec` remains in the worker TX
path.

### Resolved: stream receive window is not tied to FIFO capacity

QUIC config leaves Quinn's default stream receive window
(`third_party/quinn/quinn-proto/src/config/transport.rs:353-369`) while the
Session FIFO default is 64 KiB (`crates/hammer-runtime/src/app/session.rs:863-868`).
VPP sets stream flow-control limits from `sm_properties.rx_fifo_size` and
`tx_fifo_size` (`third_party/vpp/src/plugins/quic_quicly/quic_quicly_crypto.c:681-684`).
The current mismatch makes full-frame FIFO rejection and partial enqueue
reachable.

### Resolved: per-datagram stream scan and allocations remain

`StreamIoTable::take_events` (`quic/src/stream_io.rs:226-243`) scans every
installed stream and allocates a new `Vec` after each datagram. The approved
event model is exact Session targeting with fixed worker scratch and no
steady-state allocation.

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

**Aligned for the shared listener seam, QUIC listener skeleton, barrier
publication, active connect, Session-FIFO stream ownership, and borrowed RX
decode. The remaining issue work is the final executable test gate before
commit.**

## 2026-08-07 current HEAD confirmation

The current `feature/213` HEAD aligns with VPP on the shared listener seam,
WorkerBarrier publication, 64-byte `Context` layout, fixed RX/TX scratch,
exact two-kind timer dispatch, active connect, Session-FIFO stream ownership,
and borrowed RX decode. The old `PendingRx.bytes: Vec<u8>` blocker is gone.

Command verification: `cargo check -p hammer-plugin-quic --lib` and
`git diff --check main...HEAD` pass. No tests were run in this continuation, per
the repository test-timing rule.

## Commands run

- Vendored VPP source inspection with `rg`, `sed`, and numbered source views.
- `cargo check -p hammer-plugin-quic --lib` (this continuation).
- `git diff --check` (this continuation).

Tests were not run in this continuation, per maintainer instruction. The
review verdict remains `Needs changes`.

## 2026-08-07 active connect alignment

`SessionConnectEndpoint` is the Hammer equivalent of VPP
`session_endpoint_cfg_t`: it is one public struct carrying remote/local
endpoints, worker identity, connection identity, application, opaque, and
hostname. The transport connect callback receives only this endpoint config,
matching VPP `connect(transport_endpoint_cfg_t *)`.

QUIC active connect now follows `quic_connect_connection` /
`quic_udp_session_connected_callback`:

- QUIC allocates a worker-owned Connection Context before opening the lower
  UDP Session.
- The ContextId is passed as the lower Application Connection opaque, matching
  VPP passing `ctx_index` as the UDP connect api_context.
- The QUIC connected callback resolves that exact ContextId, initializes
  `Endpoint::connect`, and immediately sends the first Initial through
  `send_packets`.

The duplicate STREAM, full-FIFO rejection, exact app RX dequeue delta, receive
window alignment, exact dirty-stream event publication, and Session FIFO-mode
Quinn payload storage fixes are present. `cargo check --workspace` and
`cargo check -p hammer-plugin-quic --tests` pass; executable tests were not run
because the issue remains incomplete.

Quinn TX now uses `bytes::BytesMut` through the existing `BytesBuffer` type
instead of `Vec<u8>`, so the packet encoder writes into a fixed-capacity
existing buffer rather than a growable `Vec`. Borrowed RX frame ownership is
now complete: `Frame`/`Iter` borrow the existing packet payload, and no new
frame view type was introduced.

## VPP feature review

### Feature and changed surface

The remaining QUIC dataplane completion work in `feature/213`: Session FIFO
stream ownership, active connect, exact RX flow-control behavior, and
scratch-borrowed RX decode. The changed files are the QUIC plugin and the
vendored `quinn-proto` packet/frame/session interfaces.

### VPP analog and evidence

- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:1767-1864`
  `quic_quicly_udp_session_rx_packets` peeks datagrams from the connected UDP
  RX FIFO into fixed scratch and passes packet contexts to quicly without an
  intermediate payload copy.
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:242-318`
  `quic_quicly_send_packets` uses fixed TX buffers and reserves Session TX
  FIFO space before quicly advances send state.
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:530-625`
  `quic_quicly_on_receive` writes QUIC stream bytes directly to the stream
  Session RX FIFO, rejects a full FIFO without advancing quicly state, and
  later `quic_quicly_ack_rx_data` at `:497-527` advances credit from the exact
  app dequeue delta.

### Verdict

**Aligned.** `Frame`/`Iter` now borrow the decrypted packet payload in place,
`PartialDecode::finish` borrows the caller scratch, `Incoming` keeps one
datagram allocation, and stream payload is delivered through Session FIFOs.
No new frame view type was introduced; existing `Frame`, `Iter`, `Packet`, and
`Incoming` names carry the ownership change.

### Findings

No blocking findings. The remaining gate is the repository's final executable
test run before commit.

### Commands run

- `cargo check -p quinn-proto --tests`
- `cargo check -p hammer-plugin-quic --tests`
- `cargo check --workspace`
- `cargo fmt --all`
- `git diff --check`
- `cargo test -p hammer-plugin-quic stream_io::tests`
- `cargo test -p quinn-proto --lib`
- `cargo test --workspace`

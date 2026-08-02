# Issue #228 VPP alignment review

## Feature and changed surface

This change adds a UDP `SessionTransport` to Hammer:

- `hammer-plugin-udp` registers a callable `udp` transport with
  `start_listen`, `stop_listen`, and `connect`.
- `UdpMain` owns barrier-protected listener state and a shared VPP-style UDP
  session lookup table.
- `UdpWorker` owns per-worker `Pool<UdpConnection>`, the exact tuple lookup,
  Session lifecycle, and datagram TX through `TransportInternalTx`.
- `udp-input` performs tuple/listener delivery before falling back to external
  destination-port dispatch; datagrams enter the exact Session RX FIFO with a
  `SessionDgramHeader`.
- `udp-output` prepends IPv4/IPv6 headers and computes UDP checksums before
  routing to `ip-lookup`.
- Session control (`listen`/`unlisten`) holds the worker barrier before invoking
  the UDP transport VFT; UDP control operations validate that they are on the
  main engine and inside the barrier phase when workers exist.

## VPP analog and evidence

- `third_party/vpp/src/vnet/session/session_node.c:35-38` checks main/barrier
  before Session control work and forwards non-main control to main.
- `third_party/vpp/src/vnet/session/session_node.c:854-903` wraps Session main
  control handling in `vlib_worker_thread_barrier_sync/release`.
- `third_party/vpp/src/vnet/udp/udp.c:155-200` implements
  `udp_session_bind`; `udp.c:664-675` registers the UDP transport VFT.
- `third_party/vpp/src/vnet/udp/udp.c:17-62` publishes transport-owned UDP
  ports under the caller's barrier phase and uses only relaxed refcount
  accounting, not RCU.
- `third_party/vpp/src/vnet/udp/udp_local.c:194-201` reads the packet-path
  sparse destination-port vector directly.
- `third_party/vpp/src/vnet/udp/udp_input.c:298-335` performs Session lookup,
  listener accept, wrong-thread classification, and FIFO enqueue.
- `third_party/vpp/src/vnet/session/session_lookup.c:1114-1134` keeps a shared
  session lookup table whose value is a Session Handle.

## Hammer implementation

- `crates/hammer-service/src/session/runtime.rs`: `SessionMain::listen` and
  `unlisten` acquire the worker barrier before calling the transport VFT.
- `crates/hammer-runtime/src/engine.rs`: `ensure_main_thread_with_barrier()`
  is the generic main/barrier validation used by UDP control paths.
- `crates/hammer-plugins/transport/udp/src/worker.rs`: `UdpMain` owns shared
  listener state and `UdpSessionLookup`; `UdpWorker` owns connections,
  worker-local tuple lookup, accept/connect, datagram delivery, and internal TX.
- `crates/hammer-plugins/transport/udp/src/lookup.rs`: worker-local tuple
  Bihash plus a shared Session Handle lookup mirroring VPP Session lookup.
- `crates/hammer-plugins/transport/udp/src/input.rs`: parses UDP, performs
  Session delivery, and then falls back to the existing destination-port
  sparse dispatch.
- `crates/hammer-plugins/transport/udp/src/output.rs`: writes IPv4/IPv6 headers
  and UDP checksums, matching VPP's output push path.
- `crates/hammer-infra/src/sparse_vec.rs`: VPP-style sparse vector used for the
  packet-path destination-port next-slot table.

## Findings

Verdict: **Aligned**.

No blocking findings remain.

Non-blocking note:

- Hammer publishes active-open UDP Sessions immediately after transport
  creation. A datagram arriving on a non-owning worker therefore hits the VPP
  `READY`/`ACCEPTING` wrong-thread classification and is dropped with
  `UdpInputError::WrongWorker` instead of the VPP `OPENED` clone/migrate path.
  This is an intentional divergence until Hammer models half-open datagram
  Sessions.

## Commands run

- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo check --workspace --all-targets`
- `cargo test -p hammer-plugin-udp --all-targets`
- `cargo test --workspace`

# Hammer

Hammer is a VPP-style high-performance network data plane framework written in
Rust. It is organized around a packet graph runtime: graph nodes process frames
of data-plane buffers on worker-owned state, while the control plane coordinates
lifecycle, configuration, and barriers outside the hot path.

## Workspace Layout

| crate | role |
|---|---|
| `hammer-infra` | Bottom-layer infrastructure: the fixed-capacity process-global Main Heap, lock-free queues, pools, timer wheels, VPP-style Bihash, memory segments, checksums, SIMD helpers, and ring buffers. |
| `hammer-core` | Base types: config schema, errors, lifecycle, metrics, logging, network primitives, forwarding tables, and protocol wire types. |
| `hammer-component-macros` | Registration macros for packet graph nodes and initialization functions. |
| `hammer-runtime` | Runtime engine for worker threads, graph dispatch, barriers, service registry, and session/app handles. |
| `hammer-service` | Network stack and packet graph services: interface management, IP, ICMP, TCP, UDP, session layer, device driver, and graph registration. |
| `hammer-app` | Application-plane attach/session interface for local and shared-memory app sessions. |
| `hammer-ipc` | Daemon and CLI IPC protocol, framing, request/reply types, and handler registration. |
| `hammer` | Standalone daemon binary. |
| `hammerctl` | Control CLI for daemon operations. |

Dependency direction stays one-way: higher-level binaries and services depend on
runtime, core, and infra crates; bottom-layer crates do not depend back up the
stack.

## Runtime Model

Hammer follows VPP's packet graph shape:

- A data worker owns hot-path state and runs graph nodes over frames of buffers.
- A graph node transforms packet metadata or payload state and selects named next
  arcs for subsequent graph nodes.
- Session runtime owns app/session FIFO readiness and TX packet preparation.
- Transport logic owns protocol facts such as TCP sequence, ACK, recovery, and
  timer decisions.
- Control-plane changes use barrier synchronization to observe stable worker
  state without adding locks to packet processing.

## Configuration

Configuration is TOML. The daemon loads `startup.toml` by default through the
normal config loader.

```toml
[log]
level = "info"

[[inbounds]]
type = "tun"
id = "tun"
interface_name = "tun0"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
mtu = 1408
stack = "system"
auto_route = true
sniff = true

[[outbounds]]
type = "block"
id = "block"

[[route.rules]]
domain_keyword = ["doubleclick", "analytics"]
outbound = "block"

[route]
final = "block"
auto_detect_interface = true
```

Optional protocols are enabled through Cargo features on their owning crates.
WireGuard and Amnezia are not currently supported; their future plugin-owned
implementation is tracked in [#115](https://github.com/Kotodian/hammer-ios-rs/issues/115).

## Build And Test

```bash
cargo build --workspace
cargo build --workspace --release
cargo test --workspace
cargo test -p hammer-runtime
cargo fmt --all
cargo fmt --all -- --check
cargo clippy --workspace --all-targets

make build
make build-release
make run
make ctl
make test
make clippy
make fmt
```

## Documentation

- `CONTEXT.md` defines the shared data-plane vocabulary.
- `docs/adr/` records architecture decisions.
- `docs/superpowers/specs/` contains architecture specs.
- `docs/superpowers/plans/` contains dated implementation plans.
- `docs/superpowers/sdd/` contains task execution briefs and reports.

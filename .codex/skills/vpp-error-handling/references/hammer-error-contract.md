# Hammer Error Contract

Use this reference when implementing or reviewing Hammer error handling. It
summarizes the VPP evidence and the current Hammer error map, including legacy
surfaces that must not be extended.

## VPP Reference Model

- `third_party/vpp/src/vlib/error.c` registers per-node error counters and
  writes a `u16` error code onto dropped buffers before routing them to the
  next frame.
- `third_party/vpp/src/vnet/tcp/tcp_input.c` and `tcp_output.c` declare
  `vlib_error_desc_t` arrays from `tcp_error.def` and attach them to the node.
  The packet path increments local counters and stores them once per frame.
- `third_party/vpp/src/vnet/session/session_node.c` declares
  `session_error_counters` for TX, timer, and no-buffer outcomes and increments
  them through `vlib_node_increment_counter`.

The Hammer analog is `BufferNodeError`, `DataPlaneRuntime::record_current_node_error`,
and `GraphRuntime::node_error_count`. Node-local typed enums such as
`TcpNodeError`, `IpInputError`, and `IcmpInputError` provide the codes; graph
nodes choose a drop or continue next arc.

## Hammer Ownership Map

| Crate or boundary | Owned failures |
| --- | --- |
| `hammer-infra` | Main Heap, heap/segment/physmem mapping, FIFO, ring, queue, timer, thread-owned access |
| `hammer-core` | `BufferInvariant`, `DataPlaneError`, frame/index/buffer identity and ownership invariants |
| `hammer-runtime` | graph execution, FileMain, worker/barrier lifecycle, plugin activation, runtime config and lifecycle seams |
| `hammer-service` | interface, device, feature arc, session queue, session state, binary API framing |
| plugin | protocol/device-owned parse, lookup, timer, transport, output, and device failures |
| daemon/CLI/app adapter | process-local presentation, attach, client connection, IPC presentation |

Translate a failure at the real crate, process, or DSO seam that needs a
different owner-local category. Preserve `#[source]` and the failure category.

## Current Code Map

- `crates/hammer-core/src/error.rs`: typed `BufferInvariant` and
  `DataPlaneError` variants with structured identity fields.
- `crates/hammer-runtime/src/error.rs`: typed runtime, worker, File, plugin,
  graph, app-session, and attach variants. It also contains legacy
  `ConfigParse`, `ConfigValidation`, `Lifecycle`, and `Subsystem` variants.
- `crates/hammer-service/src/session/error.rs`: `SessionQueueError` with stable
  `u16` codes and crate-private `SessionError`; both currently convert through
  `RuntimeError::subsystem`.
- `crates/hammer-plugins/transport/tcp/src/protocol/mod.rs`: `TcpError` and
  `TcpControlPacketParseError`; the TCP-to-runtime conversion currently goes
  through `RuntimeError::subsystem`.
- `crates/hammer-plugins/transport/tcp/src/lib.rs`: `TcpNodeError`,
  `TcpOutputError`, and `TcpResetError`; node enums provide `code()` for
  `record_current_node_error`.
- `crates/hammer-plugins/ip/src/ip/icmp.rs`, `local.rs`, and
  `crates/hammer-plugins/ip/src/protocol/ip.rs`: typed node/protocol error
  enums for packet outcomes.

## Packet Path

For a missing TCP connection in `established.rs`, the node records
`TcpNodeError::EstablishedSessionMissing.code()` through
`record_current_node_error` and returns a drop or safe next decision. It does
not return `RuntimeError` for each packet, format a message, or allocate an
error object.

Keep packet-path counters worker-owned and lock-free. The graph runtime owns
error slot encoding and counter publication.

## Control Plane

Recoverable failures use owner-local typed variants. Include the facts needed
to diagnose or retry, such as node, worker, session, path, protocol, slot,
capacity, stage, or requested identity. Do not use `String`-only variants as
the semantic contract.

Do not add new uses of:

```rust
RuntimeError::Subsystem { subsystem, source }
RuntimeError::ConfigValidation { message: String }
RuntimeError::Lifecycle { stage: String, message: String }
```

These are legacy surfaces. If a change must touch them, add typed variants owned
by the seam being fixed or include the cleanup in the approved plan.

## Translation Seams

- Use `#[from]` only when the semantic category is unchanged.
- Construct the destination variant explicitly when categories differ.
- Translate once at a real boundary. Do not wrap the same source in successive
  enums at every caller.
- Do not use `to_string()` or `format!` to carry an error through a seam.
- Preserve `Error::source()` chains and concrete variant fields.

## Failure Atomicity

- Validate every participant before mutating shared state.
- For FIFO protocol layers, commit destination writes before consuming source
  bytes. On error, leave both visible FIFO positions unchanged.
- For transport/timer actions, a failed action must not flush an uncommitted
  batch or leave an invalid ownership transition visible.
- For config and worker initialization, leave the published registry or worker
  state unchanged until construction is complete.
- VPP cleanup is explicit, owner-local, and fallible: `session_free`,
  `session_lookup_del_half_open`, `segment_manager_dealloc_fifos`, and then
  `app_worker_connect_notify` with the concrete error. Do not invent a generic
  cleanup helper.
- Cleanup returns a typed result that preserves both the primary operation
  error and the cleanup error; do not replace the primary error with a log line
  or a successful cleanup result.
- Logging is observability, not error handling. Do not use `tracing::*` or
  `println!` on a recoverable or cleanup path in place of a typed `Result`.

## Panic Boundaries

Programmer bugs and impossible internal states are local assertions or panics
with the violated condition and relevant identities. They are not recoverable
`Result` variants. Never unwind across FFI or plugin ABI boundaries; contain the
panic at the owning runtime boundary.

## Tests

Test the error contract, not the display text:

- Match concrete variants and relevant fields.
- Assert `error.source()` chains at translation seams.
- Assert failure atomicity or retry behavior for fallible mutation.
- Exercise malformed input and resource failures as typed errors.
- Do not create source-text tests that read `.rs` files and use `contains` or
  regexes to claim architectural correctness.

## Review Commands

Use these searches to find legacy or newly introduced error patterns:

```sh
rg -n "Subsystem|ConfigParse|ConfigValidation|Lifecycle \{|TcpError::Dispatch|Internal\(|Other\(|Message\(|Invariant" crates docs -g '*.rs' -g '*.md'
rg -n "tracing::(error|warn|debug|info)|let _ =|\.ok\(\)" crates -g '*.rs'
rg -n "to_string\(\)|format!\(" crates -g '*.rs'
```

When a search hits a legacy surface in a file you are changing, do not leave
the pattern silently extended. Add the typed replacement or call it out as a
blocking cleanup in the plan.

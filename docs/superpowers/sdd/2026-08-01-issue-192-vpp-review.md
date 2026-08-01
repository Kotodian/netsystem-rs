# Issue #192 VPP error-boundary review

## Feature and changed surface

Issue #192 adds `#[runtime_error(subsystem = "...")]` to
`hammer-component-macros`. The attribute generates the existing one-step
conversion from an owner-local typed error into `hammer_runtime::RuntimeError`.
The migration covers every owner-local error that previously constructed the
legacy subsystem carrier directly in Runtime App Session, Service Session and
Binary API, and the TLS, TUN, IP, TCP, and UDP plugins.

The macro validates its arguments, accepts enum and struct error types,
preserves conditional compilation attributes, supports generic error types,
and retains the source value in the runtime error source chain. It does not add
a new error category or move an error to a different owner.

## VPP analog and evidence

- VPP keeps packet-processing failures node-local: `vlib_register_errors` owns
  each node's error range and worker counters in
  `third_party/vpp/src/vlib/error.c:113-195`.
- TCP and Session packet paths increment their node-owned counters directly in
  `third_party/vpp/src/vnet/tcp/tcp_output.c:717-718` and
  `third_party/vpp/src/vnet/session/session_node.c:2157-2160`.
- Recoverable initialization and control operations return their owning
  subsystem's `clib_error_t *`, including TCP initialization in
  `third_party/vpp/src/vnet/tcp/tcp.c:1543-1558` and TLS initialization in
  `third_party/vpp/src/vnet/tls/tls.c:1149-1187`.

Hammer intentionally uses Rust typed owner-local errors and a runtime seam
carrier rather than copying VPP's `clib_error_t`. The relevant VPP alignment is
the separation between node-local packet errors and recoverable control-plane
errors, plus retention of the subsystem that owns the failed operation.

## Findings

Verdict: **Aligned**.

No blocking or non-blocking findings remain.

- Packet-path node error enums, next arcs, and counters are unchanged. The new
  attribute applies only to errors already crossing the runtime/control seam.
- Each migrated source remains typed and owner-local. TLS connection creation
  now adds explicit client/server categories before the runtime translation,
  preserving the rustls source instead of wrapping a raw third-party error at
  the seam.
- A repository-wide comparison against the pre-change branch found 47 direct
  `RuntimeError::subsystem` or `Self::subsystem` constructions. All applicable
  owner-local conversions and call sites were migrated.
- The remaining production constructions are intentional: the generated macro
  implementation, `InterfaceError`'s semantic conversion that passes an
  existing `RuntimeError` through unchanged, and UDP's cross-DSO `RBoxError`
  boundary where the concrete source type is unavailable.
- No graph scheduling, packet data movement, FIFO position, transport state,
  public error variant, lock, atomic, trait object, or crate dependency was
  changed.

## Commands run

- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace`: passed with existing workspace warnings.
- `git diff --check`: passed.
- Focused and workspace tests were not run, per maintainer instruction.

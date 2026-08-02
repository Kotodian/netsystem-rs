# Issue #210 VPP alignment review

## Feature and changed surface

Issue #210 exposes the UDP-owned `UdpLocal` capability through the existing
plugin root. The capability provides `register_dst_port` and
`unregister_dst_port` with an explicit IPv4/IPv6 selector and `NodeId` owner.
UDP allocates the graph-local next slot when a port is first registered, shares
the existing slot for repeated registration by the same owner, and clears only
the port mapping after the final owner release. The UDP plugin retains no
barrier and adds no Binary API, CLI, App API, or synchronization wrapper.

The UDP input snapshot now uses two bitmap-backed port tables. Dispatch keeps a
direct `u16` next-slot array for O(1) packet lookup; owner and reference-count
metadata is sparse and control-plane-only. All worker graph instances retain
the same existing snapshot handle, so a caller's barrier-protected update is
observed by every worker without adding another publication protocol.

The IPv6 checksum fixture calls the existing `internet_checksum_parts` helper;
it does not introduce a second checksum implementation.

## VPP analog and evidence

- `third_party/vpp/src/vnet/udp/udp_local.h:63-68` defines the corresponding
  `udp_register_dst_port` and `udp_unregister_dst_port` operations with an
  address-family selector.
- `third_party/vpp/src/vnet/udp/udp_local.c:447-482` allocates the local next
  slot with `vlib_node_add_next` and publishes the per-family destination-port
  mapping.
- `third_party/vpp/src/vnet/udp/udp_local.c:485-505` unregisters by writing
  `UDP_NO_NODE_SET` while retaining the port-info record and graph next slot.
- `third_party/vpp/src/vnet/udp/udp_local.c:97-100` and `194-201` use separate
  IPv4/IPv6 sparse lookup vectors and perform direct destination-port lookup on
  the packet path.
- `third_party/vpp/src/vnet/udp/udp_local.c:50-88` keeps unknown-port punt,
  drop, and ICMP handling in the UDP local node; Hammer preserves that next-arc
  classification.

Hammer intentionally adds owner/refcount validation because the issue's local
capability has multiple independent consumers. That is control-plane policy;
the packet path still carries only a local `u16` next slot, matching Hammer's
protocol-dispatch contract.

## Findings

Verdict: **Aligned**.

No blocking or non-blocking findings remain.

- `UdpLocal` is an optional prefix-field extension of the existing plugin root;
  the domain names remain `UdpLocal`, `register_dst_port`, and
  `unregister_dst_port`. `UdpLocal_CTO` is only the generated `abi_stable`
  adapter type used at the DSO seam.
- The caller owns worker-barrier/lifecycle synchronization. UDP stores only the
  existing `ArcSwap` snapshot and node identity and does not add a barrier,
  atomic pointer, lock, completion counter, or second publication handle.
- Bitmap tables reduce the fixed action-table footprint while preserving O(1)
  data-plane lookup. Sparse owner metadata is never read by packet dispatch.
- Control failures are typed and owner-local: owner conflict, owner mismatch,
  missing registration, and reference-count exhaustion retain actionable fields.
- `internet_checksum_parts` dispatches to AVX-512, AVX2, or SSE2 on x86-64,
  SIMD on aarch64, and the scalar fallback elsewhere (`crates/hammer-infra/src/checksum.rs:70-177`).

## Commands run

- `cargo fmt --all`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check -p hammer-plugin-udp --all-targets`: passed.
- `cargo test -p hammer-plugin-udp --all-targets`: passed (6 unit tests, 12 integration tests).
- `cargo test -p hammer-runtime --all-targets`: passed (124 unit tests and all runtime integration/bench targets).
- `cargo test -p hammer-plugin-udp --test udp_input_nodes`: passed (12 tests).

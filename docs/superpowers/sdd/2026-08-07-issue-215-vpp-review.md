# Issue 215 VPP Review

## Feature and changed surface

This issue replaces the vendored Quinn `[PacketSpace; 3]` array with three
separately owned packet-space units:

- `Connection.initial: Option<Box<HandshakeSpace>>`
- `Connection.handshake: Option<Box<HandshakeSpace>>`
- `Connection.application: Option<Box<ApplicationSpace>>`

The changed surface is vendored `quinn-proto` only. No `QuicWorker`, Session,
UDP transport, or packet Graph Node ownership is changed.

## VPP analog and evidence

- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:68-89` shows
  `quic_quicly_connection_delete` owns one `quicly_conn_t` and releases it
  with `quicly_free` after stopping the connection TX timer.
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:166-216` shows the
  connection lifecycle state machine that deletes the quicly connection when
  handshake fails or close is confirmed.
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:1608-1628` classifies
  `QUICLY_ERROR_STATE_EXHAUSTION` and `PTLS_ERROR_NO_MEMORY` as fatal,
  transitions the connection to closed, and increments
  `QUIC_ERROR_CLOSED_CONNECTION`; expected packet handling stays inside quicly.
- `third_party/vpp/src/plugins/quic/quic_error.def:26` defines the packet-drop
  counter used for expected drops.

Hammer keeps Quinn's per-space ACK/loss/PTO/ECN semantics but changes the
ownership shape so discarded Initial/Handshake state is physically released by
dropping the box, matching VPP's `quicly_free` ownership boundary.

## Implementation comparison

- Initial and Handshake state are installed as explicit boxes and `discard_space`
  drains in-flight sent records, corrects path accounting, then drops the box.
- Late packets for absent Initial/Handshake spaces fail `unprotect_header` with
  `None`; the caller increments the packet-drop counter and does not recreate
  the space.
- Application state is installed lazily by `init_0rtt` or `upgrade_crypto(Data)`
  and survives Initial/Handshake discard and key update.
- Key update state (`key_phase`, `key_phase_size`, `prev_crypto`, `next_crypto`,
  packet-number filter) moved into `ApplicationSpace`.
- `largest_acked_packet_sent` is removed; RTT and ECN use the acknowledged
  `SentPacket::time_sent` directly.
- No `PacketSpace`, `ThinRetransmits`, universal `Retransmits`, or
  `Index<SpaceId>` compatibility layer remains.

## Error handling

- Expected packet drops are counted through `udp_rx.on_packet_drop()` and are
  not returned as control-plane errors.
- Fatal protocol/resource failures remain typed `TransportError` results or
  connection close state.
- Missing required active spaces are internal invariant violations asserted by
  `expect`, not late-packet outcomes.
- No new log-only error handling is added by this diff.

## Verdict

`Aligned` for ownership, lifecycle, and error handling.

## Remaining measurement note

Issue #233 remains open, so the final Main Heap residency measurement and batch
established-idle-connection residency report are still blocked by that issue.
The Quinn state and deterministic protocol work is otherwise complete.

## Commands run

- `cargo check --workspace`
- `cargo check -p quinn-proto --no-default-features`
- `cargo check -p quinn-proto --tests --no-default-features`
- `cargo fmt --all -- --check`
- `git diff --check`

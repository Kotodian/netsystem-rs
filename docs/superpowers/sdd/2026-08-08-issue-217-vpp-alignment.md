# Issue 217 VPP Alignment Review

## Feature and changed surface

This review covers the QUIC Initial acceptance path and the post-handshake
Connection Session lifecycle:

- `hammer-service` gains transport-backed upper Session creation and connected
  publication so a QUIC Connection Session is a real `SessionType::Transport`.
- `hammer-plugin-quic` keeps the lower UDP Session FIFO RX path but classifies
  packet drops as worker-local counters instead of returning control-plane
  errors for ordinary malformed or unsupported input.
- QUIC lower Session App close/reset/cleanup callbacks now close the owning
  Connection Context and notify the upper Connection Session.

## VPP analog and evidence

- VPP QUIC registers `quic_udp_session_rx_callback` as the lower UDP Session
  App RX callback: `third_party/vpp/src/plugins/quic/quic.c:633-636`.
- The quicly implementation consumes the UDP Session RX FIFO in
  `quic_quicly_udp_session_rx_packets`:
  `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:1767-1880`.
- VPP classifies packet drops with `QUIC_ERROR_PACKET_DROP`:
  `third_party/vpp/src/plugins/quic/quic_error.def:26`.
- VPP separates `udp_session_handle` from the app-facing `c_s_index` in
  `quic_ctx_t`: `third_party/vpp/src/plugins/quic/quic.h:218-250`.
- VPP creates the app-facing QUIC Connection Session only after handshake
  success: `quic_quicly_notify_app_connected` at
  `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:93-162`.

## Changed Hammer surfaces

- `SessionWorker::create_upper_transport_session`
- `SessionWorker::publish_connected_transport_session`
- QUIC `Context::lower_session` and `Context::transport_session`
- QUIC `ConnectionContext::connection_session`
- QUIC `QuicWorker::close_connection`
- QUIC worker-local `rx_datagram_drops` and `rx_packet_drops`

## Findings

### Blocking

None after this review.

### Non-blocking

- The QUIC drop counters are worker-local. They match VPP's counter model in
  spirit, but are not yet exposed through a Session/node counter API.
- The existing per-connection `StreamIoTable` still uses `HashMap`; this was
  present before issue #217 and is outside the Connection Session lifecycle
  fix, but it should be audited before the QUIC feature is declared final.
- The oversized-datagram test covers consumption; executable coverage for the
  handshake path and malformed packet path is part of the final pre-commit
  test gate.

## Verdict

`Aligned`.

Final gate:

- `cargo clippy -p hammer-service -p hammer-plugin-quic --all-targets`
- `cargo test -p hammer-service -p hammer-plugin-quic`
- `cargo fmt --all -- --check`
- `git diff --check`

The final test gate passed with 20 QUIC plugin unit tests, 63 hammer-service
unit tests, and the hammer-service integration suites.

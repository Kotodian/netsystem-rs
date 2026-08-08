# Issue 218 VPP Alignment Review

## Feature and changed surface

Issue 218 completes QUIC TLS 1.3 handshakes through the existing lower UDP
Session FIFO and `SessionTransport` dispatch. The app-facing QUIC Connection
Session is created only after Quinn reports `Event::Connected`; active failure
completes the outer Application Connection with a concrete category, while
passive failure only cleans up the lower Session and QUIC context.

Changed surface:

- `hammer-runtime::app::ApplicationSessionReply` and
  `ApplicationSessionStatus`
- `hammer-runtime::attach` publication/completion path
- `hammer-service::session` connect/accept completion methods and
  `SessionConnectError`
- `hammer-plugin-quic` ConnectionContext/EngineConnection identity, handshake
  event handling, and timer cleanup
- `hammer-app` request path and interleaved completion buffering

## VPP analog and evidence

- `third_party/vpp/src/vnet/session/application_worker.c:620-630`
  `app_worker_connect_notify` is the single active-connect completion carrier:
  success has a Session, failure has none, and the app opaque is retained.
- `third_party/vpp/src/vnet/session/session.c:748-800`
  `session_stream_connect_notify` cleans up the half-open entry, then either
  notifies failure with no Session or allocates/initializes/publishes the
  Session before notifying success.
- `third_party/vpp/src/plugins/quic/quic.c:78-112` stores
  `ctx->client_opaque = sep->opaque` and uses the lower UDP
  `api_context = ctx_index`.
- `third_party/vpp/src/plugins/quic/quic.c:438-480`
  `quic_udp_session_connected_callback` does not publish the app Session; it
  only attaches the lower UDP Session and initializes the QUIC engine.
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:93-162`
  `quic_quicly_notify_app_connected` allocates the app Session only on success.
  Failure notifies the active App with no Session.
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:170-210`
  handshake-state connection close notifies only an active client with
  `SESSION_E_TLS_HANDSHAKE`.
- `third_party/vpp/src/vnet/session/session_api.c:367-425`
  `session_connected_msg_t` carries context, status, and success-only Session
  facts. Hammer keeps descriptors on the existing App Session publication path
  and writes the tagged completion on the existing CTRL reply queue.

## Hammer mapping

- `ApplicationConnectionId` is the Hammer equivalent of VPP `api_context` /
  `client_opaque`.
- `ConnectionContext::EngineConnection::client_opaque` retains the outer active
  Application Connection through the handshake. It is not `app_opaque`, which
  remains the application-supplied Stream Session opaque.
- `SessionWorker::stream_connect_pending` plus
  `complete_stream_connect` correspond to `session_stream_connect_notify`:
  construct from the outer Application Connection, publish, write the
  connected completion, then complete the pending identity.
- `SessionWorker::stream_connect_failed` corresponds to
  `app_worker_connect_notify(s, 0, err, opaque)`: publish a failure completion
  without constructing a Session and complete the pending identity only after
  publication is accepted.
- `QuicTimerKind::Handshake` replaces `Accept` and covers the active/passive
  handshake deadline. Active expiry reports `TimedOut` per issue #218; passive
  expiry only cleans up.

## Findings

### Non-blocking: Hammer keeps an immediate connect `Response`

VPP does not have a separate immediate connect response with a pending handle;
the connected message is the completion. Hammer's issue #218 protocol keeps
`Response` for the existing synchronous AppClient request path and returns the
pending `ApplicationConnectionId`. This is an accepted protocol adaptation from
the issue spec, not a VPP API copy.

### Non-blocking: VPP accept timer does not notify the App

VPP `quic_accept_timer_expired`
(`third_party/vpp/src/plugins/quic/quic.c:794-800`) disconnects the lower UDP
transport without sending `SESSION_E_TIMEDOUT`. Issue #218 explicitly requires
active handshake timeout to complete the outer Application Connection with
`TimedOut`; Hammer follows the issue requirement.

## Verdict

`Aligned`. The final focused tests and pre-commit gate pass; the passive
listener test uses a real `SessionMain::listen -> stream_accept` path, and the
worker retains the outer listener identity instead of consulting QUIC Main
after the handshake.

## Commands run

- `cargo fmt --all`
- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets`
- `cargo check -p hammer-plugin-quic --all-targets`
- `cargo test -p hammer-runtime`
- `cargo test -p hammer-service`
- `cargo test -p hammer-app`
- `cargo test -p hammer-plugin-quic --lib -- --nocapture` (21 passed)
- `cargo clippy --workspace --all-targets`
- `git diff --check`

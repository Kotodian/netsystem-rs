# Issue 218: VPP-aligned QUIC handshake completion

## Result

#218 completes QUIC TLS 1.3 handshakes through the existing lower UDP Session
FIFO and Session Transport dispatch. A QUIC Connection Session is created only
after Quinn reports `Event::Connected`. Active failure completes the pending
Application Connection with a concrete category; passive failure publishes no
Application Session.

Hammer does not add a Graph Node, another `SessionEvtType`, another completion
queue, a QUIC-specific Session constructor, or a QUIC-specific runtime API.

## VPP reference

- `quic_udp_session_connected_callback` retains the lower UDP Session but does
  not publish the app-facing QUIC Session.
- `quic_quicly_notify_app_connected` allocates and initializes the app Session
  only after handshake success, then calls `app_worker_connect_notify` with the
  client opaque.
- Handshake failure calls `app_worker_connect_notify` with no Session and the
  same client opaque; server failure has no app notification.
- `app_worker_connect_notify` emits `SESSION_CTRL_EVT_CONNECTED`; the external
  app adapter turns it into `session_connected_msg_t` containing context,
  status, and success-only Session/FIFO facts.
- One UDP FIFO datagram may contain coalesced QUIC packets. VPP consumes that
  datagram as one record while decoding each contained packet.

Primary sources:

- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:93-162`
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:170-207`
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:1356-1370`
- `third_party/vpp/src/vnet/session/application_worker.c:620-630`
- `third_party/vpp/src/vnet/session/session_api.c:367-425`

## Existing surface audit

### Reuse unchanged

- `SessionDgramHeader`, `Fifo::reserve_write`, and Session FIFO notification
  already provide the lower UDP datagram record seam.
- `QuicWorker::process_udp_rx` peeks one complete record, lets Quinn process the
  complete datagram, and consumes the record once. Quinn's descriptor-aware
  datagram API handles coalesced packets before Hammer drains connection events,
  so handshake completion affects only a later FIFO datagram.
- `QuicWorker::schedule_connection_outputs` commits one header plus payload in a
  single FIFO reservation. Generic UDP output remains outside #218.
- `ConnectionContext` already keeps lower UDP Session, QUIC context, and upper
  Connection Session identities distinct.
- `SessionWorker::stream_connect` and `SessionWorker::stream_accept` already
  construct a transport Session from the correct outer Application Connection
  or Application Listener facts.
- `SessionWorker::connection_published`, `SessionWorker::connected`, and the
  existing App Session publication path already provide the publication steps.
- Client and server config construction already forces rustls early data off.

### Insufficient as currently composed

- `SessionMain::application_connect` reclaims the outer
  `ApplicationConnectionId` as soon as the QUIC connect callback returns. At
  that point only lower UDP setup has completed.
- Active `ConnectionContext` does not retain the outer Session Connection
  identity needed for final success or failure.
- `Event::ConnectionLost { reason }` discards `reason`; the handshake timer also
  removes the context without completing the active Application Connection.
- `create_upper_transport_session` derives Application facts from the lower UDP
  Session. For QUIC that lower Session belongs to the built-in QUIC Application,
  not the outer client or listener. It is not the correct VPP publication path.
- `ApplicationSessionReply` represents only immediate request replies. Its
  untagged `context/status/handle` layout cannot safely interleave an async
  connected completion with later listen/connect/unlisten replies.
- `SessionEvt` carries Session identity. Before handshake success there is no
  Session identity, and its layout has no connect context or error category.

## Connection identity and lifecycle

Follow VPP's existing context fields instead of introducing an origin type.
VPP sets `listener_ctx_id` to `QUIC_CTX_INVALID_INDEX` for an active connect,
stores the caller correlation in `client_opaque`, and stores a real
`listener_ctx_id` for an accepted connection. Hammer keeps the existing
`ConnectionContext::listener: Option<ContextId>` with the same semantics and
adds only `application_connection: Option<SessionConnectionId>` for the outer
active Application Connection. The existing active/passive constructors set
the two fields consistently; no plugin-specific identity alias or wrapper is
introduced.

Active connect proceeds as follows:

1. Main Thread registers the outer Application Connection and passes its
   existing `SessionConnectionId` into QUIC.
2. QUIC creates a separate built-in Application Connection for the immediate
   lower UDP Session. That inner identity may be reclaimed after UDP completes.
3. The outer identity remains in `application_connection` while TLS runs on the
   Data Worker; `listener` remains `None`, matching VPP's invalid listener id.
4. On `Event::Connected`, QUIC calls the existing `SessionWorker::stream_connect`
   with the outer identity, then performs the existing publication steps.
5. The App Session publication is tagged as an active-connect success. The
   existing attach publisher sends descriptors and then emits the connected
   variant on the existing Application CTRL reply queue.
6. The Application Connection becomes completed only after its publication has
   been accepted. Completed entries are reclaimed under the Main Thread worker
   barrier before later connection allocation, and all remaining entries are
   removed on Application detach.

Passive connect uses `SessionWorker::stream_accept` with the outer listener from
the QUIC listener context. A passive handshake failure closes the lower UDP
Session and removes QUIC state without publishing a Session or completion.

## Application control protocol

Keep `SessionEvtType::Connect` unchanged. Change the existing
`ApplicationSessionReply` from one untagged record into a tagged protocol with
these legal states:

```rust
pub enum ApplicationSessionReply {
    Response {
        context: u64,
        status: ApplicationSessionStatus,
        handle: u64,
    },
    Connected {
        connection: ApplicationConnectionId,
        session: SessionHandle,
    },
    ConnectFailed {
        connection: ApplicationConnectionId,
        status: ApplicationSessionStatus,
    },
}
```

This reuses the existing type, serializer, CTRL queue, and wait signal. It makes
success-without-Session and failure-with-Session unrepresentable. The App SDK
buffers interleaved `Response` and connected variants by their typed identity.
`AppClient::connect` continues to return the pending `ApplicationConnectionId`;
an SDK `wait_connection(ApplicationConnectionId)` may synchronously wait for the
VPP-shaped asynchronous completion and correlate the existing descriptor
publication by `SessionHandle`.

The tagged representation is wire-incompatible with attach protocol version 2.
The daemon and `hammer-app` therefore bump `ATTACH_PROTOCOL_VERSION` together;
an old client is rejected during attach instead of decoding a completion as an
immediate reply.

`ApplicationSessionStatus` remains the serialized status vocabulary and gains
specific final-connect categories rather than another public error carrier:

- TLS alert, retaining the alert byte;
- QUIC version unsupported;
- handshake timeout;
- connection refused or reset;
- peer close, retaining its numeric code;
- QUIC transport/protocol error, retaining its numeric code;
- local connection resource exhaustion.

Immediate setup rejection remains distinct from final connect failure.
`TransportConnectFailed` must not be used for a completed TLS handshake attempt.
Retry is not a completion category: a valid QUIC Retry updates Quinn state and
the same pending Application Connection continues.

## Session error seam

Add one owner-local `hammer-service::session::SessionConnectError` for the
generic final-connect categories above. It is required because the existing
`ApplicationSessionStatus` is a serialized App boundary status and
`QuicWorkerError` is plugin-owned; neither may become the other's internal
domain model.

Add `SessionWorker::stream_connect_failed(SessionConnectionId,
SessionConnectError)`. It publishes a failure completion without constructing a
Session, marks the Application Connection completed only after publication is
accepted, and leaves Session construction tables unchanged.

QUIC retains the concrete `quinn_proto::ConnectionError`, classifies it at the
plugin boundary, and translates once into `SessionConnectError`. TLS crypto
transport codes retain their TLS alert byte. Display text and `to_string()` are
not part of classification.

The existing `SessionWorker::stream_connect` should be deepened to perform the
VPP `session_stream_connect_notify` success transaction: construct from the
outer Application Connection, publish, enqueue the connected completion, then
complete the pending identity. TCP and UDP are its only existing callers and
should use the same completed operation instead of repeating
`connection_published`/`connected` choreography.

## QUIC event and cleanup rules

- Rename `QuicTimerKind::Accept` to `Handshake`; it covers both client and
  server handshake deadlines.
- `Event::Connected` is accepted only from `Handshaking`. It creates exactly one
  upper Session through the origin-specific Session operation and transitions
  to `Established` only after publication succeeds.
- `Event::ConnectionLost { reason }` while active and handshaking classifies and
  publishes failure before removing the context. The same event while passive
  and handshaking performs protocol-local cleanup only.
- Handshake timer expiry uses the same active/passive split and reports
  `SessionConnectError::TimedOut` for active connect.
- Cleanup stops the exact Handshake and Transmit timer tokens, detaches the
  lower Session App context, schedules lower UDP disconnect, clears Quinn
  state, and removes the QUIC context.
- Failure publication backpressure must not be discarded. Until publication is
  accepted, the context and pending Application Connection remain available for
  retry; cleanup does not turn a full CTRL/publication queue into an invisible
  connect failure.
- 0-RTT never invokes either `stream_connect` or `stream_accept` and never tags
  an App Session publication.

## Layer isolation contract

- `app-session/quic` may call Quinn, adjacent lower UDP Session FIFO APIs,
  origin-specific Session connect/accept operations, exact timer operations,
  and transport-neutral Session cleanup. It must not access App descriptors,
  serialize control messages, register Graph Nodes, or call generic UDP buffer
  output directly.
- `hammer-service::session` owns pending Application Connection lookup, Session
  construction/publication order, final-connect error translation into the App
  status vocabulary, and failure atomicity. It must not inspect Quinn errors or
  QUIC connection state.
- `hammer-runtime::app` owns the tagged serialized reply protocol, Session
  handle/layout facts, and existing CTRL queue mechanics. It must not define
  QUIC-specific status or context types.
- `hammer-runtime::attach` owns descriptor delivery and ordering with the
  connected CTRL message. It must not decide whether a QUIC handshake
  succeeded.
- `hammer-app` owns buffering interleaved replies/completions and mapping the
  successful Session handle to the already-existing App Session publication.

## Proposed interface approval record

The implementation requires explicit approval for these non-trivial changes:

1. Owner-local public `SessionConnectError` and
   `SessionWorker::stream_connect_failed`. Existing wire status and
   plugin-owned Quinn errors cannot model the Session-owned translation seam.
2. Change existing `ApplicationSessionReply` into the tagged variants above and
   add concrete final-connect `ApplicationSessionStatus` variants. The current
   record cannot distinguish immediate replies from asynchronous completions.
3. Deepen existing `SessionWorker::stream_connect` to own the complete VPP
   success transaction. This replaces duplicated transport choreography; it
   does not add a second success constructor.
4. Tag the existing App Session publication with an optional active Application
   Connection and let the existing attach publisher emit the final CTRL reply.
   This is needed because FIFO descriptors cannot be carried inside Hammer's
   shared-memory CTRL element as VPP segment handles can.

## Verification

Final pre-commit verification, after implementation and review:

```bash
cargo fmt --all -- --check
cargo test -p hammer-runtime --test session_msg_queue
cargo test -p hammer-service
cargo test -p hammer-app
cargo test -p hammer-plugin-quic
cargo check --workspace
cargo clippy --workspace --all-targets
```

Focused QUIC tests must cover deterministic client/server handshake, exact
datagram-record TX, coalesced RX consumption, single upper Session publication,
outer Application identity, TLS alert preservation, version mismatch, Retry,
timeout, cleanup of both Session/context/timers, and 0-RTT non-publication.
Tests match concrete variants and fields; they do not assert display strings or
source text.

# QUIC Streams bind one-to-one to Transport Sessions

Status: superseded by ADR 0038

`app-session/quic` owns UDP-facing QUIC Connections and the private QUIC send, receive, flow-control, reset, and close state backing every stream. Each QUIC Stream is represented at the Session layer by exactly one Session, just as each TCP Connection is represented by one Session. The Session's opaque Transport Index addresses the exact private stream state needed by the QUIC driver; that state references its parent `QuicConnection` and is not a second app-facing stream identity. TCP uses the same one-Session-to-one-transport-state contract, with its Transport Index addressing `TcpConnection`. When the application selects no upper protocol, the same Session is published directly as one AppSession and uses its existing RX and TX FIFOs.

This follows VPP's single `quic_ctx_t` pool with one context per connection and one per stream. Hammer uses one worker-owned `Pool<QuicCtx>` whose Rust enum distinguishes connection and stream contexts. The Session connection index addresses the stream context, while `quic_connection_ctx_id` inside that context addresses the parent connection context in the same pool. Session Queue resolves an `RxDeq` event to that exact Session before invoking the VPP-named transport `app_rx_evt`; the App never calls QUIC directly.

Hammer does not provide an `app-session/quic-stream` plugin because a raw QUIC Stream is already the byte-stream service represented by the Session; an identity-only protocol layer would add a FIFO pair and payload transfer without adding protocol semantics. Optional upper protocols such as HTTP/3 are selected and constructed by Session above those lower Sessions. `app-session/quic` neither calls those protocols nor selects their policy.

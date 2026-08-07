# HTTP/3 aggregates QUIC Streams by connection

Status: accepted

`app-session/http3` owns one worker-local HTTP/3 Connection across the request, control, QPACK encoder, and QPACK decoder streams carried by one QUIC Connection. Each lower Session represents one QUIC Stream. An HTTP/3 request stream transforms between that Session's FIFO pair and one upper AppSession FIFO pair, while HTTP/3 internal streams remain attached only to the HTTP/3 Connection and do not create application-visible AppSessions.

`app-session/quic` owns QUIC stream creation, reset, close, and the private stream state backing each corresponding Session. Its `SessionTransport` implementation can map each exact stream Transport Index to the parent `QuicConnection` index. Session consumes this lower topology fact and applies the independently selected App Session protocol policy. The QUIC driver never selects, creates, invokes, or destroys HTTP/3 state. Session owns each corresponding Session lifecycle, FIFO event routing, and application publication, but owns no QUIC Connection Session or lookup table. `app-session/http3` never calls `app-session/quic`, selects a transport, or receives mutable QUIC state.

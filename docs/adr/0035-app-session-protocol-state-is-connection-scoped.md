# App Session protocol state is connection-scoped

Status: superseded by ADR-0037

`AppSessionProtocol::Self` is one worker-owned protocol connection rather than one fixed lower-FIFO/upper-FIFO pair. One `AppSessionProtocolConnectionId` may associate multiple lower and upper Sessions: TLS and HTTP/1 are naturally one-to-one, HTTP/2 may associate one lower Session with multiple upper Sessions, and HTTP/3 may associate multiple lower QUIC Stream Sessions with only its application-visible upper request Sessions. Protocol stream and control state remains private to the concrete connection. The lower Transport Connection remains owned below Session and is not represented as a Session.

Hammer retains one `AppSessionProtocol` trait, one plugin registration path, and one Application policy selection path. Session owns Session and FIFO allocation, the Session lifecycle for each transport-owned stream, event routing, and AppSession publication; the protocol connection decides only its protocol relationships and byte transformations. No sibling multiplexer trait, topology mode, or public protocol Stream type is introduced.

Every Session has one exact Transport Index used for `SessionTransport` dispatch. TCP uses it to address `TcpConnection`; QUIC uses it to address the protocol-private stream state for that Session, which in turn references its parent `QuicConnection`. The lower driver may expose that parent connection index as a transport topology fact, but it never receives a `SessionId`, calls an App Session protocol, or manages upper-layer state. Session decides how the lower topology participates in its independently selected App Session protocol policy.

Every Transport Session RX, TX, close, reset, and cleanup event continues to target the exact Session; neither Session nor the protocol connection scans a connection root or recursively traverses a protocol chain to discover work. This lower transport topology relationship does not make a QUIC Connection a Session or transfer TCP/QUIC connection and stream ownership out of their transport plugins.

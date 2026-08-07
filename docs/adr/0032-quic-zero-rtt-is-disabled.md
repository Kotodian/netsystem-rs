# QUIC 0-RTT is disabled

Status: accepted

Hammer does not send, accept, create AppSessions for, or deliver QUIC 0-RTT Stream data. A QUIC Connection must complete its TLS 1.3 handshake before either peer-opened or locally opened QUIC Streams are published as AppSessions. This keeps replayable early data outside the application contract and preserves the boundary in which `quic-handshake` owns the complete handshake lifecycle while `quic-recv-process` owns application Stream delivery; enabling 0-RTT later requires a separate application opt-in and replay-policy decision.

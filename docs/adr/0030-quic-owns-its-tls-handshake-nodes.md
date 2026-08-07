# QUIC owns its TLS handshake nodes

Status: accepted

All QUIC Graph Nodes, including any node that advances the TLS 1.3 handshake, live in the QUIC transport plugin and operate on the same Data Worker-owned QUIC Connection state. The QUIC plugin uses concrete `rustls::quic` state through its vendored `quinn-proto` engine, while the TLS plugin remains an AppSession protocol for TLS records over adjacent FIFOs and exposes no QUIC state or node. Splitting QUIC handshake progress across the TLS and QUIC plugins is rejected because QUIC CRYPTO-frame reassembly, encryption levels, packet protection keys, loss recovery, timers, and connection lifecycle cannot be isolated at that plugin boundary without duplicating ownership or adding cross-plugin coordination to the packet path.

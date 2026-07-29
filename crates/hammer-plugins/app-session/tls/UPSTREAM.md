# Upstream provenance

The Hammer TLS plugin protocol core was designed against TLS 1.3 and the
deterministic traces in RFC 8448. The state-machine and wire-codec organization
was selectively informed by rustls 0.23.31, particularly its TLS 1.3 handshake
and message modules.

Hammer does not include rustls as a dependency and does not copy its
`CryptoProvider`, record buffering, connection types, or payload ownership.
Cryptographic work is performed through `hammer-service` Crypto Engine
capabilities, and wire payloads remain in caller-owned memory.

rustls 0.23.31 is distributed under `Apache-2.0 OR ISC OR MIT`. The ISC notice
applicable to the referenced upstream work is retained in
`LICENSE-rustls-ISC`.

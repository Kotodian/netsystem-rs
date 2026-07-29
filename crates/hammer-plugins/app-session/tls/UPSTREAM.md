# Upstream dependency

The Hammer TLS plugin directly depends on rustls 0.23.31. rustls owns the TLS
state machine, transcript, key schedule, authentication, record buffering, and
record protection. Hammer supplies adjacent App Session FIFOs through standard
nonblocking `Read`, `BufRead`, and `Write` implementations and does not maintain
a second TLS or cryptographic implementation.

rustls 0.23.31 is distributed under `Apache-2.0 OR ISC OR MIT`. The ISC notice
applicable to the referenced upstream work is retained in
`LICENSE-rustls-ISC`.

# Session Transport Stage 5 Review

## Feature and Changed Surface

Issue #272 stage 5 closes the Transport registration migration across
`hammer-service`, `hammer-runtime`, and the built-in transport/application
plugins. Tests now obtain Transport identities from registration instead of
assuming a fixed registry slot.

## VPP Analog and Evidence

- `third_party/vpp/src/vnet/session/transport.c` owns the process-global
  `tp_vfts` vector and publishes entries through
  `transport_register_new_protocol`.
- `third_party/vpp/src/vnet/session/transport.c` implements
  `transport_protocol_get_vft` as an index lookup into that vector.
- `third_party/vpp/src/vnet/session/transport.h` dispatches listen/connect
  operations through the selected protocol entry.

Hammer keeps the VFT authority separate from `TransportMain`, appends entries
through `register_transport`, and returns the slot used by Session control
messages and plugin-owned Main state.

## Verdict

Complete for stage 5.

The four concrete Main initializers register their owner-defined concrete
callbacks directly into the process-global VFT authority. The Session runtime
stores only the returned numeric slot, and workers carry that slot for exact
protocol dispatch. Production and test callers of `SessionMain::listen`,
`connect`, and `connect_stream` pass registration-returned slots; no caller
passes a VFT value into Session runtime APIs.

## Findings

### Non-blocking: repository context files unavailable

The requested root `CONTEXT.md`, `docs/adr/`, and issue-tracker guide are not
present in this checkout. The review therefore relies on `AGENTS.md`, issue
#272's available body, the current diff, and vendored VPP sources. This does
not change the implementation contract, but historical ADR validation remains
outside this checkout.

### Resolved: fixed Transport slots in workers

Concrete TCP, UDP, and QUIC workers no longer declare or use fixed `ID`
constants. Their protocol fact is supplied by the owner Main at worker
installation, and test workers use a slot returned by test registration.
Session queue dispatch compares the Session protocol against
`transport.protocol()`.

### Resolved: deleted registration surface

The old `SessionTransportStartListen`, `SessionTransportStopListen`,
`SessionTransportConnect`, `SessionTransportConnectStream`, and
`SessionTransportRegistration` surfaces are absent from Rust sources. The
Session runtime now exposes the connection index and a narrow ownership check;
it no longer returns `(protocol, index)` through `session_transport`.

### Resolved: owner registration and slot propagation

TCP, UDP, QUIC, and HTTP each call `register_transport` from their own ordered
Main initializer with concrete owner callbacks. The returned slot is stored in
the concrete Main and passed into each worker; Session dispatch compares that
slot through `SessionTransport::protocol`. No central protocol enum, fixed
transport ID, or dynamic dispatch is involved.

### Resolved: VFT storage boundary

`transport_vft(protocol)` is used only inside the service Session control/runtime
authority. QUIC and HTTP obtain dependency identities from the owning plugin's
`protocol()` API and do not inspect the VFT table. All owner and test
registrations use `TransportVft::new(...)`; the VFT fields remain internal to
the service crate.

## Commands Run

- `rg` audits for fixed Transport slots and deleted registration symbols.
- `git diff --check`.
- No build or test command yet; repository rules reserve those commands for
  the final pre-commit gate after formatting and review are complete.

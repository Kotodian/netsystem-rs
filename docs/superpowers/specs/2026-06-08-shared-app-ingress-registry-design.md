# Shared App Ingress Registry Design

> **Container choices superseded by Issue #104 and ADR 0014.** `FlatHashTable` and `hammer_infra::vec::Vec` no longer exist; ordinary storage uses standard collections through the Main Heap, while packet-path exact lookup uses an existing VPP-style dataplane primitive.

## Goal

Make TCP and UDP reuse the same app-ingress registration structure inside `hammer-service`.

This specifically means:

- no TCP-private `TcpAppBridgeTable`
- no UDP-private app target table with a different storage shape
- transport code provides only its lookup key and classification
- app target storage and lookup live in `hammer-service::app`

## Design

### Shared registry

Add a generic registry in `crates/hammer-service/src/app/registry.rs`:

- `AppIngressRegistry<K>`

Where:

- `K: hammer_infra::map::FlatHashKey`
- values are stored as `AppIngressTarget`

Internal storage uses:

- `FlatHashTable<K, u32>` for key-to-slot lookup
- `hammer_infra::vec::Vec<AppIngressTarget>` for contiguous target storage

This keeps the data-plane-facing shape uniform across transports and avoids transport-private target maps.

### TCP usage

TCP uses:

- `AppIngressRegistry<TcpLookupId>`

`tcp/rcv_process.rs` keeps the pending handoff mechanism for now, but once it resolves `TcpLookupId`, it looks up the shared registry and delivers through the common app backend.

### UDP usage

UDP uses:

- `AppIngressRegistry<u16>`

`udp/input.rs` may still keep a fast per-port action classification, but the app target itself must come from the shared registry rather than being embedded in a UDP-private action payload.

### Shared delivery path

Both transports continue to share:

- `ServiceAppBackend`
- `deliver_buffer_to_app(...)`

Only the transport-specific key lookup stays in transport code.

## Non-Goals

- Do not solve cross-worker zero-copy handoff here.
- Do not move pending TCP app-bridge state onto packet metadata in this change.
- Do not introduce control-plane `bind/listen/accept` wrappers here.
- Do not change `hammer-app` API in this step.

## Migration Plan

1. Add `AppIngressRegistry<K>` in `hammer-service::app`.
2. Convert TCP receive-side app target storage to the shared registry.
3. Convert UDP registered-app target storage to the shared registry.
4. Keep focused TCP/UDP app seam tests green.

## Acceptance Criteria

- `TcpAppBridgeTable` no longer exists.
- TCP and UDP both reference the same generic app-ingress registry type.
- TCP and UDP app delivery tests continue to pass.
- No new `std::collections` are introduced on the data-plane-facing path.

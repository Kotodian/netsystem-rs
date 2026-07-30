---
name: fifo-layer-isolation
description: Audit or implement Hammer app-session protocol layers that transform adjacent FIFOs. Use for TLS, HTTP, plaintext, FIFO notifications, protocol ingress/egress, backpressure, and protocol lifecycle changes.
---

# FIFO Layer Isolation

Model a protocol layer as exactly one lower and one upper `AppSession` FIFO
pair. The protocol may borrow only those adjacent FIFOs and its worker-owned
connection state.

For ingress, consume lower RX only after committing bytes to upper RX. For
egress, consume upper TX only after committing bytes to lower TX. On error,
leave both visible FIFO positions unchanged. Do not allocate payload `Vec`s,
copy through a stack buffer, inspect another layer, receive an entire
`AppSession`, or schedule graph nodes from protocol code.

Use FIFO notification transitions as the only forwarding trigger:

- RX enqueue targets the session that consumes that RX FIFO.
- TX enqueue targets the session that consumes that TX FIFO.
- RX/TX dequeue notifications retry only the adjacent producer that was
  blocked by capacity.

Do not recursively drain a chain and do not create per-layer work queues. A
single worker-owned session event queue may carry the target session identity.
Session, not the protocol, turns typed protocol facts such as upper-ready or
close into policy decisions.

Test partial input, full destination FIFO, notification coalescing, no-copy
FIFO transfer, error atomicity, and reverse-order teardown.

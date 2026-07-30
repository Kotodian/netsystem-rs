---
name: vpp-session-event-alignment
description: Align Hammer VPP-style session, TLS, transport, and FIFO event changes with vendored VPP. Use when changing SessionEvt delivery, session callbacks, app-session protocols, TLS accept/connect, listener behavior, or worker scheduling.
---

# VPP Session Event Alignment

Read `third_party/vpp/` before proposing a session execution model. Start with
`src/vnet/session/` and the relevant protocol plugin, such as `src/vnet/tls/`.

Treat a VPP event's `session_index` as the identity of its exact target
session. Do not turn an event for one session into a root-session scan or a
protocol-chain traversal.

Keep the correspondence explicit:

- A transport owns a transport connection and retains only its opaque Session
  listener/session identity.
- Session owns listener/connect policy, layer ordering, event routing, and app
  publication.
- A protocol operates on the target session and its adjacent FIFOs. It reports
  protocol facts; Session applies policy and schedules follow-up work.
- An external app is notified only at the app-facing session selected by
  Session policy.

For every change, state the VPP call site, the target session identity, the
owning worker, and the FIFO event that triggers the next transition. Reject
generic root-chain `advance`, stack scans, plaintext fallback accepts, and
transport access to app protocol/configuration.

Verify with focused event-order tests plus `cargo fmt --all -- --check` and
`git diff --check`. Do not use source-text assertions as behavioral proof.

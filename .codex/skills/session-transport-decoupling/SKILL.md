---
name: session-transport-decoupling
description: Keep Hammer application policy, Session scheduling, and transport plugins separate. Use when changing attach, listen, connect, SessionListenerId, TCP plugin registration, AppWorker ownership, or protocol policy/configuration.
---

# Session Transport Decoupling

Apply these ownership rules:

- `attach` establishes app/shared-memory/message-queue identity only.
- `listen` and `connect` select the app-session protocol policy and the
  transport independently.
- Session owns the selected protocol policy, protocol connection lifecycle,
  FIFO/event routing, and transport scheduling policy.
- AppWorker owns only app-facing publication, shared-memory allocation, and
  external app event exchange. It must not retain a transport session.
- TCP and other transport plugins own their worker-local transport connections
  and retain only opaque `SessionListenerId`/`SessionId` values.

Keep TCP's normal path transport-neutral: Session owns TX FIFO retention;
transport reads the bottom session FIFO and prepends its headers. Do not put
TLS config, app identity, protocol policy, or app FIFO traversal in a
transport plugin.

Before adding a type or API, identify its owner and prove an existing Session,
AppSession, `SessionHandle`, `SessionId`, or generic infra primitive cannot
express the fact. New non-trivial VPP/TCP surfaces require explicit approval.

Verify listener acceptance through the transport registration, connect policy
selection, app detach cleanup, and that no app-owned structure contains a
transport session.

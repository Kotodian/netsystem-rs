# Transport-Owned Timer Policy Design

Source PRD: https://github.com/Kotodian/hammer-ios-rs/issues/41

## Problem Statement

Hammer's generic Session Runtime still owns and exposes transport timer machinery. It advances a shared timer wheel, delivers raw timer ids, exposes timer-wheel access through the session control context, and stores protocol connection state directly in generic session entries. TCP then reconciles all timer kinds at unrelated synchronization points. This leaks TCP timer count, connection shape, active-bit semantics, resolution, and dispatch policy across the Session/Transport boundary.

The leak makes ownership harder to reason about, allows stale or unrelated timer refreshes, and shapes the generic session API around TCP. It also blocks a clean QUIC implementation: QUIC needs independent connection and stream state machines, finer timer resolution, connection-level multiplexing, and transport-internal packetization rather than a TCP-shaped header callback.

## Solution

Align Hammer with VPP's ownership semantics while expressing the boundary with Rust generics and typed state. Session Worker remains the worker-local owner of app/session FIFOs, readiness, lifecycle coordination, and Session Runtime scheduling. Separate TCP and future QUIC workers own protocol objects, lookup, clocks, timer wheels, timer policy, expiry budgets, exact timer tokens, and protocol dispatch.

SessionQueueNode samples one absolute time per dispatch and statically dispatches that time to each registered transport before processing session control and I/O work. Each transport converts absolute time to its own ticks. Session Runtime neither stores nor interprets transport timer state.

A generic, statically dispatched SessionTransport interface connects Session Worker to a compile-time transport set without dynamic dispatch or a TCP/QUIC enum in generic session code. Session entries carry a transport-neutral dispatch key, an opaque generation-safe Transport Index, and a typed Session Lifecycle. TCP uses Session-Packetized TX; future QUIC uses Transport-Internal TX while Session FIFO remains the payload owner in both cases.

## User Stories

1. As a Session Runtime maintainer, I want generic session code to contain no TCP timer concepts, so that the Session/Transport boundary remains enforceable.
2. As a TCP maintainer, I want TCP to own its timer wheel and clock state, so that timer policy changes stay inside TCP.
3. As a TCP maintainer, I want expiry to identify one exact timer kind, so that handlers do not scan every timer to discover work.
4. As a TCP maintainer, I want set, update, and reset operations to synchronize the wheel immediately, so that unrelated connection operations do not refresh timers.
5. As a TCP maintainer, I want armed and pending-expiry states represented separately, so that active-timer behavior matches the transport's actual ownership state.
6. As a TCP maintainer, I want rearming a pending timer to make its old expiry stale, so that delayed dispatch cannot fire a superseded deadline.
7. As a TCP maintainer, I want reset to clear both armed and pending state, so that canceled work cannot be dispatched later.
8. As a data-plane maintainer, I want timer state to remain worker-local, so that hot paths require no cross-worker locks.
9. As a runtime maintainer, I want SessionQueueNode to sample one absolute Instant, so that all transport updates within a dispatch observe a consistent point in time.
10. As a transport implementer, I want to convert absolute time using my own resolution and expiry budget, so that TCP and QUIC are not forced onto one tick policy.
11. As a QUIC implementer, I want a transport worker separate from Session Worker, so that QUIC connection and stream state remain protocol-private.
12. As a QUIC implementer, I want connection-level timer and dispatch ownership, so that one connection can coordinate many stream sessions.
13. As a QUIC implementer, I want transport-internal TX, so that stream scheduling, multiplexing, packetization, and encryption are not forced through TCP's header-prepend model.
14. As a TCP implementer, I want Session-Packetized TX, so that Session Runtime can continue preparing buffers from session-owned FIFO bytes before TCP commits headers and send state.
15. As an app/session maintainer, I want Session FIFO to retain payload ownership for TCP retransmission and future QUIC transmission, so that transports do not create private payload copies.
16. As a Rust maintainer, I want transport integration to use a generic trait and static dispatch, so that the hot path avoids dynamic dispatch.
17. As a Rust maintainer, I want a compile-time transport set, so that adding QUIC does not add protocol variants to Session Runtime.
18. As a Session Runtime maintainer, I want a transport-neutral protocol dispatch key, so that session code can select a registered transport without understanding its object model.
19. As a Session Runtime maintainer, I want an opaque generation-safe Transport Index, so that stale pool indexes are rejected without exposing pool representation or protocol state.
20. As a maintainer, I want the Transport Index field and generic type parameter named by their roles rather than mechanically matching, so that type and value vocabulary remains clear.
21. As a lifecycle maintainer, I want session ownership states represented by distinct typed records, so that illegal owner transitions are not representable through loose flags.
22. As a lifecycle maintainer, I want Closed to retain the Transport Index while asynchronous cleanup continues, so that TCP TIME_WAIT, QUIC draining, and QUIC stream cleanup remain addressable.
23. As a lifecycle maintainer, I want Transport Deleted to carry no Transport Index, so that deleted transport objects cannot be referenced through session state.
24. As a TCP maintainer, I want TCP connection state to remain independent from Session Lifecycle, so that protocol closing states are not generalized into app/session ownership states.
25. As a QUIC maintainer, I want QUIC connection and stream send/receive states to remain independent from Session Lifecycle, so that generic session states do not flatten QUIC semantics.
26. As a QUIC maintainer, I want a connection close to notify all associated stream sessions through transport-neutral Session Worker operations, so that fan-out does not require type erasure or TCP-shaped callbacks.
27. As a Session Worker maintainer, I want fields to remain private behind transport-neutral methods, so that transports can coordinate lifecycle and FIFO work without reaching into session internals.
28. As a maintainer, I want no TcpQueue wrapper, so that a redundant transport-specific scheduling abstraction does not obscure the shared L5 Session Worker.
29. As a maintainer, I want no Live wrapper around lifecycle records, so that state names describe concrete ownership directly.
30. As a maintainer, I want failures to leave timer ownership and session ownership coherent, so that partial TX or timer updates cannot publish invalid data-plane work.
31. As a reviewer, I want architecture tests to reject TCP timer vocabulary in session modules, so that the boundary cannot silently regress.
32. As a reviewer, I want behavior tests at the SessionTransport seam, so that transport scheduling and lifecycle contracts can be verified without coupling tests to private storage.
33. As an operator, I want existing TCP connection, retransmission, delayed ACK, persist, keepalive, pacing, and TIME_WAIT behavior preserved, so that the ownership refactor causes no protocol regression.

## Implementation Decisions

- Follow VPP's semantic and ownership model, not its C API shape: Session Worker schedules session work; protocol workers own transport state and timer policy.
- Keep Session Worker, TCP Worker, and future QUIC Worker as separate worker-local owners. Do not nest Session Runtime inside a transport worker.
- Keep one shared L5 session namespace across transports. Session scheduling records remain transport-neutral.
- SessionQueueNode samples one absolute Instant per dispatch. Registered transport updates run before session control events and session I/O work, preserving deterministic VPP-style ordering.
- Each transport owns its timer resolution, last-update state, tick conversion, expiry budget, expired scratch storage, pending exact tokens, and timer wheel. TCP may retain a 10 ms resolution while QUIC may choose 1 ms.
- Remove the Session Runtime timer wheel, shared session timer constants, expired timer records, pending transport timer queue, tick conversion, raw timer delivery, and tick-based polling API.
- Remove raw timer-wheel access from the session control boundary. Generic session code must not schedule, cancel, update, refresh, or dispatch transport timers.
- Introduce a generic SessionTransport interface with an opaque generation-safe Transport Index and Segment parameterization. Transport methods receive the concrete generic Session Worker through private, transport-neutral operations.
- Use a statically dispatched compile-time transport set. Protocol selection uses a transport-neutral dispatch key; there is no dynamic trait object and no TCP/QUIC enum embedded in generic session code.
- Make SessionEntry generic over the transport-provided index. It stores the dispatch key, Transport Index where the lifecycle state permits it, and session-owned scheduling state; it does not store a TCP connection or QUIC state.
- Store Session Lifecycle as a closed enum of Active, App Closed, Transport Closed, Closed, and Transport Deleted. State-bearing variants use distinct typed records and state-specific consuming transitions.
- Active, App Closed, Transport Closed, and Closed retain a Transport Index. Transport Deleted carries no index. Closed remains addressable until asynchronous transport cleanup finishes.
- Keep transport protocol state machines private: TCP connection state, QUIC connection state, and QUIC stream send/receive state are not variants of Session Lifecycle. Transport Closing remains protocol-private.
- Associate each transport with a typed TX strategy instead of optional methods. Session-Packetized TX lets Session Runtime select FIFO bytes and prepare a TX Batch before the transport performs one typed TX action and the runtime flushes the committed batch. Transport-Internal TX lets the transport engine schedule and emit packets from session-owned bytes.
- TCP uses Session-Packetized TX. Future QUIC uses Transport-Internal TX and may coordinate multiple stream sessions from one connection update. QUIC is not required to emulate TCP header prepending.
- Preserve the existing ownership rule that Session FIFO owns TX payload bytes until ACK cleanup. TCP recovery and future QUIC retransmission must not retain private payload copies.
- TCP timer identity becomes a private typed kind covering retransmit, RACK, TLP, delayed ACK, persist, keepalive, TIME_WAIT, and pacing.
- TCP timer state keeps separate typed armed and pending sets. A timer is active when either set contains its kind. Expiry moves the exact kind from armed to pending; dispatch clears pending; reset clears both.
- TCP's private timer owner performs immediate set, update, and reset against the wheel and emits exact generation-safe timer tokens. It has no dependency on Session Worker.
- TCP Worker drains exact tokens and invokes the matching connection handler directly. A token whose connection generation or timer state no longer matches is stale and is ignored.
- Rearming a timer while an older expiry is pending creates a new armed deadline. The old pending token must not clear or fire the new deadline.
- Delete full-timer reconciliation and refresh-mask behavior. Do not scan all TCP timer kinds at connection synchronization points.
- Delete public raw TCP timer ids, timer count, and mask semantics. Exact timer identities and active-state representation remain TCP-private.
- Delete the redundant TcpQueue concept. The existing Session Work Batch remains the single shared scheduling mechanism for session work.
- Do not add SessionAccess wrappers, TLS type erasure, raw-pointer type erasure, generic timer-action carriers, or TCP-specific runtime/buffer APIs.
- Continue to use hammer-infra data-plane primitives. If implementation discovers a genuinely shared missing primitive, it requires separate approval before adding a generic owning-layer API.
- Errors in transport time update, exact timer dispatch, lifecycle transition, or TX action propagate through the existing result boundary. A failed transport action must not flush an uncommitted TX Batch or leave an invalid ownership transition visible.

### Approved New Types and Interfaces

- Generic SessionTransport trait: approved because the existing per-connection protocol trait mixes session scheduling, TCP-shaped TX callbacks, and raw timer dispatch; it cannot represent independent transport workers or QUIC internal TX.
- Compile-time transport set: approved because static protocol registration is required without dynamic dispatch or protocol variants in Session Runtime.
- Typed Session Lifecycle records and closed state enum: approved because the existing generic state storage does not encode independent app and transport ownership or prohibit an index after transport deletion.
- Associated typed TX strategies for Session-Packetized TX and Transport-Internal TX: approved because one optional-method interface would force TCP and QUIC through incompatible packetization contracts.
- Private typed TCP timer kind, timer sets, and timer state: approved because raw ids and a single active mask cannot distinguish wheel-armed work from exact expiry pending dispatch.
- Private TCP timer owner and exact timer token: approved because the Session Runtime wheel and full-timer reconciliation cannot provide transport-owned resolution, immediate synchronization, or exact dispatch.

## Testing Decisions

- Prefer the highest stable seam: exercise SessionQueueNode with a test transport set and observe transport update ordering, session scheduling, lifecycle notifications, FIFO effects, and emitted data-plane work. Tests should assert externally visible behavior, not concrete worker fields or timer-wheel layout.
- Add compile-time and architecture guardrails proving session modules do not import TCP connection types, TCP timer constants, raw keep masks, or timer-wheel policy.
- Verify that one sampled absolute Instant is delivered to every registered transport before control and I/O dispatch, while each transport independently converts time according to its resolution and expiry budget.
- Verify static dispatch with at least two transport implementations or test transports so the generic session surface cannot accidentally encode TCP-only assumptions.
- Verify Session Lifecycle transitions for app-first close, transport-first close, both owners closed, delayed transport deletion, stale deletion notification, and final app cleanup. Assert that Closed retains the Transport Index and Transport Deleted cannot expose one.
- Verify a QUIC-shaped test transport can fan one connection event out to multiple stream sessions through transport-neutral Session Worker methods and can use Transport-Internal TX without implementing TCP header behavior.
- Verify a TCP-shaped test transport uses Session-Packetized TX: Session Runtime selects FIFO bytes and prepares buffers, TCP commits protocol state and output intent, and only a successful commit becomes graph-visible.
- Verify that a failed typed TX action does not flush buffers or consume session-owned bytes.
- Verify each TCP timer kind through externally observable connection behavior, including retransmit, RACK, TLP, delayed ACK, persist, keepalive, TIME_WAIT, and pacing where supported by current behavior.
- Verify exact-token dispatch invokes only the expired timer handler and never scans or refreshes unrelated timers.
- Verify set, update, reset, expiry-to-pending, dispatch, and active-state behavior. Cover reset while pending, rearm while pending, stale generation, duplicate expiry, deleted connection, zero elapsed ticks, large elapsed time, and expiry-budget carryover.
- Verify unrelated ACK, RX, TX, and lifecycle operations do not move active timer deadlines unless TCP policy explicitly updates that exact timer.
- Preserve existing end-to-end TCP tests for state transitions, output, reset, lookup, app/session boundaries, session queue dispatch, and session runtime behavior. These suites are prior art for validating behavior above private implementation.
- Run focused hammer-service tests while iterating, then formatting, linting, and the full workspace test suite before completion.

## Out of Scope

- Implementing QUIC, QUIC cryptography, QUIC packet formats, congestion control, stream APIs, or a production QUIC Worker.
- Changing TCP congestion-control ownership or adding a congestion-control graph node.
- Redesigning the app/session FIFO and message-queue boundary.
- Changing normal TX payload ownership, introducing transport-private payload copies, or adding feature-specific buffer owners.
- Introducing dynamic dispatch, a C-style virtual function table, runtime protocol plugin loading, or a TCP/QUIC enum in Session Runtime.
- Creating a generic runtime timer service shared by transports.
- Reworking unrelated TCP protocol algorithms beyond the timer ownership changes required to preserve existing behavior.
- Adding new hammer-infra primitives without a separately justified and approved shared requirement.

## Further Notes

- VPP is the semantic and ownership reference: session/runtime schedules session work, while TCP and QUIC own their protocol workers, timers, and exact dispatch. Rust traits, generics, associated types, and monomorphization replace VPP's C function-table mechanics.
- The central acceptance condition is architectural: Session Runtime must contain no TCP or QUIC timer semantics, yet it remains the scheduler of session work.
- The design intentionally anticipates both TCP and QUIC. It does not require speculative QUIC implementation, but a QUIC-shaped test transport must prove the interface supports connection-level coordination, stream lifecycle fan-out, independent timer resolution, and Transport-Internal TX.
- Existing domain documentation and the transport-worker ownership ADR are the vocabulary and decision source for implementation.


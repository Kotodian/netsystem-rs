# Private per-app MQ and appsl-rx-mqs-input VPP alignment

## Problem Statement

Hammer currently routes app-to-session events through one shared per-worker `tx_evt_q`, then moves them into `session_evt_q` before `session-queue` consumes them. This is neither VPP's shared worker-MQ path nor VPP's private per-application MQ path, and it lets one Application fill a shared queue and affect other Applications. The `appsl-rx-mqs-input` node is also named after VPP but does not implement VPP's pending MQ list, eventfd, POSTPONED, or direct event-lane drain semantics.

## Solution

Give every Application its own private per-Data-Worker Session Message Queue, publish those queues once during attach, and align `appsl-rx-mqs-input` with VPP: worker-local pending MQ list, signal-fd read, snapshot drain, POSTPONED re-queue, direct entry into Session Worker event lanes, and Interrupt-only wake of `session-queue`. External Applications get isolation and a faster event path; local Applications use the same ownership model.

## User Stories

1. As an external Application developer, I want my app-to-session events to use a private per-worker MQ, so that another Application's event volume or full queue cannot block my sessions.
2. As an external Application developer, I want attach to publish the Application MQ segment and one write descriptor per Data Worker once, so that session accept does not repeat TX MQ descriptor handshakes.
3. As an external Application developer, I want `accept()` to construct an `AppSession` by selecting the per-worker MQ from the session handle, so that session publication remains small and predictable.
4. As a daemon maintainer, I want `ApplicationMain` to own per-Application MQ resources, so that Application attach, detach, and rollback have one authority.
5. As a daemon maintainer, I want attach to register all Data Workers before returning, so that an Application cannot publish events into a queue the dataplane is not yet draining.
6. As a daemon maintainer, I want attach failure to roll back partially installed worker registrations, so that a failed attach leaves no orphaned FileMain or pending list state.
7. As a daemon maintainer, I want detach to drain remaining MQ events, remove worker registrations, and only then release MQ storage, so that no events are silently stranded in freed shared memory.
8. As a Session Worker, I want `appsl-rx-mqs-input` to drain pending Application MQs by snapshot, so that continuous producers are handled without an unbounded drain loop.
9. As a Session Worker, I want non-empty MQs to be marked POSTPONED and re-added to the pending list, so that remaining events are processed in a later appsl dispatch.
10. As a Session Worker, I want app events to enter ctrl/new/old event lanes directly, so that the double queue hop through `session_evt_q` is removed.
11. As a Session Worker, I want `session-queue` to dispatch control before new IO and new IO before old IO, so that lifecycle events win over stale TX/RX work.
12. As a Session Worker, I want appsl to wake `session-queue` only when the worker is in Interrupt state, so that polling workers are not interrupted unnecessarily.
13. As a Session Worker, I want a VPP-shaped Polling/Interrupt/Idle state, so that appsl and `session-queue` scheduling follow VPP rather than a fixed polling loop.
14. As a Session Worker, I want a timerfd-style deadline wake tied to worker state, so that Idle and Interrupt workers wake at VPP-compatible intervals without requiring transport timer-wheel ownership.
15. As a transport maintainer, I want my existing TimerWheel to remain transport-owned and to continue being advanced by `session-queue` update time, so that this change does not move transport timers into Session Runtime.
16. As a Hammer infrastructure maintainer, I want a generic `LinkedList<T>` with multiple heads and generation-safe handles, so that pending MQ and event-lane lists do not duplicate hand-rolled link management.
17. As an infrastructure maintainer, I want the generic list to be tested independently of Application MQ and Session Event semantics, so that low-level link behavior is proven once.
18. As an attach protocol maintainer, I want protocol versioning and variable descriptor counts, so that adding per-worker MQ descriptors does not silently break older clients.
19. As a maintainer, I want `network.session.app_mq_capacity` to control per-Application MQ capacity, so that operators can size queue memory without editing constants.
20. As a maintainer, I want local and external Applications to share the same per-Application MQ model, so that there is one appsl path instead of divergent local and remote event paths.
21. As a test author, I want tests that prove one full Application MQ does not block another pending Application MQ, so that the isolation guarantee is executable.
22. As a test author, I want tests that prove POSTPONED continuation, self-wake, and Interrupt-only `session-queue` wake, so that appsl behavior matches VPP semantics.
23. As a test author, I want attach/accept integration tests through the real AppClient/AppServer path, so that descriptor counts and worker mapping are validated end to end.
24. As a maintainer, I want ADR and glossary vocabulary updated, so that future contributors use `Application Rx MQ`, `Appsl Pending Rx MQ`, `Session Event Lane`, and `Session Worker State` consistently.

## Implementation Decisions

- Every Application, local or external, owns private per-Application MQ resources.
- Each Application has one Session Message Queue per Data Worker; `DataWorkerId.slot()` maps directly to queue index. Hammer does not add VPP's main-thread `+1` queue because Hammer's main thread does not carry session event work.
- `ApplicationMain` owns the MQ set and creates it at attach time. External Applications use shared segments; local Applications use local segments.
- `ApplicationMain` exposes attach entry points for external and local Applications, requires SessionMain and Data Workers to be ready, and registers all workers before returning. Any worker failure rolls back the attach.
- Detach removes Application identity and control-plane state, drains remaining per-Application MQ events, removes worker FileMain registrations and pending entries, and only then drops MQ and segment resources.
- `network.session.app_mq_capacity` controls per-Application MQ usable event capacity. Default is 2048; minimum is 128.
- Attach protocol is versioned and publishes the Application MQ segment plus one write descriptor per Data Worker. Session accept no longer sends per-session TX event MQ descriptors.
- `AppClient` retains a `worker_index -> SessionMsgQueue` mapping and uses it when constructing accepted sessions.
- `appsl-rx-mqs-input` remains a Driver node because Driver is Hammer's INPUT equivalent.
- `appsl-rx-mqs-input` keeps a worker-local pending list of Application Rx MQs with PENDING and POSTPONED flags.
- FileMain callbacks for per-Application MQ signal fds only add the MQ to the pending list and wake `appsl-rx-mqs-input`.
- `appsl-rx-mqs-input` drains each pending MQ by snapshot, reads signal fds unless POSTPONED, re-adds non-empty MQs as POSTPONED, self-wakes when pending remains, and wakes `session-queue` only when the Session Worker is Interrupt.
- App events enter Session Worker event lanes directly instead of passing through `session_evt_q`. `session_evt_q` remains available for internal Session Message Queue events.
- Session Worker uses generic `LinkedList`-based ctrl/new/old event lanes. The same infra list abstraction is used for the pending MQ list.
- `session-queue` dispatch order is update time, drain internal MQ into lanes, control lane, new IO lane, old IO lane, then flush generated TX.
- Session Worker state follows VPP: Polling, Interrupt, and Idle.
- FileMain gains a timer/deadline file abstraction. Linux uses timerfd; macOS uses the kqueue backend or an equivalent fallback. Timer values follow VPP state-derived timeouts.
- Transport TimerWheel ownership is unchanged. `session-queue` continues to advance transport time through its existing transport update hook.
- No new dynamic dispatch, no business wrapper types, and no io_uring-style AppRing surfaces are introduced.

## Testing Decisions

Good tests validate external behavior and ownership boundaries rather than internal list mechanics or private queue fields. The primary test seam is Session Runtime dispatch with per-Application MQ fakes and a transport fake, matching existing Session Runtime dispatch tests. Attach protocol behavior is tested through the real AppClient/AppServer integration seam.

Modules under test:
- `hammer-infra`: generic `LinkedList` multi-head operations, removal, move-to-back, and stale handle rejection.
- `hammer-runtime`: FileMain timer/deadline file registration and platform backend behavior.
- `hammer-service`: Application MQ attach/detach lifecycle, worker registration rollback, appsl pending/POSTPONED drain, event-lane ordering, and Session Worker state transitions.
- `hammer-app`: attach/accept protocol versioning, descriptor counts, worker mapping, and session construction.

Prior art includes Session Runtime dispatch tests with transport fakes, Session Message Queue multi-ring tests, attach descriptor lifetime tests, and FileMain readiness tests.

Acceptance tests must cover:
- Attach failure rollback removes all partial worker registrations.
- Detach drains remaining MQ events before resource release.
- One full Application MQ does not prevent another pending MQ from being drained.
- POSTPONED entries continue on a later appsl dispatch.
- Appsl wakes `session-queue` only in Interrupt state.
- Control lane dispatches before new IO; new IO dispatches before old IO.
- `AppClient` selects the correct per-worker MQ for accepted sessions.
- Worker state transitions and timerfd deadlines are observable without depending on wall-clock sleeps.

## Out of Scope

- Rewriting TCP transport timer ownership or dispatch.
- Removing the shared worker MQ path for future non-private deployments.
- Changing Session Event identity rules or adding generation to event identity.
- Reintroducing io_uring AppRing, SQE/CQE, or submission/completion surfaces.
- Implementing TLS, QUIC, or other App Session protocol behavior.
- Replacing the existing `hammer-app` async API surface.

## Further Notes

The design is captured in ADR-0027 and a dated implementation plan. This spec is the issue-tracker synthesis and should stay consistent with those documents. External Application performance and isolation are the primary constraints; local Applications use the same model but must not add hot-path overhead to external event processing.

# Task 1 Code-Quality Re-review

## Verdict

PASS. No Critical or Important findings remain that block Task 1.

Scope re-reviewed: `f653c2ea..3822940d`, with particular attention to the
follow-up fix `3822940d` and the disposition of the three findings recorded in
`cb2512d2`. The review also checked the generation-safety follow-up recorded in
`ccfebb28` and GitHub issue #42.

The earlier spec review remains accepted as PASS. This re-review focuses on
static queue typing, worker graph registration order and uniqueness, Local/SVM
backend selection, the previously identified TX clone, app-event generation
safety, public callback behavior, and focused regression coverage.

## Finding Disposition

### Resolved 1: the TCP session queue remains statically typed through worker graph registration

The type-erased TLS cache introduced during Task 1 has been removed by
`3822940d`. `TCP_SESSION_QUEUE_RUNTIME_DATA`, its `TypeId`/backend key, and
`ensure_tcp_session_queue` no longer exist.

`wire_worker_graph` now selects the Local or SVM segment backend once, creates
the concrete `SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>`, and
passes the resulting typed `SessionQueueHandle` through one consolidated
`register_typed_worker_graph` path. The handle is used to construct every TCP
consumer node and attach the session queue without reconstructing its generic
type from an untyped cached pointer.

The graph installation order is also coherent: `install_worker_graph` first
registers the seven typed TCP/session nodes, filters those entries from the
distributed registration slice, and then calls `init_graph` for the remaining
nodes so named next arcs can resolve after their targets are present. The
packet-graph regression test verifies that each filtered node has exactly one
static registration and is omitted from deferred registration.

This resolves the Task 1 ownership, TLS, and static type-safety finding.

### Deferred 2: the normal TX path still deep-clones connection state

`TcpWorker::tx_action` still clones the complete `TcpConnection` for candidate
TX rollback. That clone rebuilds allocation-owning recovery and SACK state, so
the original performance concern remains technically valid.

It does not block Task 1: the deep clone predates Task 1, and Task 1 explicitly
required retaining the candidate clone. Replacing it would introduce a new
private bounded TX preview/checkpoint type or an equivalent new API shape.
Explicit user approval has been requested for that surface, as required by the
repository rules; this review does not treat the replacement as authorized or
prescribe it as part of `3822940d`.

### Deferred 3: app-to-session event identity is tracked outside Task 1

The slot-only `SessionEvt.session_index` weakness also remains technically
valid, but it predates #41 and changing the Local/SVM app-event identity and
shared-memory ABI is outside the transport-owned timer refactor.

Commit `ccfebb28` records the problem and acceptance-test shape in
`app-event-generation-follow-up.md`. GitHub issue #42, "Make app-to-session
events generation-aware", is OPEN with `enhancement` and `needs-triage` labels.
Both records explicitly cover delayed old `Close` and `TxDeq` events after slot
reuse, the Local/SVM boundary, and ABI/versioning impact.

This is therefore a deferred generation-safety issue, not an unresolved Task 1
failure.

## Additional Re-review Notes

- `register_tcp_input`, `register_tcp_listen`, `register_tcp_established`,
  `register_tcp_rcv_process`, and `register_tcp_syn_sent` now look up nodes that
  were registered by `wire_worker_graph`, instead of registering them directly.
  Repository search found no ordinary call sites; these functions are graph
  macro init callbacks, and normal worker initialization wires the typed graph
  exactly once before deferred macro registration. This is not a behavioral
  regression in the supported initialization path.
- Those callback functions remain `pub` even though their behavior is now
  internal lifecycle plumbing. Narrowing or documenting that surface would be
  useful cleanup, but no external compatibility contract was found in this
  repository, so it is non-blocking.
- Direct repeated calls to `wire_worker_graph` are not an advertised lifecycle
  operation and were not added as a required idempotent path. Normal worker
  initialization calls it once; the duplicate-registration guard verifies the
  supported static/deferred registration split.

## Verification

- `cargo test -p hammer-service --lib typed_tcp_worker_graph_isolated_by_runtime_and_backend -- --test-threads=1`:
  PASS, 1 test.
- `cargo test -p hammer-service --lib packet_graph::tests -- --test-threads=1`:
  PASS, 2 tests.
- `cargo test -p hammer-service --test tcp_session_app_boundary`: PASS, 7 tests.
- `cargo fmt --all -- --check`: PASS.
- `git diff --check 3822940d^..3822940d`: PASS.
- `gh issue view 42 --repo Kotodian/hammer-ios-rs`: confirmed OPEN,
  `enhancement`, `needs-triage`.

The focused builds emit numerous pre-existing warnings. No full workspace test
suite was run for this re-review.

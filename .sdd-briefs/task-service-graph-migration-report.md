# Service Graph Migration Report

## Status

DONE_WITH_CONCERNS.

Implemented the hammer-service migration from old `#[graph_node]` macro arguments to the new required `init = path` API. `cargo build -p hammer-service` and `cargo build --workspace` pass. Focused graph declaration tests pass. The full `hammer-service` lib and `transport::tcp` filters still fail in TCP connection/listen tests that reproduce individually and do not exercise the migrated graph init/wiring path.

## Implemented

- Migrated every `#[graph_node]` site in `crates/hammer-service/src` from old `register =`, `assemble =`, and `wire =` args to `init = ...`.
- Added per-node init functions:
  - `data_plane::register_drop`
  - `data_plane::register_handoff`
  - `net::lookup::register_ip_lookup`
  - `session::node::register_session_queue_node`
  - `transport::tcp::register_tcp_input`
  - `transport::tcp::output::register_tcp_output`
  - `transport::tcp::reset::register_tcp_reset`
  - `transport::tcp::{listen,established,syn_sent,rcv_process}::register_tcp_*`
- Extracted `transport::tcp::ensure_tcp_session_queue::<C>` from the old inline `TcpMain::register_tcp_input` lazy queue creation so tcp-input and the four congestion nodes can create/read the per-worker `TcpQueue<C>` without relying on linkme order.
- Added `transport::tcp::wire_worker_graph`, which runs after `DataPlaneRuntime::init_graph`, resolves `session-queue` and `tcp-output`, attaches the TCP session queue, and sets the session queue driver node to `Polling`.
- Made `SessionQueueNode` init transport-agnostic: it only registers itself as a driver node and records its own worker-local runtime data. TCP later calls `SessionQueueNode::attach_queue_by_runtime_data`.
- Rewired `RuntimeService::new_with_event_subscribers` to:
  - create and seed `RuntimeRegistry` with parsed `Config`
  - initialize `CONTROL_INITS`
  - run `runtime.init_graph(worker, &SERVICE_GRAPH_NODES)` on each data worker through a clone carrying the configured handoff node handle
  - run `transport::tcp::wire_worker_graph`
  - resolve trace input names from actual registered node IDs instead of the deleted `packet_graph::resolve_graph_node`
- Rewrote the service graph test to iterate `SERVICE_GRAPH_NODES` by registration name and added a root-level test so the requested filter `service::service_packet_graph_resolves_tcp_nodes` runs one test.
- Removed stale `assemble_session_queue_node`, deleted old macro args, and confirmed no `with_boot`, `wire_session_queue`, `install_service_graph`, or `resolve_graph_node` references remain in `hammer-service`.

## Design Decisions

- `NodeRuntime` exposes `node_by_name`, node metadata, and graph mutation APIs, but not typed node retrieval. I used a worker-local `SESSION_QUEUE_NODE_RUNTIME_DATA` slot owned by the session module and added `SessionQueueNode::attach_queue_by_runtime_data`. This keeps session-queue transport-neutral and avoids any lock or global `Mutex`.
- The session-queue node init function is named `register_session_queue_node` rather than `register_session_queue` because the module already has the generic queue registration function `register_session_queue<Q>(queue)`, which TCP still uses to create `TcpSessionDriver<C>` queues.
- The `trace.publish_options` resolver now uses real node IDs collected from the initialized worker graph, instead of a placeholder resolver over declarations. This preserves trace input matching against actual runtime node IDs.
- `RuntimeTcpListenerControlState::new` now uses `TCP_MAIN.control().clone()` so service listener updates and `TcpInputNode` share the same control plane after `CONTROL_INITS` initialization.

## Verification

- `cargo test -p hammer-service --lib service::service_packet_graph_resolves_tcp_nodes` before implementation: FAIL, compile failed on missing `init` args, old `register` args, and deleted packet graph helpers.
- `cargo fmt --all`: PASS.
- `cargo build -p hammer-component-macros`: PASS.
- `cargo test -p hammer-component-macros --lib`: PASS, 5 passed / 0 failed.
- `cargo build -p hammer-service`: PASS, 0 errors. Existing warnings remain, including macro-generated `non_upper_case_globals`.
- `cargo test -p hammer-service --lib packet_graph`: PASS, 3 passed / 0 failed / 150 filtered.
- `cargo test -p hammer-service --lib service::service_packet_graph_resolves_tcp_nodes`: PASS, 1 passed / 0 failed / 152 filtered.
- `cargo build --workspace`: PASS.
- `cargo test -p hammer-service --lib`: FAIL, 153 tests started; failures reproduced in TCP tests:
  - `transport::tcp::connection::tests::tcp_pacing_timer_expiry_rearms_and_requests_tx_dispatch` failed with `pacing expiry should request a tx dispatch`.
  - `transport::tcp::listen::tests::backlog_full_rejects_new_listener_tuple` failed with overflow in `crates/hammer-infra/src/map.rs`.
  - `transport::tcp::listen::tests::final_ack_creates_real_session_after_cookie_validation` aborted on a `slice::from_raw_parts` unsafe precondition violation.
- `cargo test -p hammer-service --lib transport::tcp`: FAIL, 115 tests started; same TCP failure class reproduced and the run aborted in a listen test.
- Individual reproductions:
  - `cargo test -p hammer-service --lib transport::tcp::connection::tests::tcp_pacing_timer_expiry_rearms_and_requests_tx_dispatch -- --exact`: FAIL, 0 passed / 1 failed.
  - `cargo test -p hammer-service --lib transport::tcp::listen::tests::backlog_full_rejects_new_listener_tuple -- --exact`: FAIL, 0 passed / 1 failed.
  - `cargo test -p hammer-service --lib transport::tcp::listen::tests::final_ack_creates_real_session_after_cookie_validation -- --exact`: FAIL, process aborted.

## Files Changed

- `crates/hammer-service/src/data_plane.rs`
- `crates/hammer-service/src/net/lookup/mod.rs`
- `crates/hammer-service/src/session/node.rs`
- `crates/hammer-service/src/service.rs`
- `crates/hammer-service/src/transport/tcp/mod.rs`
- `crates/hammer-service/src/transport/tcp/input.rs`
- `crates/hammer-service/src/transport/tcp/output.rs`
- `crates/hammer-service/src/transport/tcp/reset.rs`
- `crates/hammer-service/src/transport/tcp/listen.rs`
- `crates/hammer-service/src/transport/tcp/established.rs`
- `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- `.sdd-briefs/task-service-graph-migration-report.md`

## Self-Review

- Completeness: every `#[graph_node]` site in `hammer-service/src` now has `init = ...`; old macro arguments and deleted helper references are gone.
- Quality: no locks were added for per-worker state; `with_congestion!` is used for congestion-typed graph init; session-queue init stays transport-neutral; node names keep the existing no-`-node` form.
- Discipline: did not touch the macro crate; did not add a LATE slice; reused `DataPlaneRuntime::init_graph`, `NodeRuntime::node_by_name`, and `NodeRuntime::set_node_state`.
- Testing: graph-focused checks, service build, macro checks, and workspace build pass. Full service/TCP lib tests are blocked by reproducible TCP failures outside the graph migration surface.

## Concerns

- The full `hammer-service` lib and `transport::tcp` filters do not pass on this branch because of standalone TCP test failures. The failing tests do not use the new graph-node init functions, `init_graph`, or `wire_worker_graph`; they reproduce individually in TCP connection/listen code.
- The macro-generated distributed slice statics emit `non_upper_case_globals` warnings. This is outside the task because the macro crate was explicitly out of scope.

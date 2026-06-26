# Task: Service-side graph-node init migration to `init = path`

## Where this fits

This is the final implementation step of the "graph-node init extensible redesign"
plan. The macro crate (`hammer-component-macros`) has ALREADY been rewritten
(committed at HEAD) so that `#[graph_node]` NO LONGER generates any init body.
The new macro API is:

```rust
#[hammer_component_macros::graph_node(
    graph = service,                 // linkme slice name (required)
    init = path::to::register_fn,    // REQUIRED: fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId>
    kind = internal,                 // optional: internal | driver | handoff (default internal)
    name = "foo",                    // optional, default = struct's NODE_NAME
    next = FooNext,                  // optional, for NodeRegistration::next(name, Next::COUNT)
)]
pub struct FooNode { ... }
```

The macro now emits ONLY a `NodeEntry { registration, kind, init: <the path> }`
into the `SERVICE_GRAPH_NODES` linkme slice. It does NOT generate a wrapper
init fn, does NOT generate any body. The OLD args `register = ...`, `assemble =
...`, `wire = ...` are GONE and will fail to parse.

`NodeEntry` (hammer-adapter/src/node.rs): `{ registration: NodeRegistration,
kind: NodeKind, init: fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId> }`,
`#[derive(Clone, Copy)]`.

Because every `#[graph_node]` site in `hammer-service` still uses the OLD
args (`register = ...`, `assemble = ...`, `wire = ...`), **the service crate
does NOT compile right now.** Your job is to migrate every `#[graph_node]`
site to the new `init = ...` API and add the per-node `register_*` init fns,
plus the TCP per-worker graph-assembly fn `wire_worker_graph`, so
`cargo build -p hammer-service` passes and `cargo test -p hammer-service --lib`
passes (especially `packet_graph::service_graph_contains_tcp_nodes` and
`service::service_packet_graph_resolves_tcp_nodes`).

## The design (VPP two-phase, NO `late` slice)

Read the plan body at `/Users/linqiankai/.cursor/plans/graph-node_init_extensible_redesign_fde5b57b.plan.md`
for the full rationale. Summary:

1. **Node init fns only register themselves.** A node's `register_*` init fn
   calls `rt.nodes().try_register_*(Self::new(...))` and returns the NodeId.
   It does NOT look up other nodes by name, does NOT do cross-node wiring.
   (VPP `vlib_register_all_static_nodes`.)

2. **Cross-node wiring happens AFTER all nodes are registered**, in a
   per-worker assembly step. `DataPlaneRuntime::init_graph(worker, &SERVICE_GRAPH_NODES)`
   walks all entries calling each `init`, then calls
   `resolve_named_next_nodes()` (VPP `vlib_node_main_init`). AFTER that, the
   TCP subsystem's per-worker `wire_worker_graph(rt, worker)` does the
   `attach_queue` + bind `tcp-output` + `set_node_state(Polling)`.

3. **No `late`/LATE slice.** All nodes go in the single `SERVICE_GRAPH_NODES`.
   `packet_graph.rs` is ALREADY correct (single slice) — do NOT add a LATE slice.

4. **session-queue is transport-agnostic.** Its init fn ONLY registers itself:
   `rt.nodes().try_register_driver(SessionQueueNode::new()?)`. It does NOT
   attach_queue, does NOT know tcp-output, does NOT use `with_congestion!`.
   The TCP subsystem calls `attach_queue` on it from `wire_worker_graph`.

5. **`TcpQueue<C>` lazy-creation with NO ordering dependency.** The 4 TCP
   congestion nodes (listen/established/syn_sent/rcv_process) and tcp-input
   all need a `TcpQueue<C>`. linkme order is NOT guaranteed, so you CANNOT
   rely on "tcp-input runs first". Extract a TCP-subsystem fn
   `ensure_tcp_session_queue::<C>(rt, worker) -> CoreResult<NodeRuntimeData>`
   that: on first call builds
   `register_session_queue(TcpSessionDriver::<C>::new(worker_id, rt.packet_buffers().clone()))`
   and caches the returned `runtime_data` in `TcpWorkerOwnedState` TLS
   (`queue_runtime_data` / `set_queue_runtime_data` already exist in
   `crates/hammer-service/src/transport/tcp/lookup.rs`); subsequent calls
   return the cached data. tcp-input's init and the 4 congestion nodes' inits
   ALL call `ensure_tcp_session_queue::<C>` — whoever runs first creates it,
   the rest get the cache. No lock (TLS is per-worker single-threaded).
   The existing `TcpMain::register_tcp_input::<C>` (tcp/mod.rs:82-114) already
   does this lazy-create inline — refactor that inline block INTO
   `ensure_tcp_session_queue::<C>` and have `register_tcp_input` call it.

## Per-node migration table

For each `#[graph_node]` site, write a `register_*` init fn in the node's
own module and change the attribute to `init = path::to::register_*`.

| Node | init fn location | init fn body |
|---|---|---|
| `DropNode` | `crates/hammer-service/src/data_plane.rs` | `rt.nodes().try_register_internal(DropNode)` |
| `HandoffNode` | same | `rt.nodes().register_internal_with_handle(rt.handoff_node_handle()?, HandoffNode)` (returns CoreResult<NodeHandle>; use `?`) |
| `TcpOutputNode` | `crates/hammer-service/src/transport/tcp/output.rs` | `rt.nodes().try_register_internal_with_next_names(TcpOutputNode::new([NodeId::new(0); TcpOutputNext::COUNT]), &TcpOutputNext::NEXT_NAMES)` |
| `TcpResetNode` | `crates/hammer-service/src/transport/tcp/reset.rs` | `rt.nodes().try_register_internal_with_next_names(TcpResetNode::new([NodeId::new(0); TcpResetNext::COUNT]), &TcpResetNext::NEXT_NAMES)` (NOT congestion-typed) |
| `TcpInputNode<C>` | `crates/hammer-service/src/transport/tcp/mod.rs` — rename existing `register_tcp_input_graph_node` to `register_tcp_input` | `crate::with_congestion!(\|C\| TCP_MAIN.get().ok_or_else(|| CoreError::internal("tcp main not initialized"))?.register_tcp_input::<C>(rt, worker))`. `register_tcp_input::<C>` internally calls `ensure_tcp_session_queue::<C>(rt, worker)` instead of its current inline lazy-create. |
| `TcpListenNode<C>` | `crates/hammer-service/src/transport/tcp/listen.rs` | `crate::with_congestion!(\|C\| { let rd = ensure_tcp_session_queue::<C>(rt, worker)?; let q = TcpQueue::<C>::new(rd); let control = TCP_MAIN.get()....control().clone();  let next = [NodeId::new(0); TcpListenNext::COUNT]; rt.nodes().try_register_internal_with_next_names(TcpListenNode::<C>::new(control, q, next), &TcpListenNext::NEXT_NAMES) })` |
| `TcpEstablishedNode<C>` | `.../established.rs` | same pattern, with `TcpEstablishedNext` and `TcpEstablishedNode::<C>::new(...)` matching its real constructor arity |
| `TcpSynSentNode<C>` | `.../syn_sent.rs` | same pattern with `TcpSynSentNext` |
| `TcpRcvProcessNode<C>` | `.../rcv_process.rs` | same pattern with `TcpRcvProcessNext` |
| `IpLookupNode` | `crates/hammer-service/src/net/lookup/mod.rs` — rename existing `register_ip_lookup_graph_node` to `register_ip_lookup` | `IP_MAIN.get().ok_or_else(|| CoreError::internal("ip main not initialized"))?.register_node(rt)` |
| `SessionQueueNode` | `crates/hammer-service/src/session/node.rs` — NEW `register_session_queue` | ONLY `rt.nodes().try_register_driver(SessionQueueNode::new()?)`. No TCP, no attach, no with_congestion. |

**IMPORTANT for the 4 congestion nodes:** You MUST read each node's actual
`::new` constructor signature (the `#[node]` macro generates `new` from the
struct's `#[node(default = ...)]` fields; look at how tests construct them,
e.g. listen.rs:757 `TcpListenNode::new(&control, queue, next)`). Match the
real arity. The plan's table shows the shape; verify against the real
constructor. The `control` arg is `TcpInputControlPlane` (cloneable — it's
`Arc<ArcSwap>` internally; `TCP_MAIN.get()?.control().clone()` should work,
check the type). `TcpQueue<C>` is `SessionQueueHandle<TcpSessionDriver<C>>`,
built via `TcpQueue::<C>::new(rd)`.

## TCP `wire_worker_graph` (NEW, in tcp/mod.rs)

```rust
pub fn wire_worker_graph(rt: &DataPlaneRuntime, worker: usize) -> CoreResult<()> {
    crate::with_congestion!(|C| {
        let rd = ensure_tcp_session_queue::<C>(rt, worker)?;
        let queue = TcpQueue::<C>::new(rd);
        let sq_id = rt.nodes().node_by_name("session-queue")
            .ok_or_else(|| CoreError::internal("session-queue not registered"))?;
        let tcp_output = rt.nodes().node_by_name("tcp-output")
            .ok_or_else(|| CoreError::internal("tcp-output not registered"))?;
        // attach_queue only needs the SessionQueueNode's runtime_data to find
        // its TLS slot. The SessionQueueNode was registered by its own init fn;
        // retrieve its runtime_data. Options (pick the cleanest):
        //  (a) add SessionQueueNode::attach_queue_by_runtime_data(rd, ...) assoc fn
        //      that does what attach_queue does today using rd.usize_word(0);
        //  (b) get the &SessionQueueNode back from NodeRuntime by NodeId.
        // Read node.rs NodeRuntime API to see what's available. attach_queue's
        // current body (session/node.rs:157-178) uses ONLY self.runtime_data,
        // so (a) is clean — add a pub(crate) assoc fn and have attach_queue
        // delegate to it.
        SessionQueueNode::attach_queue_by_runtime_data(
            /* sq runtime_data */,
            queue,
            tcp_output.into(),
            dispatch_registered_session_queue_once_at::<TcpConnection<C>>,
        )?;
        rt.nodes().set_node_state(sq_id, NodeState::Polling)?;
        Ok(())
    })
}
```

The `sq runtime_data`: `SessionQueueNode::new()` allocates a TLS slot and
returns a node whose `runtime_data` is `NodeRuntimeData::from_usize(slot)`.
`register_session_queue` (session/node.rs) returns a `SessionQueueHandle<Q>`
whose `runtime_data()` gives the QUEUE's runtime_data, NOT the
SessionQueueNode's. You need the SessionQueueNode's OWN runtime_data (the
slot index). Cleanest: have `SessionQueueNode::register_session_queue`
init fn store its own runtime_data somewhere retrievable, OR retrieve the
registered `&SessionQueueNode` from `NodeRuntime` by `sq_id` and read
`.runtime_data`. Investigate `NodeRuntime`'s node-instance retrieval API
in `crates/hammer-adapter/src/node.rs` (look for a method to get a
registered node back by NodeId). If NodeRuntime does not expose typed node
retrieval, the clean fallback is: `SessionQueueNode` init fn, after
registering, cannot easily hand its runtime_data to wire_worker_graph
through a global — BUT you can make `SessionQueueNode` store its slot in a
TLS or have `register_session_queue` ALSO record the node's runtime_data in
`TcpWorkerOwnedState` (a new `session_queue_runtime_data: Option<NodeRuntimeData>`
field). Choose the minimal, clean option. Do NOT introduce a Mutex.

`dispatch_registered_session_queue_once_at` — find its real path/signature
via grep (it's referenced in the deleted `wire_session_queue` in git history:
`dispatch_registered_session_queue_once_at::<TcpConnection<C>>`). It's a
`SessionQueueDispatchFn`.

## service.rs (Task 9 wiring)

`crates/hammer-service/src/service.rs` currently calls
`packet_graph::install_service_graph(...)` (line ~369) and
`packet_graph::resolve_graph_node` (line ~403, ~1113-1120). Both are DELETED
from packet_graph.rs already. Replace with the new flow. Find the worker
startup / install_on_workers closure (grep `install_on` / where the
`DataPlaneRuntime` per-worker is set up). In that worker setup:

```rust
runtime.init_graph(worker, &crate::packet_graph::SERVICE_GRAPH_NODES)?;
crate::transport::tcp::wire_worker_graph(&runtime, worker)?;
```

`init_graph` signature (adapter buffer.rs):
`pub fn init_graph(&self, worker: usize, entries: &[NodeEntry]) -> CoreResult<()>`.
`runtime.handoff_node_handle()` must be set first (via
`with_handoff_node_handle`) before init_graph, because HandoffNode's init
reads it — check the existing service.rs flow sets it.

The `service_packet_graph_resolves_tcp_nodes` test (service.rs:1112-1120)
uses `packet_graph::resolve_graph_node("session-queue")` etc. That fn is
DELETED. Rewrite the test to iterate `packet_graph::SERVICE_GRAPH_NODES` by
name (like packet_graph.rs:26-41 already does) and assert the expected node
names are present. The trace call at service.rs:403
`trace.publish_options(&config.trace, packet_graph::resolve_graph_node)?` —
`resolve_graph_node` is gone; check what `publish_options` actually needs
(read its signature) and provide the equivalent. It likely needs a
`fn(&str) -> Option<NodeId>` closure — you can pass
`|name| SERVICE_GRAPH_NODES.iter().find(|e| e.registration.name() == Some(name)).map(|_| NodeId::new(0))`
OR better, build a real name->NodeId map after init_graph. Read the actual
`publish_options` signature to decide. If it needs a real NodeId-by-name
resolver against a registered graph, that requires a built runtime —
investigate and do the minimal correct thing.

## Delete stale code

- `crates/hammer-service/src/session/node.rs`: delete `assemble_session_queue_node`
  (lines ~115-127) and the `assemble =` / `wire =` args on the
  `#[graph_node]` attribute (lines ~129-135). Add the new `register_session_queue`
  init fn + the `attach_queue_by_runtime_data` assoc fn (if you go that route).
- `crates/hammer-service/src/net/lookup/mod.rs`: the `#[graph_node]` on
  `IpLookupNode` currently uses `register = ip_lookup` (OLD arg) — change to
  `init = crate::net::lookup::register_ip_lookup`. Rename
  `register_ip_lookup_graph_node` -> `register_ip_lookup` if not already.
  Delete `assemble_ip_lookup_node` if still present.
- Any remaining `with_boot` / `Boot` / `wire_session_queue` references in
  service crate are dead — they should already be gone; if build flags any,
  delete them.

## Acceptance / verification (run these, report output)

1. `cargo build -p hammer-component-macros` — still passes (5/5 lib tests).
2. `cargo build -p hammer-service` — passes (zero errors).
3. `cargo test -p hammer-service --lib packet_graph` —
   `service_graph_contains_tcp_nodes` passes.
4. `cargo test -p hammer-service --lib service::service_packet_graph_resolves_tcp_nodes`
   (after you rewrite it) — passes.
5. `cargo test -p hammer-service --lib` — full lib suite passes (or report
   any pre-existing failures unrelated to your change).
6. `cargo test -p hammer-service --lib transport::tcp` — TCP tests pass
   (the big tcp/mod.rs test suite; they build their own graph via
   `tcp_output_graph` helper, NOT via linkme, so they should be unaffected —
   but verify).
7. `cargo build --workspace` — passes (downstream crates hammer-ffi etc.).
   If a downstream crate breaks on a deleted API you removed, fix it
   minimally (it's in scope: you deleted the API, you fix the callers).

Report the exact commands + pass/fail counts in your report file.

## Constraints (from AGENTS.md + plan)

- No `Mutex` / locks for per-worker state. Per-worker state lives in
  `TcpWorkerOwnedState` TLS (single-threaded per worker). This is already
  the case — don't regress it.
- No `_underscore` prefixed variable names; use bare `_` for unused.
- No redundant comments narrating what code does.
- Node names have NO `-node` suffix (already done globally — keep it).
- `with_congestion!` is the transport-layer CC dispatch (transport/mod.rs:42),
  NOT tcp-specific. Use it for congestion-typed node init fns.
- Reuse existing APIs; don't add wrappers. `try_register_internal`,
  `try_register_driver`, `try_register_internal_with_next_names`,
  `register_internal_with_handle`, `node_by_name`, `set_node_state`,
  `NodeState::Polling` all exist on `NodeRuntime` (accessed via
  `rt.nodes()`).
- Don't touch the macro crate (it's done and committed). Don't touch
  packet_graph.rs's slice structure (single `SERVICE_GRAPH_NODES`, no LATE).

## Report file

Write your full report to `/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/.sdd-briefs/task-service-graph-migration-report.md`.

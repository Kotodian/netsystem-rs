---
name: hammer-graph-node-init
description: Use Hammer graph_node and node macros to initialize graph nodes, register next arcs, install runtime data, and keep DataPlaneMain ownership correct.
---

# Hammer Graph Node Initialization

Use the existing `#[graph_node]` and `#[node(...)]` macros. A plugin Main does
not register itself as a node, and no runtime graph-registration abstraction is
needed.

## Registration shape

For a stateful node, give `graph_node` an explicit `init` callback. The callback
receives the owning `&DataPlaneMain`, constructs the node with its owner-provided
state, and calls the existing node registration API.

```rust
#[graph_node(
    graph = service,
    init = register_ip_lookup,
    next = IpLookupNext,
)]
pub struct IpLookupNode {
    #[node(default = register_ip_lookup_runtime(table.clone()))]
    runtime_data: NodeRuntimeData,
    table: FibTableHandle,
}

fn register_ip_lookup(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let table = ip_main_table_handle()?;
    runtime.nodes().try_register_internal_with_next_names(
        IpLookupNode::new(table),
        &IpLookupNext::NEXT_NAMES,
    )
}
```

`#[node(default = expression)]` supplies constructor arguments for generated
node initialization and runtime-data registration. Keep node state concrete;
the callback must not add a lock or erased registry merely to pass state.

For a zero-state unit node, `graph_node(kind = internal, next = Next)` may
generate the init callback and `Node::new()` call. Use an explicit `init` when
the node needs a Main-owned handle or other runtime state.

## Lifecycle boundary

`install_packet_graph` walks the link-time `NodeEntry` inventory and invokes each
node init callback with a worker-owned `DataPlaneMain`. The callback registers
the node and its named next arcs; it does not initialize plugin Main state.
Plugin Main initialization belongs to `#[init_function(...)]` and must run
before `install_packet_graph` via `runs_before = ["install_packet_graph"]`.

Worker initialization is separate: use `#[worker_init_function]` only for state
owned by each Data Worker. Do not use `thread_local!`, a packet-path mutex, or a
foreign worker mutation to make node setup convenient.

## Evidence

- `crates/hammer-component-macros/src/lib.rs`: `graph_node` expansion,
  generated unit-node init, and `#[node(default)]` handling.
- `crates/hammer-runtime/src/graph/install.rs`: graph inventory installation.
- `crates/hammer-plugins/ip/src/ip/input.rs`: explicit node init callback.
- `crates/hammer-plugins/ip/src/lookup/mod.rs`: stateful node registration
  with `FibTableHandle` and next-name registration.

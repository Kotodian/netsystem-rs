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

`main_loop::run` completes the registered normal init and config callbacks,
then calls `DataPlaneMain::init_graph_from_declarations`. That method walks the
`NodeEntry` inventory and invokes each node init callback with the thread-zero
`DataPlaneMain`. The callback registers the node and its named next arcs; it
does not initialize plugin Main state.

Graph materialization is a direct lifecycle call, not a registered init
function. Never name it in `runs_before` or `runs_after`. Plugin Main
initialization belongs to `#[init_function(...)]`; every normal init callback
already completes before graph materialization. Ordering attributes may refer
only to real registrations in the same lifecycle category.

Worker initialization is separate: use `#[worker_init_function]` only for state
owned by each Data Worker. Do not use `thread_local!`, a packet-path mutex, or a
foreign worker mutation to make node setup convenient.

## Evidence

- `crates/hammer-component-macros/src/lib.rs`: `graph_node` expansion,
  generated unit-node init, and `#[node(default)]` handling.
- `crates/hammer-runtime/src/main_loop.rs`: init/config ordering before graph
  materialization.
- `crates/hammer-runtime/src/data_plane/dispatch.rs`:
  `DataPlaneMain::init_graph_from_declarations`.
- `crates/hammer-plugins/net/ip/src/input.rs`: generated unit-node init.
- `crates/hammer-plugins/net/ip/src/lookup.rs`: explicit node callbacks and
  named-next registration.

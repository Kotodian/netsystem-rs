use hammer_adapter::{
    DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::graph::{GraphSpec, NodeCtx, NodeRegistry, PacketGraphAssembler};

#[hammer_component_macros::node_next]
enum TestNext {
    Drop,
    #[allow(dead_code)]
    Lookup,
}

#[hammer_component_macros::node(role = internal)]
struct DropNode;

#[hammer_component_macros::node(role = internal, next = TestNext)]
struct TestOutputNode;

/// Service-defined dependency bag for the test graph. The graph layer is
/// generic over `D` and never names its fields; the test reads them directly.
struct TestDeps {
    runtime_data_seed: u64,
}

fn noop_process(
    _runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    _frame: &mut hammer_adapter::BufferFrame,
) -> CoreResult<NodeResult> {
    Err(CoreError::internal("test node never processes"))
}

impl Node for DropNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut hammer_adapter::BufferFrame,
    ) -> CoreResult<NodeResult> {
        noop_process(runtime, NodeRuntimeData::empty(), frame)
    }
    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        noop_process
    }
    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::empty())
    }
}

impl Node for TestOutputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut hammer_adapter::BufferFrame,
    ) -> CoreResult<NodeResult> {
        noop_process(runtime, NodeRuntimeData::empty(), frame)
    }
    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        noop_process
    }
    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::empty())
    }
}

fn build_drop(ctx: &NodeCtx<'_, TestDeps>) -> CoreResult<NodeId> {
    // Touch the dependency bag to prove the typed `D` path works end-to-end.
    let _seed = ctx.deps().runtime_data_seed;
    let _ = _seed;
    ctx.runtime().try_register_internal(DropNode::new())
}

fn build_test_output(ctx: &NodeCtx<'_, TestDeps>) -> CoreResult<NodeId> {
    let drop = ctx.node("drop-node")?;
    let next = TestNext::nodes(drop, drop);
    ctx.runtime().try_register_internal(TestOutputNode::new(next))
}

#[test]
fn assembler_registers_nodes_in_dependency_order() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let mut registry: NodeRegistry<TestDeps> = NodeRegistry::new();
    registry
        .register("drop-node", build_drop)
        .expect("register drop");
    registry
        .register("test-output-node", build_test_output)
        .expect("register test-output");

    let spec = GraphSpec::from_names(["drop-node", "test-output-node"]);
    let assembler = PacketGraphAssembler::new(&registry, spec);
    let deps = TestDeps {
        runtime_data_seed: 42,
    };
    let graph = assembler
        .assemble_on(runtime.nodes(), 0, &deps)
        .expect("assemble");

    let [drop_id, out_id] = [graph.nodes[0], graph.nodes[1]];
    assert_eq!(runtime.node_by_name("test-output-node"), Some(out_id));
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(out_id, TestNext::Drop as usize)
            .unwrap(),
        drop_id
    );
}

#[test]
fn node_registry_rejects_duplicate_name() {
    let mut registry: NodeRegistry<TestDeps> = NodeRegistry::new();
    registry
        .register("dup-node", build_drop)
        .expect("first register");
    let err = registry
        .register("dup-node", build_drop)
        .expect_err("duplicate must fail");
    assert!(err.to_string().contains("node builder already registered"));
}

#[test]
fn node_registry_errors_on_unknown_name() {
    let registry: NodeRegistry<TestDeps> = NodeRegistry::new();
    let err = registry.get("nope").expect_err("unknown must fail");
    assert!(err.to_string().contains("node builder not registered"));
}

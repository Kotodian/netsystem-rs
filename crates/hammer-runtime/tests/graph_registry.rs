use hammer_adapter::{
    DataPlaneRuntime, InternalNode, Node, NodeId, NodeKind, NodeProcessFn, NodeRegistration,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::graph::Graph;

#[hammer_component_macros::node_next]
enum TestNext {
    Drop,
}

#[derive(Clone, Copy)]
struct DropNode;

impl DropNode {
    #[inline]
    fn new() -> Self {
        Self
    }
}

#[hammer_component_macros::node(role = internal, next = TestNext)]
struct TestOutputNode;

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

impl InternalNode for DropNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("drop-node", 0)
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

fn init_drop(runtime: &hammer_adapter::NodeRuntime, _: usize, _: &()) -> CoreResult<NodeId> {
    runtime.try_register_internal(DropNode::new())
}

fn init_test_output(runtime: &hammer_adapter::NodeRuntime, _: usize, _: &()) -> CoreResult<NodeId> {
    runtime.try_register_internal_with_next_names(
        TestOutputNode::new([NodeId::new(0); TestNext::COUNT]),
        &TestNext::NEXT_NAMES,
    )
}

static TEST_GRAPH_NODES: [(
    NodeRegistration,
    NodeKind,
    fn(&hammer_adapter::NodeRuntime, usize, &()) -> CoreResult<NodeId>,
    Option<fn(&hammer_adapter::NodeRuntime, usize, &()) -> CoreResult<()>>,
); 2] = [
    (
        NodeRegistration::next("drop-node", 0),
        NodeKind::Internal,
        init_drop,
        None,
    ),
    (
        NodeRegistration::next("test-output-node", TestNext::COUNT),
        NodeKind::Internal,
        init_test_output,
        None,
    ),
];

fn graph_node_slot(
    graph_nodes: &[(
        NodeRegistration,
        NodeKind,
        fn(&hammer_adapter::NodeRuntime, usize, &()) -> CoreResult<NodeId>,
        Option<fn(&hammer_adapter::NodeRuntime, usize, &()) -> CoreResult<()>>,
    )],
    name: &str,
) -> Option<NodeId> {
    graph_nodes
        .iter()
        .position(|(registration, ..)| match registration {
            NodeRegistration::Next {
                name: node_name, ..
            }
            | NodeRegistration::Sibling {
                name: node_name, ..
            } => *node_name == name,
            NodeRegistration::Plain => false,
        })
        .and_then(|slot| u32::try_from(slot).ok())
        .map(NodeId::new)
}

#[test]
fn graph_init_resolves_named_next_edges() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let graph = Graph::new(&TEST_GRAPH_NODES);
    graph.init(runtime.nodes(), 0, &()).expect("init");

    let drop_id = graph.node("drop-node").expect("drop");
    let out_id = graph.node("test-output-node").expect("output");
    assert_eq!(runtime.node_by_name("test-output-node"), Some(out_id));
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(out_id, TestNext::Drop as usize)
            .unwrap(),
        drop_id
    );
}

static REVERSE_GRAPH_NODES: [(
    NodeRegistration,
    NodeKind,
    fn(&hammer_adapter::NodeRuntime, usize, &()) -> CoreResult<NodeId>,
    Option<fn(&hammer_adapter::NodeRuntime, usize, &()) -> CoreResult<()>>,
); 2] = [
    (
        NodeRegistration::next("test-output-node", TestNext::COUNT),
        NodeKind::Internal,
        init_test_output,
        None,
    ),
    (
        NodeRegistration::next("drop-node", 0),
        NodeKind::Internal,
        init_drop,
        None,
    ),
];

#[test]
fn graph_init_allows_reverse_registration_order() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let graph = Graph::new(&REVERSE_GRAPH_NODES);
    graph
        .init(runtime.nodes(), 0, &())
        .expect("init reverse order");
    assert!(runtime.node_by_name("test-output-node").is_some());
}

#[test]
fn graph_register_supports_dynamic_nodes() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let graph = Graph::<()>::new(&[]);
    graph
        .register(
            NodeRegistration::next("drop-node", 0),
            NodeKind::Internal,
            init_drop,
            None,
        )
        .expect("register");
    graph.init(runtime.nodes(), 0, &()).expect("init");
    assert!(graph.node("drop-node").is_some());
}

#[test]
fn graph_register_rejects_duplicate_name() {
    let graph = Graph::<()>::new(&TEST_GRAPH_NODES);
    let err = graph
        .register(
            NodeRegistration::next("drop-node", 0),
            NodeKind::Internal,
            init_drop,
            None,
        )
        .expect_err("duplicate");
    assert!(err.to_string().contains("already registered"));
}

#[test]
fn graph_node_maps_registration_name_to_slot_id() {
    assert_eq!(
        graph_node_slot(&TEST_GRAPH_NODES, "drop-node"),
        Some(NodeId::new(0))
    );
    assert_eq!(
        graph_node_slot(&TEST_GRAPH_NODES, "test-output-node"),
        Some(NodeId::new(1))
    );
    assert!(graph_node_slot(&TEST_GRAPH_NODES, "missing").is_none());
}

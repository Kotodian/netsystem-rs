#[test]
fn protocol_namespace_exposes_runtime_tcp() {
    let _ = std::any::type_name::<hammer_runtime::protocol::tcp::TcpControlPlane>();
}

#[test]
fn graph_namespace_exposes_graph() {
    let _ = std::any::type_name::<hammer_runtime::graph::Graph<()>>();
}

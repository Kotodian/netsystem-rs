use std::fs;
use std::path::Path;

#[test]
fn tcp_state_nodes_emit_segments_to_congestion_next() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/tcp");
    for file in [
        "listen.rs",
        "syn_sent.rs",
        "syn_rcvd.rs",
        "established.rs",
        "close_wait.rs",
        "fin_wait1.rs",
        "fin_wait2.rs",
        "closing.rs",
        "last_ack.rs",
        "time_wait.rs",
    ] {
        let source = fs::read_to_string(root.join(file)).expect("read tcp state node");
        assert!(
            source.contains("Congestion"),
            "{file} must expose a Congestion next"
        );
        assert!(
            !source.contains("Next::Output"),
            "{file} must not keep Output as the TCP segment emission next"
        );
    }
}

#[test]
fn congestion_node_exposes_transport_nexts() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/congestion");
    let node = fs::read_to_string(root.join("node.rs")).expect("read congestion node");
    let bbr = fs::read_to_string(root.join("bbr.rs")).expect("read bbr node");

    assert!(node.contains("pub enum CongestionControlNext"));
    assert!(node.contains("Transmit"));
    assert!(node.contains("Defer"));
    assert!(node.contains("Drop"));
    assert!(bbr.contains("sibling_of = CongestionControlNode"));
    assert!(bbr.contains("BbrCongestionNode::runtime_nexts(runtime)?"));
}

#[test]
fn tcp_connection_construction_has_no_algorithm_registry_or_state_turbofish() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/tcp");
    let mut combined = String::new();
    for file in ["state.rs", "state_machine.rs", "session.rs", "listen.rs"] {
        combined.push_str(&fs::read_to_string(root.join(file)).expect("read tcp source"));
    }

    for forbidden in [
        "TcpCongestionAlgorithm",
        "TcpCongestionRegistry",
        "TcpConnectionConfigState",
        ">>::new(",
        "TcpConnection::new::<",
    ] {
        assert!(
            !combined.contains(forbidden),
            "tcp construction kept a rejected registry or explicit constructor state: {forbidden}"
        );
    }
}

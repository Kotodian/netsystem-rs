use std::fs;
use std::path::Path;

#[test]
fn tcp_state_nodes_emit_segments_to_output_next() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/tcp");
    for file in [
        "listen.rs",
        "syn_sent.rs",
        "rcv_process.rs",
        "established.rs",
    ] {
        let source = fs::read_to_string(root.join(file)).expect("read tcp state node");
        assert!(source.contains("Output"), "{file} must expose Output next");
        for forbidden in [
            "CongestionNext",
            "CongestionNode",
            "BbrCongestionNode",
            "TcpCongestionNext",
            "next = TcpCongestion",
        ] {
            assert!(
                !source.contains(forbidden),
                "{file} must not expose congestion graph next surface: {forbidden}"
            );
        }
    }
}

#[test]
fn congestion_control_is_not_a_packet_graph_node() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/congestion");
    let bbr = fs::read_to_string(root.join("bbr.rs")).expect("read bbr controller");
    let module = fs::read_to_string(root.join("mod.rs")).expect("read congestion mod");

    for forbidden in [
        "CongestionControlNode",
        "CongestionControlNext",
        "BbrCongestionNode",
        "sibling_of = CongestionControlNode",
    ] {
        assert!(
            !bbr.contains(forbidden) && !module.contains(forbidden),
            "congestion control kept packet graph node surface: {forbidden}"
        );
    }
}

#[test]
fn tcp_connection_construction_has_no_algorithm_registry_or_state_turbofish() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/tcp");
    let mut combined = String::new();
    for file in ["connection.rs", "listen.rs"] {
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
